"""The demo maker's pure parts: symbol mapping, the one guard, hedging and the executions loop."""

import asyncio
import importlib.util
import sys
import time
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from hypercall_rfq_example import Hypercall, Leg, Rfq
from hypercall_rfq_example.client import ExecutionLeg

spec = importlib.util.spec_from_file_location("maker", Path(__file__).parents[1] / "maker.py")
maker = importlib.util.module_from_spec(spec)
sys.modules["maker"] = maker  # dataclasses look their module up by name
spec.loader.exec_module(maker)


def rfq(legs):
    return Rfq("r", 1, "open", "0x", legs, 2_000_000, 2_000_000, {"mode": "full_size_once"}, 0, 0, 0, 0, {})


class FakePricer:
    async def mark(self, symbol):
        return maker.Mark(100.0, 0.25)


def execution(seq, contracts=2):
    leg = ExecutionLeg("BTC-20261226-100000-C", "buy", Decimal(contracts), Decimal(100))
    return maker.Execution(seq, f"e{seq}", "r", "q", "maker", 1_000_000, 0, [leg])


def test_to_deribit_drops_the_leading_zero_of_the_day():
    assert maker.to_deribit("BTC-20261226-100000-C") == "BTC-26DEC26-100000-C"
    assert maker.to_deribit("ETH-20270105-3000-P") == "ETH-5JAN27-3000-P"


def test_limits_cap_open_quotes_and_contracts_per_leg():
    limits = maker.Limits(max_open_quotes=1, max_contracts=3.0)
    one_contract = rfq([Leg("BTC-20261226-100000-C", "buy", 1_000_000)])
    assert limits.allows(one_contract, 2_000_000)  # 2 contracts
    assert not limits.allows(one_contract, 4_000_000)  # 4 contracts is over 3
    limits.track("q1", time.time() * 1000 + 60_000)
    assert not limits.allows(one_contract, 1_000_000)  # one quote already open
    limits.track("q1", 0)  # expired quotes stop counting
    assert limits.allows(one_contract, 1_000_000)


async def test_each_execution_hedges_its_delta_and_small_residuals_carry_over():
    hedges = []

    class Hc:
        class perps:
            @staticmethod
            async def order(coin, is_buy, size, **_):
                hedges.append((coin, is_buy, round(size, 6)))
                return {"stage": "enqueued"}

    bot = maker.Maker(Hc(), FakePricer(), maker.Limits(10, 10), dry_run=False, quote_ttl_s=20)
    # We bought 2 calls at delta 0.25: +0.5 BTC, so sell 0.5 BTC of perp.
    await bot.on_execution(execution(1))
    assert hedges == [("BTC", False, 0.5)] and abs(bot.unhedged["BTC"]) < 1e-12
    # 0.002 contracts is 0.0005 BTC of delta, below the 0.001 minimum: carried.
    await bot.on_execution(execution(2, contracts=Decimal("0.002")))
    assert len(hedges) == 1 and abs(bot.unhedged["BTC"] - 0.0005) < 1e-12
    await bot.on_execution(execution(3, contracts=Decimal("0.002")))
    assert hedges[-1] == ("BTC", False, 0.001) and abs(bot.unhedged["BTC"]) < 1e-12


async def test_an_executions_feed_error_is_retried_not_fatal(monkeypatch):
    from hypercall_rfq_example import HypercallError

    handled = []
    calls = {"n": 0}

    class Feed:
        async def executions(self, cursor_file=None):
            calls["n"] += 1
            if calls["n"] == 1:
                yield execution(1)
                raise HypercallError(404, "rfq_v2_not_found", "transient")
            # The cursor resumes after execution 1, so the retry starts at 2.
            yield execution(2)
            raise asyncio.CancelledError  # end the test

    class Hc:
        rfq_v2 = Feed()

    class Bot(maker.Maker):
        async def on_execution(self, ex):
            handled.append(ex.sequence)

    async def no_sleep(_):
        return None

    monkeypatch.setattr(maker.asyncio, "sleep", no_sleep)
    bot = Bot(Hc(), FakePricer(), maker.Limits(10, 10), dry_run=True, quote_ttl_s=20)
    with pytest.raises(asyncio.CancelledError):
        await bot.run_executions("exec.cursor")
    assert calls["n"] == 2 and handled == [1, 2]


ACCOUNT = "0x00000000000000000000000000000000000000aa"
KEY = "0x" + "11" * 32


def execution_json(seq):
    return {
        "sequence": seq, "execution_id": f"e{seq}", "rfq_id": "r", "quote_id": "q", "role": "maker",
        "package_units": "1000000", "executed_at_ms": 0,
        "legs": [{"instrument": "BTC-20261226-100000-C", "side": "buy", "quantity_micros": "2000000", "price_micros": "100000000"}],
    }


def client(handler):
    def route(request):
        if request.url.path == "/health":
            return httpx.Response(200, headers={"date": "Thu, 17 Mar 2030 17:46:40 GMT"}, json={})
        return handler(request)

    http = httpx.AsyncClient(base_url="http://hc.test", transport=httpx.MockTransport(route))
    return Hypercall("http://hc.test", ACCOUNT, KEY, http=http)


async def test_a_retry_resumes_from_the_saved_cursor_not_from_zero(tmp_path, monkeypatch):
    seen_after = []

    def handler(request):
        after = int(request.url.params["after_sequence"])
        seen_after.append(after)
        if len(seen_after) == 2:
            return httpx.Response(404, json={"code": "rfq_v2_not_found", "message": "transient"})
        if len(seen_after) == 4:
            raise asyncio.CancelledError  # end the test
        batch = [execution_json(after + 1)]
        return httpx.Response(200, json={"executions": batch, "next_after_sequence": after + 1, "has_more": True})

    handled = []

    class Bot(maker.Maker):
        async def on_execution(self, ex):
            handled.append(ex.sequence)

    async def no_sleep(_):
        return None

    monkeypatch.setattr(maker.asyncio, "sleep", no_sleep)
    cursor = tmp_path / "exec.cursor"
    bot = Bot(client(handler), FakePricer(), maker.Limits(10, 10), dry_run=True, quote_ttl_s=20)
    with pytest.raises(asyncio.CancelledError):
        await bot.run_executions(str(cursor))
    # Read 0 -> execution 1; the next read (after 1) fails; the retry resumes
    # after 1, not from 0, and execution 1 is handled once.
    assert seen_after == [0, 1, 1, 2]
    assert handled == [1, 2]
    assert cursor.read_text() == "2"


async def test_a_hedge_failure_never_books_an_execution_twice(tmp_path, monkeypatch):
    orders = []

    class Hc:
        class perps:
            @staticmethod
            async def order(coin, is_buy, size, **_):
                orders.append(size)
                raise RuntimeError("HyperCore unavailable")

    bot = maker.Maker(Hc(), FakePricer(), maker.Limits(10, 10), dry_run=False, quote_ttl_s=20)
    await bot.on_execution(execution(1))  # does not raise
    assert orders == [0.5]
    assert abs(bot.unhedged["BTC"] - 0.5) < 1e-12  # booked once, still unhedged
    assert bot.stats["executions"] == 1
