"""Demo RFQ v2 market maker from the "Build an RFQ v2 maker" tutorial.

DEMO ONLY. This is not a risk system. It shows the shape of an RFQ V2 maker
on Hypercall, end to end:

    stream RFQs (full view) -> price off Deribit's mark +/- a spread
    -> post a signed firm quote -> read executions by cursor
    -> hedge each execution's net delta with a Hyperliquid perp
    -> renew a cancel schedule (dead-man switch) -> withdraw everything on exit

Its only limits are a cap on open quotes and on contracts per leg. It does
not track worst-case exposure across open quotes, reconcile positions after
a restart, or handle ambiguous quote outcomes. Bring your own limits before
quoting real size: quotes are firm, and once posted a taker can fill them.

    pip install hypercall-rfq-example   # or, from this folder: pip install .
    export HC_ACCOUNT=0xYourAccount HC_SIGNER_KEY=0xApiWalletKey
    python maker.py --dry-run          # price and log, post nothing
    python maker.py --host http://localhost:3000 --chain-id 998
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime

import httpx

from hypercall_rfq_example import Execution, Hypercall, HypercallError, Rfq

DERIBIT = "https://www.deribit.com/api/v2"
# Smaller residual deltas wait for the next fill. Hyperliquid refuses orders
# under about $10 of notional, so keep each minimum above $10 at the current price.
MIN_HEDGE = {"BTC": 0.001, "ETH": 0.01}
log = logging.getLogger("maker")


# ---- pricing (step 3) --------------------------------------------------------


def underlying(symbol: str) -> str:
    return symbol.split("-")[0]


def to_deribit(symbol: str) -> str:
    """BTC-20261226-100000-C -> BTC-26DEC26-100000-C (Deribit drops the leading zero of the day)."""
    coin, expiry, strike, kind = symbol.split("-")
    day = datetime.strptime(expiry, "%Y%m%d")
    return f"{coin}-{day.day}{day.strftime('%b%y').upper()}-{strike}-{kind}"


@dataclass
class Mark:
    usd: float  # mark price per contract, in USD
    delta: float  # per contract, from the long side


class Pricer:
    def __init__(self, spread: float):
        self.spread = spread
        self.http = httpx.AsyncClient(base_url=DERIBIT, timeout=5)

    async def mark(self, symbol: str) -> Mark:
        r = await self.http.get("/public/ticker", params={"instrument_name": to_deribit(symbol)})
        r.raise_for_status()
        t = r.json()["result"]
        # Deribit quotes options in the coin; convert to USD per contract.
        return Mark(t["mark_price"] * t["underlying_price"], t["greeks"]["delta"])

    async def price_leg(self, leg) -> float:
        """Deribit mark, plus the spread when the taker buys, minus when it sells."""
        mark = (await self.mark(leg.instrument)).usd
        return mark * (1 + self.spread) if leg.taker_side == "buy" else mark * (1 - self.spread)


# ---- the one guard (step 6) ------------------------------------------------------


class Limits:
    """A cap on open quotes and on contracts per leg. Not a risk system."""

    def __init__(self, max_open_quotes: int, max_contracts: float):
        self.max_open_quotes = max_open_quotes
        self.max_contracts = max_contracts
        self.open: dict[str, float] = {}  # quote id -> expires_at_ms

    def allows(self, rfq: Rfq, units: int) -> bool:
        now = time.time() * 1000
        self.open = {q: exp for q, exp in self.open.items() if exp > now}
        packages = units / 1_000_000
        biggest_leg = max(float(leg.contracts_per_package) * packages for leg in rfq.legs)
        return len(self.open) < self.max_open_quotes and biggest_leg <= self.max_contracts

    def track(self, quote_id: str, expires_at_ms: float) -> None:
        self.open[quote_id] = expires_at_ms


# ---- the bot -------------------------------------------------------------------


class Maker:
    def __init__(self, hc: Hypercall, pricer: Pricer, limits: Limits, dry_run: bool, quote_ttl_s: float):
        self.hc, self.pricer, self.limits = hc, pricer, limits
        self.dry_run, self.quote_ttl_s = dry_run, quote_ttl_s
        self.answered: set[str] = set()
        self.unhedged: dict[str, float] = {}  # options delta per coin not yet hedged
        self.stats = {"rfqs": 0, "quoted": 0, "declined": 0, "executions": 0, "hedges": 0}

    async def on_rfq(self, rfq: Rfq) -> None:
        """Step 4: quote or decline each new RFQ once."""
        # Our own quote produces another change for this RFQ, so answer each
        # RFQ once, before the first await.
        if rfq.status != "open" or rfq.id in self.answered or rfq.maker_generation is None:
            return
        self.answered.add(rfq.id)
        if rfq.quote_deadline_ms <= await self.hc.now_ms():
            return
        self.stats["rfqs"] += 1
        legs = [(l.instrument, l.taker_side, str(l.contracts_per_package)) for l in rfq.legs]
        units = rfq.target if rfq.fill_policy["mode"] == "full_size_once" else rfq.remaining_target
        if not self.limits.allows(rfq, units):
            log.info("rfq %s %s: over the open-quote or size limit, declining", rfq.id, legs)
            return await self.decline(rfq)
        try:
            prices = [await self.pricer.price_leg(l) for l in rfq.legs]
        except (httpx.HTTPError, KeyError, ValueError) as error:
            log.info("rfq %s %s: no Deribit price (%s), declining", rfq.id, legs, error)
            return await self.decline(rfq)
        if any(p <= 0 for p in prices):
            log.info("rfq %s %s: zero mark, declining", rfq.id, legs)
            return await self.decline(rfq)
        log.info("rfq %s %s: pricing %s", rfq.id, legs, [round(p, 6) for p in prices])
        if self.dry_run:
            return
        try:
            quote = await self.hc.rfq_v2.quote(rfq, prices=prices, capacity=units, expires_in_s=self.quote_ttl_s)
        except (HypercallError, httpx.HTTPError, ValueError) as error:
            log.warning("rfq %s: quote not posted: %s", rfq.id, error)
            return
        self.limits.track(quote.id, quote.expires_at_ms)
        self.stats["quoted"] += 1
        log.info("quote %s on rfq %s: executable=%s capacity=%s", quote.id, rfq.id, quote.executable, quote.capacity)

    async def handle(self, rfq: Rfq) -> None:
        await self.on_rfq(rfq)
        self.hc.rfq_v2.ack(rfq)  # not reached if handling raised or was cancelled

    async def decline(self, rfq: Rfq) -> None:
        self.stats["declined"] += 1
        if not self.dry_run:
            try:
                await self.hc.rfq_v2.decline(rfq)
            except (HypercallError, httpx.HTTPError) as error:
                log.warning("rfq %s: decline refused: %s", rfq.id, error)

    async def run_executions(self, cursor_file: str) -> None:
        """Step 5: every execution, once, in order: log it, then hedge it.

        The cursor file resumes after the last execution handled, on a
        restart and on every retry below, so no execution is hedged twice. A failed read or handler
        is logged and retried with backoff instead of raised: inside the
        TaskGroup an unhandled error would also stop RFQ intake and the
        heartbeat.
        """
        backoff = 1.0
        while True:
            try:
                async for ex in self.hc.rfq_v2.executions(cursor_file=cursor_file):
                    await self.on_execution(ex)
                    backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - the feed must outlive any one failure
                log.warning("executions feed failed, retrying in %.0fs: %s", backoff, error)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def on_execution(self, ex: Execution) -> None:
        if ex.role != "maker":
            return
        delta: dict[str, float] = {}
        for leg in ex.legs:
            mark = await self.pricer.mark(leg.instrument)
            sign = 1 if leg.side == "buy" else -1
            coin = underlying(leg.instrument)
            delta[coin] = delta.get(coin, 0.0) + sign * float(leg.contracts) * mark.delta
        self.stats["executions"] += 1
        log.info(
            "execution %s rfq %s quote %s units %s legs %s delta %s",
            ex.execution_id, ex.rfq_id, ex.quote_id, ex.package_units,
            [(l.instrument, l.side, str(l.contracts), str(l.price_usd)) for l in ex.legs], delta,
        )
        for coin, d in delta.items():
            self.unhedged[coin] = self.unhedged.get(coin, 0.0) + d
        # The execution is booked once its delta is in `unhedged`. A hedge
        # failure must not escape: the executions loop would retry this
        # execution and add its delta a second time. The delta stays unhedged
        # and the next execution retries the order.
        try:
            await self.hedge()
        except Exception as error:  # noqa: BLE001 - never re-book an execution
            log.warning("hedge after execution %s failed, will retry after the next fill: %s", ex.execution_id, error)

    async def hedge(self) -> None:
        """Step 7: sell (or buy) the Hyperliquid perp against the unhedged options delta.

        Deltas too small to trade carry over to the next execution. A failed
        order keeps its delta unhedged so the next execution retries it.
        """
        for coin, delta in list(self.unhedged.items()):
            if abs(delta) < MIN_HEDGE.get(coin, 0.01):
                continue
            log.info("hedge %s: %s %.6f perp", coin, "sell" if delta > 0 else "buy", abs(delta))
            if self.dry_run:
                self.unhedged[coin] = 0.0
                continue
            try:
                result = await self.hc.perps.order(coin=coin, is_buy=delta < 0, size=abs(delta), type="ioc", slippage_bps=20)
            except (HypercallError, httpx.HTTPError, KeyError, ValueError) as error:
                log.warning("hedge %s failed, will retry after the next fill: %s", coin, error)
                continue
            self.unhedged[coin] = 0.0
            self.stats["hedges"] += 1
            log.info("hedge %s submitted: %s", coin, result)

    async def heartbeat(self) -> None:
        """Step 6: a 30-second dead-man switch, renewed every 10 seconds."""
        while True:
            if not self.dry_run:
                try:
                    await self.hc.rfq_v2.cancel_after(seconds=30)
                except (HypercallError, httpx.HTTPError) as error:
                    log.warning("heartbeat renewal failed: %s", error)
            await asyncio.sleep(10)

    async def report(self) -> None:
        while True:
            await asyncio.sleep(30)
            log.info("stats %s open_quotes=%d unhedged=%s", self.stats, len(self.limits.open), self.unhedged)


def stop_on_signals() -> None:
    """Cancel the main task on SIGINT or SIGTERM, so its `finally` still withdraws.

    Without this, SIGTERM (what `kill`, systemd, Docker and Kubernetes send)
    ends the process at once and every open quote stays live.
    """
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()

    stopping = False

    def stop(sig: signal.Signals) -> None:
        nonlocal stopping
        if stopping:
            return  # a second signal must not cut the withdrawal short
        stopping = True
        log.info("%s: withdrawing quotes and stopping", sig.name)
        task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop, sig)  # Unix only


async def main(args) -> None:
    stop_on_signals()
    hc = Hypercall(host=args.host, account=os.environ["HC_ACCOUNT"], signer_key=os.environ["HC_SIGNER_KEY"], chain_id=args.chain_id)
    if args.hyperliquid_info:
        hc.perps.info_url = args.hyperliquid_info  # e.g. a localnet HyperCore node
    try:
        log.info("provider: %s", await hc.rfq_v2.provider())
    except HypercallError as error:
        log.info("provider read unavailable: %s", error)
    maker = Maker(hc, Pricer(args.spread), Limits(args.max_open_quotes, args.max_contracts), args.dry_run, args.quote_ttl)
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(maker.heartbeat())
            tg.create_task(maker.run_executions(args.exec_cursor))
            tg.create_task(maker.report())
            # One task per RFQ. The cursor advances only once a task is done,
            # so a restart replays any RFQ still being priced or posted.
            async for rfq in hc.rfq_v2.stream(cursor_file=args.rfq_cursor, manual_ack=True):
                tg.create_task(maker.handle(rfq))
    finally:
        if not args.dry_run:
            # Never leave quotes behind.
            await hc.rfq_v2.withdraw_all()
            await hc.rfq_v2.cancel_after(None)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--host", default=os.environ.get("HC_HOST", "https://api.hypercall.xyz"))
    p.add_argument("--chain-id", type=int, default=int(os.environ.get("HC_CHAIN_ID", "999")))
    p.add_argument("--hyperliquid-info", default=os.environ.get("HC_HYPERLIQUID_INFO"), help="Hyperliquid /info URL for perp meta and mids")
    p.add_argument("--dry-run", action="store_true", help="price and log RFQs without posting anything")
    p.add_argument("--spread", type=float, default=0.02)
    p.add_argument("--quote-ttl", type=float, default=20.0, help="seconds each quote stays executable")
    p.add_argument("--max-open-quotes", type=int, default=20, help="open quotes at once")
    p.add_argument("--max-contracts", type=float, default=5.0, help="contracts per leg in one quote")
    p.add_argument("--rfq-cursor", default="rfq.cursor")
    p.add_argument("--exec-cursor", default="exec.cursor")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main(p.parse_args()))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
