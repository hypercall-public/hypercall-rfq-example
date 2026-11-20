"""Client behaviour against a mocked API: request bodies, cursors, renewals."""

import asyncio
import base64
import json
import uuid
from decimal import Decimal

import httpx
import pytest
from eth_account import Account

from hypercall_rfq_example import Hypercall, Leg, client, exact_prices, signing

KEY = "0x59c6995e998f97a5a0044966f094538e5b2f9f8c9360f0c2b3b8a4e1f2f8f96d"
SIGNER = Account.from_key(KEY).address.lower()
ACCOUNT = "0x00000000000000000000000000000000000000a1"
RFQ_ID = "01900000-0000-7000-8000-000000000001"
COMMITMENT = "0x" + "5a" * 32
NOW = 1_900_000_000_000


def maker_view(rfq_id=RFQ_ID, fill_policy=None, remaining="2000000"):
    return {
        "rfq_id": rfq_id,
        "status": "open",
        "terms_commitment": COMMITMENT,
        "target": "2000000",
        "remaining_target": remaining,
        "quote_policy": "firm_only",
        "maker_quote_generation": {"maker_generation": 3, "rfq_generation": 7},
        "parent": {
            "view": "maker",
            "terms": {
                "id": rfq_id,
                "legs": [
                    {"instrument": "BTC-20300125-100000-C", "taker_side": "buy", "quantity_micros_per_unit": "1000000"},
                    {"instrument": "BTC-20300125-110000-C", "taker_side": "sell", "quantity_micros_per_unit": "1000000"},
                ],
                "target": "2000000",
                "fill_policy": fill_policy or {"mode": "full_size_once"},
                "execution_mode": "manual",
                "quote_policy": "firm_only",
                "taker_identity": "anonymous",
                "quote_deadline_ms": NOW + 10_000,
                "expires_at_ms": NOW + 60_000,
                "fee_limits": {"per_execution": "0", "cumulative": "0"},
            },
        },
    }


class Api:
    """Records requests and answers from a route table."""

    def __init__(self, routes):
        self.routes, self.requests = routes, []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/health":
            return httpx.Response(200, headers={"date": "Thu, 17 Mar 2030 17:46:40 GMT"}, json={})
        handler = self.routes[(request.method, request.url.path)]
        body = handler(request) if callable(handler) else handler
        if isinstance(body, httpx.Response):
            return body
        return httpx.Response(200, json=body)

    def bodies(self, path):
        return [json.loads(r.content) for r in self.requests if r.url.path == path and r.content]


def make(routes) -> tuple[Hypercall, Api]:
    api = Api(routes)
    hc = Hypercall("http://hc.test", ACCOUNT, KEY, http=httpx.AsyncClient(base_url="http://hc.test", transport=httpx.MockTransport(api)))
    return hc, api


def recover(primary, message, signature, domain="Hypercall"):
    signable = signing._signable(primary, message, 999, domain)
    return Account.recover_message(signable, signature=bytes.fromhex(signature[2:])).lower()


async def test_read_grant_is_signed_by_the_signer_for_the_account_and_renewed():
    hc, api = make({("GET", f"/private/rfqs/{RFQ_ID}"): maker_view()})
    token = await hc.rfq_v2.token()
    grant = json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
    assert grant["account"] == ACCOUNT
    assert grant["expires_at_ms"] - grant["issued_at_ms"] <= 300_000
    message = signing.read_grant_message(ACCOUNT, grant["issued_at_ms"], grant["expires_at_ms"], grant["nonce"])
    assert recover("AuthorizeRfqReadV2", message, grant["signature"]) == SIGNER
    assert await hc.rfq_v2.token() == token  # reused while fresh
    hc.rfq_v2._token_expires_ms = await hc.now_ms() + 1_000  # about to lapse
    assert await hc.rfq_v2.token() != token
    await hc.rfq_v2.get(RFQ_ID)
    assert api.requests[-1].headers["authorization"].startswith("Bearer ")


def test_exact_prices_rounds_to_the_leg_granule_in_our_favour():
    legs = [Leg("A", "buy", 1_000_000), Leg("B", "sell", 500_000)]
    # Half a contract at 1.500001 USD is 750000.5 micros: not exact. The taker
    # sells leg B to us, so rounding down to a 2-micro multiple is in our favour.
    fixed = exact_prices(legs, [2_000_000, 1_500_001], 1_000_000, None)
    assert fixed == [2_000_000, 1_500_000]
    assert signing.reference_premium([leg.__dict__ for leg in legs], fixed, 1_000_000, None)
    # Untouched when already exact.
    assert exact_prices([Leg("A", "buy", 1_000_000)], [1_234_567], 1_000_000, None) == [1_234_567]
    # A granule that would move the price absurdly far is refused.
    with pytest.raises(ValueError):
        exact_prices([Leg("A", "buy", 1)], [1], 3, None)


@pytest.mark.parametrize(
    ("quantity", "granule", "buy", "sell"),
    [
        (10_000, 100, 1_234_600, 1_234_500),  # 0.01 contract per package
        (500_000, 2, 1_234_568, 1_234_566),  # 0.5 contract per package
    ],
)
def test_sub_contract_packages_round_each_leg_to_a_whole_premium(quantity, granule, buy, sell):
    assert client.price_granule(Leg("A", "buy", quantity), 1_000_000, None) == granule
    for side, expected in (("buy", buy), ("sell", sell)):
        legs = [Leg("A", side, quantity)]
        fixed = exact_prices(legs, [1_234_567], 1_000_000, None)
        assert fixed == [expected]
        assert signing.reference_premium([leg.__dict__ for leg in legs], fixed, 1_000_000, None)
    # A two-leg 0.01-contract spread: both legs move to the granule.
    spread = [Leg("A", "buy", quantity), Leg("B", "sell", quantity)]
    fixed = exact_prices(spread, [2_345_671, 1_234_568], 1_000_000, None)
    assert all(p % granule == 0 for p in fixed)
    assert signing.reference_premium([leg.__dict__ for leg in spread], fixed, 1_000_000, None)


def test_partial_fill_steps_also_need_a_whole_premium():
    # 0.01 contract per package, quoted in 0.1-package steps: 1,000 micros.
    leg = Leg("A", "buy", 10_000)
    assert client.price_granule(leg, 100_000, 100_000) == 1_000
    fixed = exact_prices([leg], [1_234_567], 100_000, 100_000)
    assert fixed == [1_235_000]
    assert signing.reference_premium([leg.__dict__], fixed, 100_000, 100_000)


async def test_quote_signs_the_terms_it_posts_with_generations_from_the_read():
    hc, api = make({("POST", f"/private/rfqs/{RFQ_ID}/quotes/firm"): {"firmness": "firm", "quote_id": "q", "status": "open", "executable": True, "remaining_capacity": "2000000", "preflight": {"status": "passed", "checked_at_ms": NOW}}})
    rfq = client.Rfq.from_view(maker_view(), 5)
    quote = await hc.rfq_v2.quote(rfq, prices=[2512.345678, 1100.000001], capacity=rfq.target, expires_in_s=20)
    body = api.bodies(f"/private/rfqs/{RFQ_ID}/quotes/firm")[0]
    terms = body["terms"]
    assert (terms["maker_generation"], terms["rfq_generation"]) == (3, 7)
    assert terms["rfq_terms_commitment"] == COMMITMENT
    assert terms["maker_account"] == ACCOUNT
    assert [l["price_micros"] for l in terms["priced_legs"]] == ["2512345678", "1100000001"]
    # Taker buys the 2512 call and sells the 1100 call: the maker receives the net.
    assert terms["maker_receives_reference_premium"] is True
    assert terms["reference_premium_micros"] == str((2_512_345_678 - 1_100_000_001) * 2)
    assert terms["capacity"] == terms["reference_quantity"] == "2000000"
    assert terms["expires_at_ms"] <= rfq.expires_at_ms
    assert uuid.UUID(terms["id"]).version == 7
    assert recover("AuthorizeRfqQuoteV2", signing.firm_quote_message(terms, body["nonce"]), body["signature"]) == SIGNER
    assert quote.executable and quote.capacity == 2_000_000


async def test_partial_fill_quotes_reference_one_step_and_offer_the_remaining_target():
    hc, api = make({("POST", f"/private/rfqs/{RFQ_ID}/quotes/firm"): {"executable": True}})
    view = maker_view(fill_policy={"mode": "repeated_partial", "minimum": "1000000", "step": "1000000"}, remaining="1000000")
    await hc.rfq_v2.quote(client.Rfq.from_view(view, 1), prices=[10, 4])
    terms = api.bodies(f"/private/rfqs/{RFQ_ID}/quotes/firm")[0]["terms"]
    assert (terms["reference_quantity"], terms["capacity"]) == ("1000000", "1000000")


async def test_nonces_strictly_increase():
    hc, _ = make({})
    nonces = [await hc.nonce() for _ in range(5)]
    assert nonces == sorted(set(nonces))


async def test_decline_withdraw_bulk_and_cancel_schedule_bodies_are_signed():
    ok = {"operation_id": "x", "status": "accepted"}
    hc, api = make({
        ("POST", f"/private/rfqs/{RFQ_ID}/declines"): ok,
        ("POST", f"/private/rfqs/{RFQ_ID}/quotes/{RFQ_ID}/withdrawals"): ok,
        ("POST", "/private/rfq-quote-withdrawals"): ok,
        ("POST", "/private/rfq-quote-cancel-schedule"): ok,
    })
    rfq = client.Rfq.from_view(maker_view(), 1)
    await hc.rfq_v2.decline(rfq)
    d = api.bodies(f"/private/rfqs/{RFQ_ID}/declines")[0]
    assert recover("DeclineRfqV2", signing.decline_message(RFQ_ID, d), d["signature"]) == SIGNER

    quote = client.Quote(RFQ_ID, RFQ_ID, 1, rfq.legs, [1, 1], 0, True, {})
    await hc.rfq_v2.withdraw(quote)
    w = api.bodies(f"/private/rfqs/{RFQ_ID}/quotes/{RFQ_ID}/withdrawals")[0]
    assert recover("InvalidateRfqV2", signing.withdraw_quote_message(RFQ_ID, RFQ_ID, w), w["signature"]) == SIGNER

    await hc.rfq_v2.withdraw_rfq(RFQ_ID)
    await hc.rfq_v2.withdraw_all()
    scopes = [b["scope"] for b in api.bodies("/private/rfq-quote-withdrawals")]
    assert scopes == [{"mode": "rfq", "rfq_id": RFQ_ID}, {"mode": "all_open"}]

    await hc.rfq_v2.cancel_after(30)
    await hc.rfq_v2.cancel_after(None)
    armed, disarmed = api.bodies("/private/rfq-quote-cancel-schedule")
    assert armed["cancel_at_ms"] > armed["expires_at_ms"] - client.ACTION_TTL_MS
    assert "cancel_at_ms" not in disarmed
    for body in (armed, disarmed):
        assert recover("ScheduleRfqQuoteCancelV2", signing.cancel_schedule_message(body), body["signature"]) == SIGNER


async def test_executions_resume_from_the_saved_cursor(tmp_path):
    pages = {0: [1, 2], 2: [3], 3: []}
    seen_after = []

    def page(request):
        after = int(request.url.params["after_sequence"])
        seen_after.append(after)
        return {
            "executions": [
                {"sequence": s, "execution_id": f"e{s}", "rfq_id": RFQ_ID, "quote_id": "q", "role": "maker",
                 "maker_account": ACCOUNT, "package_units": "1000000", "fee_micros": "0", "executed_at_ms": NOW,
                 "legs": [{"instrument": "X", "side": "sell", "quantity_micros": "500000", "price_micros": "1250000"}]}
                for s in pages[after]
            ],
            "next_after_sequence": max(pages[after], default=after),
            "has_more": False,
        }

    hc, _ = make({("GET", "/private/rfqs/executions"): page})
    cursor = tmp_path / "exec.cursor"

    async def take(n):
        out = []
        async for ex in hc.rfq_v2.executions(str(cursor), poll_s=0):
            out.append(ex)
            if len(out) == n:
                return out

    first = await take(2)
    assert [e.execution_id for e in first] == ["e1", "e2"]
    assert first[0].legs[0].contracts == Decimal("0.5") and first[0].legs[0].price_usd == Decimal("1.25")
    # The handler returned for e1 but the loop stopped inside e2: e2 is not saved.
    assert cursor.read_text() == "1"
    cursor.write_text("2")  # pretend e2 was applied before a restart
    again = await take(1)
    assert [e.execution_id for e in again] == ["e3"]
    assert seen_after[:2] == [0, 2]


class FakeSocket:
    def __init__(self, frames):
        self.frames, self.sent = list(frames), []

    async def send(self, text):
        self.sent.append(json.loads(text))

    async def recv(self):
        if not self.frames:
            await asyncio.sleep(3600)
        return json.dumps(self.frames.pop(0))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


async def test_stream_replays_the_change_feed_then_yields_full_pushes_hints_and_reauths(monkeypatch, tmp_path):
    other = "01900000-0000-7000-8000-000000000002"
    socket = FakeSocket([
        {"type": "RfqV2Authenticated", "account": ACCOUNT},
        {"type": "RfqV2Rfq", "sequence": 4, "rfq": maker_view()},  # already replayed: skipped
        {"type": "RfqV2Rfq", "sequence": 6, "rfq": maker_view(other)},
        {"type": "Error", "code": "rfq_v2_grant_expired", "channel": "rfq_v2", "message": "expired"},
        {"type": "RfqV2Changed", "sequence": 7, "rfq_id": RFQ_ID},
    ])
    monkeypatch.setattr(client.websockets, "connect", lambda *a, **k: socket)
    hc, _ = make({
        ("GET", "/private/rfqs/changes"): lambda r: {
            "changes": [{"rfq_id": RFQ_ID, "sequence": 4}] if r.url.params["after_sequence"] == "3" else [],
            "next_after_sequence": 4 if r.url.params["after_sequence"] == "3" else int(r.url.params["after_sequence"]),
            "has_more": False,
        },
        ("GET", f"/private/rfqs/{RFQ_ID}"): maker_view(),
    })
    cursor = tmp_path / "rfq.cursor"
    cursor.write_text("3")
    got = []
    async for rfq in hc.rfq_v2.stream(str(cursor)):
        got.append((rfq.id, rfq.sequence))
        if len(got) == 3:
            break
    assert got == [(RFQ_ID, 4), (other, 6), (RFQ_ID, 7)]
    assert cursor.read_text() == "6"  # the last handler had not returned yet
    kinds = [m["type"] for m in socket.sent]
    assert kinds == ["AuthenticateRfqV2", "Subscribe", "AuthenticateRfqV2"]
    assert socket.sent[1] == {"type": "Subscribe", "channel": "rfq_v2", "view": "full"}
    assert socket.sent[0]["token"] != socket.sent[2]["token"]  # re-signed after the expiry frame


async def test_manual_ack_stream_saves_only_below_the_oldest_unhandled_rfq(monkeypatch, tmp_path):
    other = "01900000-0000-7000-8000-000000000002"
    socket = FakeSocket([
        {"type": "RfqV2Authenticated", "account": ACCOUNT},
        {"type": "RfqV2Rfq", "sequence": 6, "rfq": maker_view(other)},
        {"type": "RfqV2Rfq", "sequence": 9, "rfq": maker_view()},
        {"type": "RfqV2Rfq", "sequence": 6, "rfq": maker_view(other)},  # seen: skipped
    ])
    monkeypatch.setattr(client.websockets, "connect", lambda *a, **k: socket)
    hc, _ = make({
        ("GET", "/private/rfqs/changes"): lambda r: {
            "changes": [], "next_after_sequence": int(r.url.params["after_sequence"]), "has_more": False,
        },
    })
    cursor = tmp_path / "rfq.cursor"
    cursor.write_text("3")
    got = []
    async for rfq in hc.rfq_v2.stream(str(cursor), manual_ack=True):
        got.append(rfq)
        if len(got) == 2:
            break
    assert [r.sequence for r in got] == [6, 9]
    # Both handed to tasks, neither done: the cursor stays put.
    assert cursor.read_text() == "3"
    hc.rfq_v2.ack(got[1])
    assert cursor.read_text() == "5"  # 6 is still being handled
    hc.rfq_v2.ack(got[0])
    assert cursor.read_text() == "9"


async def test_perp_order_signs_a_hypercall_directive_at_hl_price_rules(monkeypatch):
    hc, api = make({("POST", "/v1/actions/hl_limit_order"): {"status": "queued"}})

    async def info(body):
        if body["type"] == "meta":
            return {"universe": [{"name": "ETH", "szDecimals": 4}, {"name": "BTC", "szDecimals": 5}]}
        return {"BTC": "77123.5"}

    monkeypatch.setattr(hc.perps, "_info", info)
    await hc.perps.order("BTC", is_buy=False, size=0.123456, slippage_bps=20)
    body = api.bodies("/v1/actions/hl_limit_order")[0]
    action = body["action"]
    assert action["asset"] == 1 and action["isBuy"] is False and action["encodedTif"] == 3
    assert action["sz"] == 12_346_000  # 0.12346 BTC at 5 size decimals, times 1e8
    assert action["limitPx"] == 7_696_900_000_000  # 77123.5 * 0.998 -> 76969 (5 significant figures)
    assert body["account"] == ACCOUNT
    assert recover("HLOrder", signing.perp_order_message(body), body["signature"], "HypercallApiSign") == SIGNER
