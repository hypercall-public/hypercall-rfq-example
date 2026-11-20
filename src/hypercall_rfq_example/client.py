"""A small RFQ v2 maker client for the "Build an RFQ v2 maker" tutorial.

This is demo code: correct and readable rather than complete. Each method is
one REST call or one loop, so you can read it next to the API docs.
"""

from __future__ import annotations

import asyncio
import base64
import json
import math
import time
import uuid
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
import websockets
from eth_account import Account

from . import signing

GRANT_LIFETIME_MS = 270_000  # the server refuses grants longer than 5 minutes
GRANT_RENEW_MS = 60_000  # re-sign this long before expiry
ACTION_TTL_MS = 30_000  # signature lifetime for decline/withdraw/schedule
POLL_S = 10.0  # change-feed backstop while the socket is quiet
MICROS = Decimal(1_000_000)


class HypercallError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message = status, code, message


def uuid7() -> str:
    """Operation and quote IDs must be UUIDv7 (time-ordered)."""
    ms = int(time.time() * 1000)
    rand = uuid.uuid4().int
    value = (ms << 80) | (0x7 << 76) | ((rand >> 64) & 0x0FFF) << 64 | (0b10 << 62) | (rand & ((1 << 62) - 1))
    return str(uuid.UUID(int=value))


def to_micros(usd: float | Decimal) -> int:
    return int((Decimal(str(usd)) * MICROS).quantize(Decimal(1), ROUND_HALF_UP))


@dataclass
class Leg:
    instrument: str
    taker_side: str  # "buy" or "sell"; the maker takes the other side
    quantity_micros_per_unit: int

    @property
    def contracts_per_package(self) -> Decimal:
        return Decimal(self.quantity_micros_per_unit) / MICROS


@dataclass
class Rfq:
    """The maker view of one RFQ, as returned by GET /private/rfqs/{id}."""

    id: str
    sequence: int
    status: str
    terms_commitment: str
    legs: list[Leg]
    target: int  # package units, in micros
    remaining_target: int
    fill_policy: dict
    quote_deadline_ms: int
    expires_at_ms: int
    maker_generation: int | None
    rfq_generation: int | None
    raw: dict = field(repr=False)

    @classmethod
    def from_view(cls, view: dict, sequence: int) -> Rfq | None:
        parent = view["parent"]
        if parent["view"] != "maker":
            return None  # our own RFQ as taker; not ours to quote
        terms, gen = parent["terms"], view.get("maker_quote_generation") or {}
        return cls(
            id=view["rfq_id"],
            sequence=sequence,
            status=view["status"],
            terms_commitment=view["terms_commitment"],
            legs=[Leg(l["instrument"], l["taker_side"], int(l["quantity_micros_per_unit"])) for l in terms["legs"]],
            target=int(view["target"]),
            remaining_target=int(view["remaining_target"]),
            fill_policy=terms["fill_policy"],
            quote_deadline_ms=terms["quote_deadline_ms"],
            expires_at_ms=terms["expires_at_ms"],
            maker_generation=gen.get("maker_generation"),
            rfq_generation=gen.get("rfq_generation"),
            raw=view,
        )


@dataclass
class Quote:
    id: str
    rfq_id: str
    capacity: int
    legs: list[Leg]
    prices_micros: list[int]
    expires_at_ms: int
    executable: bool
    response: dict = field(repr=False)


@dataclass
class ExecutionLeg:
    instrument: str
    side: str  # our side
    contracts: Decimal
    price_usd: Decimal


@dataclass
class Execution:
    sequence: int
    execution_id: str
    rfq_id: str
    quote_id: str
    role: str
    package_units: int
    executed_at_ms: int
    legs: list[ExecutionLeg]

    @classmethod
    def from_json(cls, e: dict) -> Execution:
        return cls(
            sequence=e["sequence"],
            execution_id=e["execution_id"],
            rfq_id=e["rfq_id"],
            quote_id=e["quote_id"],
            role=e["role"],
            package_units=int(e["package_units"]),
            executed_at_ms=e["executed_at_ms"],
            legs=[
                ExecutionLeg(
                    l["instrument"], l["side"], Decimal(l["quantity_micros"]) / MICROS, Decimal(l["price_micros"]) / MICROS
                )
                for l in e["legs"]
            ],
        )


class Cursor:
    """A sequence number saved to a file, so restarts resume where they left off."""

    def __init__(self, path: str | None):
        self.path = Path(path) if path else None
        self.value = int(self.path.read_text()) if self.path and self.path.exists() else 0

    def save(self, value: int) -> None:
        if value > self.value:
            self.value = value
            if self.path:
                self.path.write_text(str(value))


class Acks:
    """A cursor that only advances past RFQs the caller has acknowledged.

    For handling RFQs concurrently: `value` is the highest sequence seen (so
    nothing is yielded twice), but the saved cursor stops just below the
    oldest RFQ still being handled. After a crash, every unacknowledged RFQ
    is replayed; ones acknowledged after it may be replayed too.
    """

    def __init__(self, cursor: Cursor):
        self.cursor = cursor
        self.value = cursor.value
        self._pending: set[int] = set()

    def hold(self, sequence: int) -> None:
        self.value = max(self.value, sequence)
        self._pending.add(sequence)

    def save(self, sequence: int) -> None:
        """A sequence passed over without being handed to the caller."""
        self.value = max(self.value, sequence)
        self._flush()

    def ack(self, sequence: int) -> None:
        self._pending.discard(sequence)
        self._flush()

    def _flush(self) -> None:
        self.cursor.save(min(self._pending) - 1 if self._pending else self.value)


class Hypercall:
    def __init__(self, host: str, account: str, signer_key: str, chain_id: int = 999, http: httpx.AsyncClient | None = None):
        self.host = host.rstrip("/")
        self.account = account.lower()
        self.key = signer_key
        self.signer = Account.from_key(signer_key).address.lower()
        self.chain_id = chain_id
        self.http = http or httpx.AsyncClient(base_url=self.host, timeout=10)
        self._clock_offset_ms: int | None = None
        self._last_nonce = 0
        self.rfq_v2 = RfqV2(self)
        self.perps = Perps(self)

    async def now_ms(self) -> int:
        """Server time, from the Date header of /health (signatures use the server clock)."""
        if self._clock_offset_ms is None:
            r = await self.http.get("/health")
            server = int(parsedate_to_datetime(r.headers["date"]).timestamp() * 1000)
            self._clock_offset_ms = server - int(time.time() * 1000)
        return int(time.time() * 1000) + self._clock_offset_ms

    async def nonce(self) -> int:
        """A millisecond timestamp, strictly increasing within this process."""
        self._last_nonce = max(await self.now_ms(), self._last_nonce + 1)
        return self._last_nonce

    def sign(self, primary: str, message: dict, domain: str = "Hypercall") -> str:
        return signing.sign(primary, message, self.key, self.chain_id, domain)

    async def request(self, method: str, path: str, *, token: str | None = None, **kwargs) -> Any:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        r = await self.http.request(method, path, headers=headers, **kwargs)
        if r.status_code >= 400:
            try:
                body = r.json()
            except ValueError:
                body = {}
            raise HypercallError(r.status_code, body.get("code", "http_error"), body.get("message", r.text))
        return r.json()


class RfqV2:
    def __init__(self, hc: Hypercall):
        self.hc = hc
        self._token: str | None = None
        self._token_expires_ms = 0
        self._acks: Acks | None = None

    # ---- read grants -------------------------------------------------------

    async def token(self) -> str:
        """A signed AuthorizeRfqReadV2 bearer token, renewed a minute before it lapses."""
        now = await self.hc.now_ms()
        if self._token is None or now > self._token_expires_ms - GRANT_RENEW_MS:
            expires = now + GRANT_LIFETIME_MS
            nonce = await self.hc.nonce()
            grant = {
                "account": self.hc.account,
                "issued_at_ms": now,
                "expires_at_ms": expires,
                "nonce": nonce,
                "signature": self.hc.sign("AuthorizeRfqReadV2", signing.read_grant_message(self.hc.account, now, expires, nonce)),
            }
            raw = json.dumps(grant, separators=(",", ":")).encode()
            self._token, self._token_expires_ms = base64.urlsafe_b64encode(raw).rstrip(b"=").decode(), expires
        return self._token

    # ---- reads ---------------------------------------------------------------

    async def provider(self) -> dict:
        """This account's quote-provider enrollment and status."""
        return await self.hc.request("GET", "/private/rfq-provider", token=await self.token())

    async def get(self, rfq_id: str, sequence: int = 0) -> Rfq | None:
        view = await self.hc.request("GET", f"/private/rfqs/{rfq_id}", token=await self.token())
        return Rfq.from_view(view, sequence)

    async def changes(self, after: int) -> dict:
        params = {"after_sequence": after, "limit": 100}
        return await self.hc.request("GET", "/private/rfqs/changes", params=params, token=await self.token())

    # ---- intake --------------------------------------------------------------

    async def stream(self, cursor_file: str | None = None, manual_ack: bool = False) -> AsyncIterator[Rfq]:
        """Yield each RFQ change routed to us, in sequence order, forever.

        Subscribes to `rfq_v2` with `view: full`, so most RFQs arrive complete.
        On every (re)connect, and whenever the socket is quiet, the change feed
        replays anything missed since the saved cursor. By default the cursor
        is saved when your loop body returns, so a crash never skips an RFQ.
        If you hand each RFQ to a task instead of handling it in the loop
        body, pass `manual_ack=True` and call `ack(rfq)` when the task is
        done: the cursor then never moves past an RFQ still being handled.
        """
        cursor: Cursor | Acks = Cursor(cursor_file)
        if manual_ack:
            cursor = self._acks = Acks(cursor)

        def handed(rfq: Rfq) -> None:
            if manual_ack:
                cursor.hold(rfq.sequence)

        def done(rfq: Rfq) -> None:
            if not manual_ack:
                cursor.save(rfq.sequence)
        ws_url = "ws" + self.hc.host.removeprefix("http") + "/ws"
        while True:
            try:
                async with websockets.connect(ws_url, max_size=2**22) as ws:
                    await self._authenticate(ws)
                    await ws.send(json.dumps({"type": "Subscribe", "channel": "rfq_v2", "view": "full"}))
                    async for rfq in self._replay(cursor):
                        handed(rfq)
                        yield rfq
                        done(rfq)
                    while True:
                        if self._token_expires_ms - await self.hc.now_ms() < GRANT_RENEW_MS:
                            await self._authenticate(ws)  # keeps the subscription
                        try:
                            frame = json.loads(await asyncio.wait_for(ws.recv(), POLL_S))
                        except asyncio.TimeoutError:
                            async for rfq in self._replay(cursor):
                                handed(rfq)
                                yield rfq
                                done(rfq)
                            continue
                        rfq = await self._from_frame(ws, frame, cursor)
                        if rfq is not None:
                            handed(rfq)
                            yield rfq
                            done(rfq)
            except (OSError, websockets.ConnectionClosed) as error:
                print(f"rfq_v2 socket dropped ({error}); reconnecting")
                await asyncio.sleep(1)

    def ack(self, rfq: Rfq) -> None:
        """Mark an RFQ from `stream(manual_ack=True)` as handled."""
        if self._acks is None:
            raise RuntimeError("ack() needs stream(manual_ack=True)")
        self._acks.ack(rfq.sequence)

    async def _authenticate(self, ws) -> None:
        await ws.send(json.dumps({"type": "AuthenticateRfqV2", "token": await self.token()}))

    async def _from_frame(self, ws, frame: dict, cursor: Cursor | Acks) -> Rfq | None:
        kind = frame.get("type")
        if kind == "Error" and frame.get("code") == "rfq_v2_grant_expired":
            self._token = None  # the server says our grant lapsed: sign a new one
            await self._authenticate(ws)
            return None
        if kind == "Error":
            raise HypercallError(0, frame.get("code") or "ws_error", frame.get("message", ""))
        if kind not in ("RfqV2Rfq", "RfqV2Changed") or frame["sequence"] <= cursor.value:
            return None
        if kind == "RfqV2Rfq":
            return Rfq.from_view(frame["rfq"], frame["sequence"])
        return await self.get(frame["rfq_id"], frame["sequence"])  # hint only: read it

    async def _replay(self, cursor: Cursor | Acks) -> AsyncIterator[Rfq]:
        after = cursor.value
        while True:
            page = await self.changes(after)
            for change in page["changes"]:
                rfq = await self.get(change["rfq_id"], change["sequence"])
                if rfq is not None:
                    yield rfq
                cursor.save(change["sequence"])
            if page["next_after_sequence"] == after or not page["has_more"]:
                cursor.save(page["next_after_sequence"])
                return
            after = page["next_after_sequence"]

    # ---- quoting -------------------------------------------------------------

    async def quote(self, rfq: Rfq, prices: list[float], capacity: int | None = None, expires_in_s: float = 20) -> Quote:
        """Sign and post a firm quote. `prices` are USD per contract, one per leg."""
        policy = rfq.fill_policy
        if policy["mode"] == "full_size_once":
            capacity, reference, step = rfq.target, rfq.target, None
        else:
            step = int(policy["step"])
            capacity, reference = capacity or rfq.remaining_target, step
        micros = exact_prices(rfq.legs, [to_micros(p) for p in prices], reference, step)
        receives, premium = signing.reference_premium(
            [l.__dict__ for l in rfq.legs], micros, reference, step
        )
        now = await self.hc.now_ms()
        terms = {
            "id": uuid7(),
            "rfq_id": rfq.id,
            "rfq_terms_commitment": rfq.terms_commitment,
            "maker_account": self.hc.account,
            # Copied from the read, never computed: a withdrawal bumps them.
            "maker_generation": rfq.maker_generation,
            "rfq_generation": rfq.rfq_generation,
            "priced_legs": [{"instrument": l.instrument, "price_micros": str(p)} for l, p in zip(rfq.legs, micros)],
            "reference_quantity": str(reference),
            "maker_receives_reference_premium": receives,
            "reference_premium_micros": str(premium),
            "capacity": str(capacity),
            "fill_policy": policy,
            "withdrawal_policy": "permitted",
            "fee_limits": {"per_execution": "0", "cumulative": "0"},
            "expires_at_ms": min(rfq.expires_at_ms, now + int(expires_in_s * 1000)),
            "protection_scope": None,
            "source_indicative_quote_id": None,
        }
        nonce = await self.hc.nonce()
        body = {"terms": terms, "nonce": nonce, "signature": self.hc.sign("AuthorizeRfqQuoteV2", signing.firm_quote_message(terms, nonce))}
        r = await self.hc.request("POST", f"/private/rfqs/{rfq.id}/quotes/firm", json=body)
        return Quote(terms["id"], rfq.id, capacity, rfq.legs, micros, terms["expires_at_ms"], r["executable"], r)

    async def _action(self) -> dict:
        now = await self.hc.now_ms()
        return {"operation_id": uuid7(), "expires_at_ms": now + ACTION_TTL_MS, "nonce": await self.hc.nonce()}

    async def decline(self, rfq: Rfq) -> dict:
        body = {**await self._action(), "maker_account": self.hc.account}
        body["signature"] = self.hc.sign("DeclineRfqV2", signing.decline_message(rfq.id, body))
        return await self.hc.request("POST", f"/private/rfqs/{rfq.id}/declines", json=body)

    async def withdraw(self, quote: Quote) -> dict:
        body = await self._action()
        body["signature"] = self.hc.sign("InvalidateRfqV2", signing.withdraw_quote_message(quote.rfq_id, quote.id, body))
        return await self.hc.request("POST", f"/private/rfqs/{quote.rfq_id}/quotes/{quote.id}/withdrawals", json=body)

    async def _bulk(self, scope: dict) -> dict:
        body = {**await self._action(), "maker_account": self.hc.account, "scope": scope}
        body["signature"] = self.hc.sign("BulkWithdrawRfqV2", signing.bulk_withdrawal_message(body))
        return await self.hc.request("POST", "/private/rfq-quote-withdrawals", json=body)

    async def withdraw_rfq(self, rfq_id: str) -> dict:
        """Withdraw every quote we have on one RFQ (bumps that RFQ's generation)."""
        return await self._bulk({"mode": "rfq", "rfq_id": rfq_id})

    async def withdraw_all(self) -> dict:
        """Withdraw every open quote (bumps our maker generation)."""
        return await self._bulk({"mode": "all_open"})

    async def cancel_after(self, seconds: float | None) -> dict:
        """Dead-man switch: withdraw everything unless renewed within `seconds`. None disarms it."""
        body = {**await self._action(), "maker_account": self.hc.account}
        if seconds is not None:
            body["cancel_at_ms"] = await self.hc.now_ms() + int(seconds * 1000)
        body["signature"] = self.hc.sign("ScheduleRfqQuoteCancelV2", signing.cancel_schedule_message(body))
        return await self.hc.request("POST", "/private/rfq-quote-cancel-schedule", json=body)

    # ---- fills ---------------------------------------------------------------

    async def executions(self, cursor_file: str | None = None, poll_s: float = 1.0) -> AsyncIterator[Execution]:
        """Yield every execution against our quotes exactly once, in order.

        Resumes from the saved cursor after a restart; the cursor is saved only
        after your loop body returns. Deduplicate on `execution_id` if your
        handler is not idempotent across a crash mid-body.
        """
        cursor = Cursor(cursor_file)
        while True:
            params = {"after_sequence": cursor.value, "limit": 100}
            page = await self.hc.request("GET", "/private/rfqs/executions", params=params, token=await self.token())
            for e in page["executions"]:
                yield Execution.from_json(e)
                cursor.save(e["sequence"])
            if not page["has_more"]:
                await asyncio.sleep(poll_s)


def price_granule(leg: Leg, reference: int, step: int | None) -> int:
    """Smallest price step, in micros, that keeps this leg's premium whole.

    A leg contributes price * quantity_micros_per_unit * units / 10^12 micros
    of premium, so its price must be a multiple of
    10^12 / gcd(quantity_micros_per_unit * units, 10^12) for the reference
    quantity and, for partial-fill policies, for one step. At 0.01 contracts
    per package that is 100 micros; at whole contracts it is 1.
    """
    granule = 1
    for units in (reference, step):
        if units:
            g = signing._SCALE // math.gcd(leg.quantity_micros_per_unit * units, signing._SCALE)
            granule = granule * g // math.gcd(granule, g)
    return granule


def exact_prices(
    legs: list[Leg], micros: list[int], reference: int, step: int | None, max_move: float = 0.01
) -> list[int]:
    """Prices the signed premium can carry exactly, moved in the maker's favour.

    The reference premium is sum(+/- price * contracts) and must be a whole
    number of micros (and so must one step's, for partial fills). Unchanged
    prices are kept when they are already exact. Otherwise every leg is
    rounded to a multiple of its `price_granule`, up on a leg the taker buys
    and down on a leg the taker sells, which makes each leg's premium, and so
    the sum, exact. A sub-contract package can need a coarse step (100 micros
    at 0.01 contracts); a price that would move by more than `max_move`
    (relative) or reach zero is refused rather than quoted.
    """
    raw = [leg.__dict__ for leg in legs]
    if all(m > 0 for m in micros) and signing.reference_premium(raw, micros, reference, step):
        return list(micros)
    fixed = []
    for leg, price in zip(legs, micros, strict=True):
        granule = price_granule(leg, reference, step)
        if leg.taker_side == "buy":
            rounded = -(-price // granule) * granule
        else:
            rounded = price // granule * granule
        if rounded <= 0 or abs(rounded - price) > max_move * price:
            raise ValueError(
                f"{leg.instrument}: at this size the price must be a multiple of {granule} micros, "
                f"which moves {price} too far; adjust the price or the size"
            )
        fixed.append(rounded)
    if not signing.reference_premium(raw, fixed, reference, step):
        raise ValueError("leg prices do not make the premium exact; adjust leg prices")
    return fixed


class Perps:
    """Hyperliquid perp orders placed through Hypercall from the same account."""

    def __init__(self, hc: Hypercall, info_url: str | None = None):
        self.hc = hc
        self.info_url = info_url or (
            "https://api.hyperliquid.xyz/info" if hc.chain_id == 999 else "https://api.hyperliquid-testnet.xyz/info"
        )
        self._meta: dict[str, tuple[int, int]] | None = None

    async def _info(self, body: dict) -> Any:
        async with httpx.AsyncClient(timeout=10) as http:
            r = await http.post(self.info_url, json=body)
            r.raise_for_status()
            return r.json()

    async def asset(self, coin: str) -> tuple[int, int]:
        """(asset index, size decimals) from Hyperliquid's perp meta."""
        if self._meta is None:
            universe = (await self._info({"type": "meta"}))["universe"]
            self._meta = {a["name"]: (i, a["szDecimals"]) for i, a in enumerate(universe)}
        return self._meta[coin]

    async def order(self, coin: str, is_buy: bool, size: float, type: str = "ioc", slippage_bps: float = 20, reduce_only: bool = False) -> dict:
        """Marketable limit order at mid +/- slippage. Returns the directive submit response."""
        asset, sz_decimals = await self.asset(coin)
        mid = Decimal((await self._info({"type": "allMids"}))[coin])
        px = mid * (1 + Decimal(slippage_bps) / 10_000 * (1 if is_buy else -1))
        px = hl_price(px, sz_decimals)
        sz = Decimal(str(size)).quantize(Decimal(1).scaleb(-sz_decimals))
        if sz <= 0:
            raise ValueError(f"{size} {coin} rounds to zero at {sz_decimals} size decimals")
        nonce = await self.hc.nonce()
        action = {
            "asset": asset,
            "isBuy": is_buy,
            "limitPx": int(px * 10**8),  # CoreWriter units: value * 1e8
            "sz": int(sz * 10**8),
            "reduceOnly": reduce_only,
            "encodedTif": {"alo": 1, "gtc": 2, "ioc": 3}[type],
            "cloid": nonce,
        }
        body = {"account": self.hc.account, "nonce": nonce, "action": action}
        body["signature"] = self.hc.sign("HLOrder", signing.perp_order_message(body), domain="HypercallApiSign")
        return await self.hc.request("POST", "/v1/actions/hl_limit_order", json=body)


def hl_price(px: Decimal, sz_decimals: int) -> Decimal:
    """Hyperliquid perp prices: at most 5 significant figures and 6 - szDecimals decimals."""
    digits = px.adjusted() + 1
    px = px.quantize(Decimal(1).scaleb(min(digits - 5, 0)))
    return px.quantize(Decimal(1).scaleb(-(6 - sz_decimals)))
