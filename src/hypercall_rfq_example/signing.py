"""EIP-712 messages a maker signs for RFQ v2, plus the reference premium.

Every type below mirrors the Rust signer in
`public-crates/hypercall-client/src/rfq_v2_signing.rs` field for field: names,
order and widths are part of the type hash. The parity test signs the Rust
golden vectors with this module and compares the bytes.

Builders take the same JSON shapes the REST API uses (snake_case keys,
amounts as decimal strings), so a request body and its signed message cannot
drift apart.
"""

from __future__ import annotations

import uuid
from typing import Any

from eth_account import Account
from eth_account.messages import _hash_eip191_message, encode_typed_data
from eth_utils import keccak

_LEG = [
    {"name": "instrumentId", "type": "bytes32"},
    {"name": "priceMicros", "type": "uint128"},
]
_FEES = [
    {"name": "perExecutionMicros", "type": "uint128"},
    {"name": "cumulativeMicros", "type": "uint128"},
]

TYPES: dict[str, dict[str, list[dict[str, str]]]] = {
    "AuthorizeRfqReadV2": {
        "AuthorizeRfqReadV2": [
            {"name": "account", "type": "address"},
            {"name": "issuedAtMs", "type": "uint64"},
            {"name": "expiresAtMs", "type": "uint64"},
            {"name": "nonce", "type": "uint64"},
        ]
    },
    "AuthorizeRfqQuoteV2": {
        "AuthorizeRfqQuoteV2": [
            {"name": "terms", "type": "RfqFirmQuoteV2SigningTerms"},
            {"name": "nonce", "type": "uint64"},
        ],
        "RfqFirmQuoteV2SigningTerms": [
            {"name": "quoteId", "type": "bytes16"},
            {"name": "rfqId", "type": "bytes16"},
            {"name": "rfqTermsCommitment", "type": "bytes32"},
            {"name": "makerAccount", "type": "address"},
            {"name": "makerGeneration", "type": "uint64"},
            {"name": "rfqGeneration", "type": "uint64"},
            {"name": "pricedLegs", "type": "RfqV2SigningPriceLeg[]"},
            {"name": "referenceQuantity", "type": "uint128"},
            {"name": "makerReceivesReferencePremium", "type": "bool"},
            {"name": "referencePremiumMicros", "type": "uint128"},
            {"name": "capacity", "type": "uint128"},
            {"name": "fillPolicy", "type": "uint8"},
            {"name": "minimumUnits", "type": "uint128"},
            {"name": "stepUnits", "type": "uint128"},
            {"name": "withdrawalPolicy", "type": "uint8"},
            {"name": "feeLimits", "type": "RfqV2SigningFeeLimits"},
            {"name": "expiresAtMs", "type": "uint64"},
            {"name": "protectionScope", "type": "bytes32"},
            {"name": "hasSourceIndicativeQuote", "type": "bool"},
            {"name": "sourceIndicativeQuoteId", "type": "bytes16"},
        ],
        "RfqV2SigningPriceLeg": _LEG,
        "RfqV2SigningFeeLimits": _FEES,
    },
    "DeclineRfqV2": {
        "DeclineRfqV2": [
            {"name": "rfqId", "type": "bytes16"},
            {"name": "makerAccount", "type": "address"},
            {"name": "operationId", "type": "bytes16"},
            {"name": "expiresAtMs", "type": "uint64"},
            {"name": "nonce", "type": "uint64"},
        ]
    },
    "InvalidateRfqV2": {
        "InvalidateRfqV2": [
            {"name": "rfqId", "type": "bytes16"},
            {"name": "quoteId", "type": "bytes16"},
            {"name": "scope", "type": "uint8"},
            {"name": "reason", "type": "uint8"},
            {"name": "operationId", "type": "bytes16"},
            {"name": "expiresAtMs", "type": "uint64"},
            {"name": "nonce", "type": "uint64"},
        ]
    },
    "BulkWithdrawRfqV2": {
        "BulkWithdrawRfqV2": [
            {"name": "makerAccount", "type": "address"},
            {"name": "scope", "type": "uint8"},
            {"name": "rfqId", "type": "bytes16"},
            {"name": "targets", "type": "RfqV2BulkWithdrawalTarget[]"},
            {"name": "operationId", "type": "bytes16"},
            {"name": "expiresAtMs", "type": "uint64"},
            {"name": "nonce", "type": "uint64"},
        ],
        "RfqV2BulkWithdrawalTarget": [
            {"name": "rfqId", "type": "bytes16"},
            {"name": "quoteId", "type": "bytes16"},
        ],
    },
    "ScheduleRfqQuoteCancelV2": {
        "ScheduleRfqQuoteCancelV2": [
            {"name": "operationId", "type": "bytes16"},
            {"name": "makerAccount", "type": "address"},
            {"name": "cancelAtMs", "type": "uint64"},
            {"name": "expiresAtMs", "type": "uint64"},
            {"name": "nonce", "type": "uint64"},
        ]
    },
    # Perp orders use the directive domain "HypercallApiSign", not "Hypercall".
    "HLOrder": {
        "HLOrder": [
            {"name": "account", "type": "address"},
            {"name": "nonce", "type": "uint64"},
            {"name": "action", "type": "LimitOrder"},
        ],
        "LimitOrder": [
            {"name": "asset", "type": "uint32"},
            {"name": "isBuy", "type": "bool"},
            {"name": "limitPx", "type": "uint64"},
            {"name": "sz", "type": "uint64"},
            {"name": "reduceOnly", "type": "bool"},
            {"name": "encodedTif", "type": "uint8"},
            {"name": "cloid", "type": "uint128"},
        ],
    },
}

FILL_POLICY = {"full_size_once": 0, "one_smaller_package": 1, "repeated_partial": 2}
ZERO16 = b"\x00" * 16
ZERO32 = b"\x00" * 32


def instrument_id(symbol: str) -> bytes:
    """Signed instrument identity: keccak256("instrument" || symbol)."""
    return keccak(b"instrument" + symbol.encode())


def uid(value: str | uuid.UUID) -> bytes:
    return uuid.UUID(str(value)).bytes


def b32(hex_value: str) -> bytes:
    return bytes.fromhex(hex_value.removeprefix("0x"))


def digest(primary: str, message: dict[str, Any], chain_id: int, domain: str = "Hypercall") -> bytes:
    """The 32-byte EIP-712 signing hash."""
    return _hash_eip191_message(_signable(primary, message, chain_id, domain))


def sign(primary: str, message: dict[str, Any], key: str, chain_id: int, domain: str = "Hypercall") -> str:
    """Sign and return the 65-byte signature as lowercase 0x hex (v = 27 or 28)."""
    signed = Account.sign_message(_signable(primary, message, chain_id, domain), key)
    return "0x" + signed.signature.hex().removeprefix("0x")


def _signable(primary: str, message: dict[str, Any], chain_id: int, domain: str):
    types = {
        "EIP712Domain": [
            {"name": "name", "type": "string"},
            {"name": "version", "type": "string"},
            {"name": "chainId", "type": "uint256"},
            {"name": "verifyingContract", "type": "address"},
        ],
        **TYPES[primary],
    }
    return encode_typed_data(
        full_message={
            "types": types,
            "primaryType": primary,
            "domain": {
                "name": domain,
                "version": "1",
                "chainId": chain_id,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "message": message,
        }
    )


# ---- message builders: wire JSON in, EIP-712 message out -------------------


def read_grant_message(account: str, issued_at_ms: int, expires_at_ms: int, nonce: int) -> dict:
    return {"account": account, "issuedAtMs": issued_at_ms, "expiresAtMs": expires_at_ms, "nonce": nonce}


def firm_quote_message(terms: dict, nonce: int) -> dict:
    """`terms` is the `terms` object of POST /private/rfqs/{id}/quotes/firm."""
    policy = terms["fill_policy"]
    source = terms.get("source_indicative_quote_id")
    scope = terms.get("protection_scope")
    return {
        "terms": {
            "quoteId": uid(terms["id"]),
            "rfqId": uid(terms["rfq_id"]),
            "rfqTermsCommitment": b32(terms["rfq_terms_commitment"]),
            "makerAccount": terms["maker_account"],
            "makerGeneration": terms["maker_generation"],
            "rfqGeneration": terms["rfq_generation"],
            "pricedLegs": [
                {"instrumentId": instrument_id(leg["instrument"]), "priceMicros": int(leg["price_micros"])}
                for leg in terms["priced_legs"]
            ],
            "referenceQuantity": int(terms["reference_quantity"]),
            "makerReceivesReferencePremium": terms["maker_receives_reference_premium"],
            "referencePremiumMicros": int(terms["reference_premium_micros"]),
            "capacity": int(terms["capacity"]),
            "fillPolicy": FILL_POLICY[policy["mode"]],
            "minimumUnits": int(policy.get("minimum", 0)),
            "stepUnits": int(policy.get("step", 0)),
            "withdrawalPolicy": 1 if terms["withdrawal_policy"] == "permitted" else 0,
            "feeLimits": {
                "perExecutionMicros": int(terms["fee_limits"]["per_execution"]),
                "cumulativeMicros": int(terms["fee_limits"]["cumulative"]),
            },
            "expiresAtMs": terms["expires_at_ms"],
            "protectionScope": b32(scope) if scope else ZERO32,
            "hasSourceIndicativeQuote": source is not None,
            "sourceIndicativeQuoteId": uid(source) if source else ZERO16,
        },
        "nonce": nonce,
    }


def decline_message(rfq_id: str, body: dict) -> dict:
    return {
        "rfqId": uid(rfq_id),
        "makerAccount": body["maker_account"],
        "operationId": uid(body["operation_id"]),
        "expiresAtMs": body["expires_at_ms"],
        "nonce": body["nonce"],
    }


def withdraw_quote_message(rfq_id: str, quote_id: str, body: dict) -> dict:
    # scope 1 / reason 1 is "maker withdraws a quote"; both are signed so a
    # withdrawal can't be replayed as an RFQ cancellation.
    return {
        "rfqId": uid(rfq_id),
        "quoteId": uid(quote_id),
        "scope": 1,
        "reason": 1,
        "operationId": uid(body["operation_id"]),
        "expiresAtMs": body["expires_at_ms"],
        "nonce": body["nonce"],
    }


def bulk_withdrawal_message(body: dict) -> dict:
    scope = body["scope"]
    tag = {"quote_ids": 0, "rfq": 1, "all_open": 2}[scope["mode"]]
    return {
        "makerAccount": body["maker_account"],
        "scope": tag,
        "rfqId": uid(scope["rfq_id"]) if tag == 1 else ZERO16,
        "targets": [
            {"rfqId": uid(t["rfq_id"]), "quoteId": uid(t["quote_id"])} for t in scope.get("quote_ids", [])
        ],
        "operationId": uid(body["operation_id"]),
        "expiresAtMs": body["expires_at_ms"],
        "nonce": body["nonce"],
    }


def cancel_schedule_message(body: dict) -> dict:
    return {
        "operationId": uid(body["operation_id"]),
        "makerAccount": body["maker_account"],
        "cancelAtMs": body.get("cancel_at_ms") or 0,  # zero removes the schedule
        "expiresAtMs": body["expires_at_ms"],
        "nonce": body["nonce"],
    }


def perp_order_message(body: dict) -> dict:
    """`body` is the POST /v1/actions/hl_limit_order request."""
    a = body["action"]
    return {
        "account": body["account"],
        "nonce": int(body["nonce"]),
        "action": {
            "asset": a["asset"],
            "isBuy": a["isBuy"],
            "limitPx": int(a["limitPx"]),
            "sz": int(a["sz"]),
            "reduceOnly": a["reduceOnly"],
            "encodedTif": a["encodedTif"],
            "cloid": int(a["cloid"]),
        },
    }


# ---- premium ---------------------------------------------------------------

_SCALE = 10**12  # prices and quantities are both micros


def reference_premium(legs: list[dict], prices_micros: list[int], reference_quantity: int, step: int | None):
    """(maker_receives, premium_micros) for a quote, or None if not exact.

    A taker buy leg pays the maker, a taker sell leg pays the taker. The
    server recomputes this and refuses any other value. For partial-fill
    policies the premium of one `step` must also be whole micros.
    Mirrors `rfq_v2_reference_premium` in Rust.
    """
    maker = taker = 0
    for leg, price in zip(legs, prices_micros, strict=True):
        scaled = price * int(leg["quantity_micros_per_unit"]) * reference_quantity
        if leg["taker_side"] == "buy":
            maker += scaled
        else:
            taker += scaled
    receives, scaled = (True, maker - taker) if maker > taker else (False, taker - maker)
    if step is not None and (scaled // reference_quantity * step) % _SCALE:
        return None
    if scaled % _SCALE:
        return None
    return receives, scaled // _SCALE
