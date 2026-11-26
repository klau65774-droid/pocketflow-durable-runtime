from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pocketflow_runtime.guards import SnapshotLimitError


def _luhn_valid(digits: str) -> bool:
    if len(digits) < 13 or len(digits) > 19 or not digits.isdigit():
        return False
    total = 0
    reverse = digits[::-1]
    for idx, ch in enumerate(reverse):
        num = ord(ch) - 48
        if idx % 2 == 1:
            num *= 2
            if num > 9:
                num -= 9
        total += num
    return total % 10 == 0


def _cn_id_valid(candidate: str) -> bool:
    if len(candidate) != 18 or not candidate[:17].isdigit():
        return False
    weights = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
    check_map = "10X98765432"
    checksum = sum(int(candidate[i]) * weights[i] for i in range(17)) % 11
    return candidate[17].upper() == check_map[checksum]


_PEM_MARKER = re.compile(
    r"-----(BEGIN|END) ((?:(?:RSA|EC|DSA|OPENSSH|ENCRYPTED) )?PRIVATE KEY)-----"
)

_REGEX_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("openai_or_anthropic_key", re.compile(r"\bsk-[A-Za-z0-9_-]{20,}")),
    ("stripe_live_key", re.compile(r"\b(?:sk|rk|pk)_live_[A-Za-z0-9]{16,}")),
    ("github_pat", re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}")),
    ("github_fine_pat", re.compile(r"\bgithub_pat_[A-Za-z0-9_]{24,}")),
    ("aws_access_key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("google_api_key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}(?![0-9A-Za-z_-])")),
    ("slack_token", re.compile(r"\bxox[baprs]-[0-9A-Za-z-]{16,}")),
    (
        "jwt_token",
        re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"),
    ),
    (
        "db_uri_password",
        re.compile(
            r"\b(?:postgres(?:ql)?|mysql|rediss?|mongodb(?:\+srv)?)://"
            r"[^\s:/@]+:[^\s/@]+@[^\s/]+",
            re.IGNORECASE,
        ),
    ),
    (
        "bearer_header_token",
        re.compile(r"(?i)\bAuthorization[ \t]*:[ \t]*Bearer[ \t]+[A-Za-z0-9._~+/=-]+"),
    ),
    (
        "secret_kv_assignment",
        re.compile(
            r"(?i)\b(?:api_key|secret_key|access_token|password|passwd)"
            r"[ \t]*[:=][ \t]*['\"]?[A-Za-z0-9._~+/=-]{10,}['\"]?"
        ),
    ),
    (
        "email_address",
        re.compile(r"(?<![\w.%+-])[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}\b"),
    ),
)

# Chinese mobile numbers only match when separated from alphanumeric order-ID prefixes
# (e.g. ORD-91002348712 or #W9100234871 is preserved as a benign order ID decoy)
# OR when explicitly labeled as phone/mobile/tel/手机.
_CN_MOBILE_LABELED = re.compile(
    r"(?i)(?<![A-Za-z])(?:phone|mobile|tel|手机|电话)[ \t]*[:=：][ \t]*"
    r"(?:\+?86[- ]?)?1[3-9][0-9]{9}\b"
)
_CN_MOBILE_STANDALONE = re.compile(
    r"(?<![A-Za-z0-9_#/-])(?:\+?86[- ]?)?1[3-9][0-9]{9}(?![A-Za-z0-9_-])"
)
_CN_ID_PATTERN = re.compile(r"(?<![A-Za-z0-9])[0-9]{17}[0-9Xx](?![A-Za-z0-9])")
_CARD_CANDIDATE = re.compile(
    r"(?<![A-Za-z0-9-])[0-9](?:[ -]?[0-9]){12,18}(?![A-Za-z0-9-]|[ -][0-9])"
)

_BENIGN_ORDER_FIELD_KEYS = frozenset(
    {"order_id", "tracking_id", "invoice_id", "reservation_id", "flight_id"}
)
_SENSITIVE_FIELDS = frozenset({
    "api_key", "secret_key", "client_secret", "password", "passwd",
    "access_token", "refresh_token", "authorization", "proxy_authorization",
    "cookie", "set_cookie", "private_key", "aws_secret_access_key", "x_api_key",
})
_REDACTION_MARKER = re.compile(r"\[REDACTED:[a-z_]+\]")


@dataclass(frozen=True, slots=True)
class RedactionResult:
    """Result of redacting a string or nested payload."""

    redacted: Any
    findings_count: int
    matched_rules: tuple[str, ...]


def redact_text(text: str, *, field_name: str | None = None) -> tuple[str, list[str]]:
    """Redact secrets and PII from a string with low false-positive rate on decoys."""
    if not text:
        return text, []

    rules: list[str] = []
    pieces: list[str] = []
    copied_to = 0
    pem_label = None
    for marker in _PEM_MARKER.finditer(text):
        if pem_label is None and marker.group(1) == "BEGIN":
            pieces.extend((text[copied_to:marker.start()], "[REDACTED:pem_private_key]"))
            rules.append("pem_private_key")
            pem_label = marker.group(2)
        elif marker.group(1) == "END" and marker.group(2) == pem_label:
            copied_to = marker.end()
            pem_label = None
    if pem_label is None:
        pieces.append(text[copied_to:])
    out = "".join(pieces)

    for rule_name, pattern in _REGEX_RULES:
        out, count = pattern.subn(f"[REDACTED:{rule_name}]", out)
        rules.extend([rule_name] * count)

    def _replace_cn_id(match: re.Match[str]) -> str:
        val = match.group(0)
        if _cn_id_valid(val):
            rules.append("cn_national_id")
            return "[REDACTED:cn_national_id]"
        return val

    out = _CN_ID_PATTERN.sub(_replace_cn_id, out)

    def _replace_labeled_phone(match: re.Match[str]) -> str:
        rules.append("cn_mobile")
        return "[REDACTED:cn_mobile]"

    out = _CN_MOBILE_LABELED.sub(_replace_labeled_phone, out)

    if field_name not in _BENIGN_ORDER_FIELD_KEYS:
        def _replace_standalone_phone(match: re.Match[str]) -> str:
            rules.append("cn_mobile")
            return "[REDACTED:cn_mobile]"

        out = _CN_MOBILE_STANDALONE.sub(_replace_standalone_phone, out)

    def _replace_card(match: re.Match[str]) -> str:
        raw = match.group(0)
        digits = "".join(ch for ch in raw if ch.isdigit())
        if _luhn_valid(digits):
            rules.append("payment_card")
            return "[REDACTED:payment_card]"
        return raw

    out = _CARD_CANDIDATE.sub(_replace_card, out)
    return out, rules


def redact_payload(payload: Any) -> RedactionResult:
    """Recursively redact secrets/PII from dicts, lists, and strings."""
    matched: list[str] = []

    def _walk(obj: Any, field_name: str | None = None, depth: int = 0) -> Any:
        if depth > 36:
            raise SnapshotLimitError("Redaction payload exceeds maximum nesting depth")
        if field_name in _SENSITIVE_FIELDS and obj is not None and obj != "":
            if isinstance(obj, str) and _REDACTION_MARKER.fullmatch(obj):
                return obj
            matched.append("sensitive_field")
            return "[REDACTED:sensitive_field]"
        if isinstance(obj, str):
            cleaned, rules = redact_text(obj, field_name=field_name)
            matched.extend(rules)
            return cleaned
        if isinstance(obj, dict):
            redacted: dict[str, Any] = {}
            reserved_keys = {str(k) for k in obj}
            next_suffix: dict[str, int] = {}
            for key, val in obj.items():
                key_text = str(key)
                cleaned_key, key_rules = redact_text(key_text)
                matched.extend(key_rules)
                if cleaned_key != key_text:
                    base_key = cleaned_key
                    suffix = next_suffix.get(base_key, 1)
                    while cleaned_key in reserved_keys or cleaned_key in redacted:
                        cleaned_key = f"{base_key}#{suffix}"
                        suffix += 1
                    next_suffix[base_key] = suffix
                redacted[cleaned_key] = _walk(val, key_text.lower().replace("-", "_"), depth + 1)
            return redacted
        if isinstance(obj, (list, tuple)):
            return [_walk(item, field_name, depth + 1) for item in obj]
        if isinstance(obj, int) and field_name in {"phone", "mobile", "tel", "id_card"}:
            cleaned, rules = redact_text(str(obj), field_name=field_name)
            if rules:
                matched.extend(rules)
                return cleaned
        return obj

    cleaned_payload = _walk(payload)
    unique_rules = tuple(dict.fromkeys(matched))
    return RedactionResult(
        redacted=cleaned_payload,
        findings_count=len(matched),
        matched_rules=unique_rules,
    )


def build_builtin_redaction_cases() -> tuple[
    list[tuple[str, dict[str, Any]]],
    list[tuple[str, dict[str, Any]]],
]:
    """Construct 15 synthetic secret/PII payloads and 18 benign decoys dynamically."""
    secrets: list[tuple[str, dict[str, Any]]] = [
        ("openai_key", {"msg": "sk" + "-proj-" + "AbCdEfGh1234567890IjKlMn"}),
        ("anthropic_key", {"msg": "sk" + "-ant-" + "api03-XyZaBc1234567890QrStUv"}),
        ("github_pat", {"msg": "ghp_" + "1234567890abcdefghijklmnopqr"}),
        ("github_fine_pat", {"msg": "github_pat_" + "11AAABBB000_xyz1234567890abcde"}),
        ("aws_key", {"msg": "AKIA" + "IOSFODNN7" + "EXAMPLE"}),
        ("google_key", {"msg": "AIza" + "SyDaGmWKa4JsXZ12" + "1234567890abcDEFghi"}),
        ("slack_bot", {"msg": "xoxb-" + "123456789012-123456789012-abcdef"}),
        ("stripe_live", {"msg": "rk_live_" + "51AbCdEf1234567890xyz"}),
        (
            "jwt",
            {
                "msg": "eyJhbGciOiJIUzI1NiJ9."
                "eyJzdWIiOiJ1c2VyXzEifQ."
                "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
            },
        ),
        (
            "pem_key",
            {
                "msg": (
                    "-----BEGIN "
                    + "RSA PRIVATE KEY-----\n"
                    + "MIIEowIBAAKCAQEA123456\n"
                    + "-----END "
                    + "RSA PRIVATE KEY-----"
                )
            },
        ),
        ("bearer_hdr", {"msg": "Authorization: Bearer " + "tok_live_9988776655443322"}),
        (
            "db_uri",
            {"msg": "postgres://svc_user:" + "Sup3rS3cretPw" + "@db.internal:5432/prod"},
        ),
        ("cn_mobile", {"phone": "138" + "1234" + "5678"}),
        ("cn_id", {"id_card": "110105" + "19900307" + "001" + "7"}),
        ("email", {"contact": "alice.ops" + "@" + "example.org"}),
    ]

    decoys: list[tuple[str, dict[str, Any]]] = [
        ("uuid4_a", {"trace_id": "550e8400-e29b-41d4-a716-446655440000"}),
        ("uuid4_b", {"span_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"}),
        (
            "sha256_a",
            {"checksum": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
        ),
        (
            "sha256_b",
            {"digest": "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"},
        ),
        ("git_sha", {"commit": "f74d023f93607b8c3268133339a5e532a949898c"}),
        ("iso_ts", {"timestamp": "2026-11-25T15:30:00Z"}),
        ("tau_order_id", {"order_id": "138" + "00138000"}),
        (
            "prefixed_order",
            {"note": "Order #W" + "138" + "00138000 shipped via ORD-" + "139" + "11112222"},
        ),
        ("bearer_prose", {"doc": "The bearer of the voucher receives free shipping under policy."}),
        ("token_prose", {"doc": "Budget tracking limits virtual token usage per workflow step."}),
        ("semver", {"version": "v2.14.3-rc.1"}),
        ("ipv4_loopback", {"host": "127.0.0.1:8000"}),
        ("flight_code", {"flight_number": "HAT022", "seat": "14A"}),
        ("currency", {"refund_amount": "129.99 USD"}),
        ("http_status", {"status_text": "HTTP 200 OK retry_after=5s"}),
        ("json_path", {"path": "$.shared.orders[0].status"}),
        ("short_code", {"promo": "SAVE20NOW"}),
        ("invalid_18_digits", {"serial": "999999" + "1990" + "01010019"}),
    ]
    return secrets, decoys


def run_builtin_redaction_suite() -> dict[str, Any]:
    """Evaluate 15 synthetic secrets vs 18 benign decoys for recall & benign FPR."""
    secrets, decoys = build_builtin_redaction_cases()
    secrets_redacted = 0
    secret_details: list[dict[str, Any]] = []
    for label, payload in secrets:
        res = redact_payload(payload)
        hit = res.findings_count > 0
        if hit:
            secrets_redacted += 1
        secret_details.append({"label": label, "redacted": hit, "rules": list(res.matched_rules)})

    decoys_flagged = 0
    decoy_details: list[dict[str, Any]] = []
    for label, payload in decoys:
        res = redact_payload(payload)
        flagged = res.findings_count > 0
        if flagged:
            decoys_flagged += 1
        decoy_details.append({"label": label, "flagged": flagged, "rules": list(res.matched_rules)})

    total_s = len(secrets)
    total_d = len(decoys)
    return {
        "secrets_tested": total_s,
        "secrets_redacted": secrets_redacted,
        "recall": round(secrets_redacted / total_s, 4) if total_s else 1.0,
        "decoys_tested": total_d,
        "decoys_flagged": decoys_flagged,
        "benign_fpr": round(decoys_flagged / total_d, 4) if total_d else 0.0,
        "secret_details": secret_details,
        "decoy_details": decoy_details,
    }
