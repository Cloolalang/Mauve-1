"""Decode AT modem result dicts (*ok*, *final*, *lines*) into human-readable hints."""

from __future__ import annotations

import re
from typing import Any

# GSM/3GPP TS 27.007 + common modem mappings (abbreviated — see annex for full list).
_CME_MESSAGES: dict[int, str] = {
    0: "Phone failure",
    1: "No connection to phone",
    2: "Phone adapter link reserved",
    3: "Operation not allowed",
    4: "Operation not supported",
    5: "PH-SIM PIN required",
    7: "SIM failure",
    10: "SIM not inserted",
    13: "SIM failure / memory problem",
    16: "Invalid characters in dial string",
    22: "Not found",
    25: "Network not allowed — emergency calls only",
    26: "Network registration denied",
    27: "Network unknown / out of PLMN coverage",
    28: "Network timeout / command blocked in current radio state",
    29: "Network timeout",
    30: "No network service (cannot register or select PLMN)",
    31: "Network timeout",
    32: "Network not allowed — emergency only",
    50: "Incorrect parameters",
    103: "Illegal MS (#3)",
    106: "Illegal ME (#6)",
}


def parse_cme_from_text(text: str) -> tuple[int | None, str]:
    u = text.strip().upper()
    m = re.search(r"\+CME\s+ERROR:\s*(\d+)", u)
    if not m:
        return None, ""
    code = int(m.group(1))
    hint = _CME_MESSAGES.get(code, f"modem CME code {code} (see 3GPP TS 27.007 annex)")
    return code, hint


def parse_cms_from_text(text: str) -> tuple[int | None, str]:
    u = text.strip().upper()
    m = re.search(r"\+CMS\s+ERROR:\s*(\d+)", u)
    if not m:
        return None, ""
    code = int(m.group(1))
    # SMS CMS codes differ; brief generic.
    hint = _CME_MESSAGES.get(code, f"sms/CMS error code {code}")
    return code, hint


def describe_modem_send_result(result: dict[str, Any] | None) -> str | None:
    """
    Return a concise English line for UI/API, or None if OK / empty.
    Prefer *final* line; fallback to ERROR-like lines when *final* is unhelpful.
    """
    if not result:
        return "No modem response."
    if result.get("ok"):
        return None

    final = str(result.get("final", "")).strip()
    cmd = str(result.get("command", "") or "").strip()

    if final.upper() == "TIMEOUT":
        return f"Timed out waiting for modem response{f' ({cmd[:48]}…)' if len(cmd) > 48 else (f' ({cmd})' if cmd else '')}"

    if final.startswith("+CME ERROR"):
        code, hint = parse_cme_from_text(final)
        return f"CME ERROR {code} — {hint}" if code is not None else f"CME: {hint or final}"

    if final.startswith("+CMS ERROR"):
        code, hint = parse_cms_from_text(final)
        return f"CMS ERROR {code} — {hint}" if code is not None else (hint or final)

    if final.upper() == "ERROR":
        tail = ""
        lines = result.get("lines") or []
        joined = "\n".join(lines)
        mt = re.search(r"\+CME\s+ERROR:\s*\d+", joined.upper())
        if mt:
            ln = mt.group(0)
            num = _extract_trailing_digits(ln)
            cme_hint = _CME_MESSAGES.get(num, "") if num is not None else ""
            tail = f" after {ln.strip()} — {cme_hint}" if cme_hint else f" ({ln.strip()})"
        return f"AT ERROR{f' ({cmd[:40]}…)' if cmd and len(cmd) > 40 else (f' ({cmd})' if cmd else '')}{tail}"

    if final.startswith("WRITE_ERROR"):
        return final

    lines = result.get("lines") or []
    if lines:
        last = str(lines[-1]).strip()
        if last.startswith("+CME ERROR"):
            return describe_modem_send_result({**result, "final": last, "ok": False})

    return f"Rejected: {final}" if final else "Modem rejected command (unknown final)."


def _extract_trailing_digits(s: str) -> int | None:
    m = re.search(r":\s*(\d+)\s*$", s.strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def combine_errors(*hints: str | None, sep: str = " ") -> str | None:
    parts = [h.strip() for h in hints if h and str(h).strip()]
    return sep.join(parts) if parts else None

