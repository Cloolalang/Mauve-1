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


def _extract_cme_from_any_line(lines: list[str] | None) -> tuple[int, str] | None:
    """If any echoed line contains +CME ERROR, return (code, hint); else None."""
    if not lines:
        return None
    for raw in lines:
        u = raw.strip().upper()
        if "+CME ERROR" not in u:
            continue
        code, hint = parse_cme_from_text(raw)
        if code is not None:
            return code, hint or f"CME code {code}"
    return None


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
        lines = result.get("lines") or []
        cme = _extract_cme_from_any_line(lines)
        if cme:
            code, hint = cme
            suf = cmd_short(cmd)
            return f"CME ERROR {code} — {hint}{suf}"

        tail = ""
        joined = "\n".join(lines)
        mt = re.search(r"\+CME\s+ERROR:\s*\d+", joined.upper())
        if mt:
            ln = mt.group(0)
            num = _extract_trailing_digits(ln)
            cme_hint = _CME_MESSAGES.get(num, "") if num is not None else ""
            tail = f" after {ln.strip()} — {cme_hint}" if cme_hint else f" ({ln.strip()})"
        base = f"AT ERROR{f' ({cmd[:40]}…)' if cmd and len(cmd) > 40 else (f' ({cmd})' if cmd else '')}{tail}"
        qh = _qiact_hint(cmd)
        return f"{base} {qh}".strip() if qh else base

    if final.startswith("WRITE_ERROR"):
        return final

    lines = result.get("lines") or []
    if lines:
        last = str(lines[-1]).strip()
        if last.startswith("+CME ERROR"):
            return describe_modem_send_result({**result, "final": last, "ok": False})

    return f"Rejected: {final}" if final else "Modem rejected command (unknown final)."


def cmd_short(command: str) -> str:
    c = command.strip()
    if not c:
        return ""
    return f' ({c[:48]}…)' if len(c) > 48 else f" ({c})"


def _qiact_hint(command: str) -> str | None:
    cu = (command or "").strip().upper()
    if not cu or "QIACT" not in cu:
        return None
    return (
        "Hint: PDP activate needs packet attach + a defined PDP profile for that CID (AT+CGDCONT), EPS "
        "registration on packet service, and no conflicting QMI/router WAN owning the context."
    )


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

