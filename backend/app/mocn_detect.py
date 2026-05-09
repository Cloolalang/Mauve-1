"""
UK Vodafone UK ↔ Three reciprocal home-layer heuristic using **network** PLMN only.

Registration PLMN = serving LTE ``MCC``/``MNC`` from ``AT+QENG=\"servingcell\"`` (no SIM/IMSI).
PCC anchor = LTE primary cell ``EARFCN`` from the same snapshot.

Same registration operator + PCC on that operator's home EARFCN list → normal home layer.
Different operator vs. list → reciprocal / MOCN-style camping on partner anchors.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_CATALOG_DIR = Path(__file__).resolve().parent / "mocn"
_BUILTIN_CATALOGS = ("catalog_uk_vodafone_h3g.json",)

_mocn_catalog_cache: tuple[str | None, list[dict[str, Any]], str | None] | None = None


def iter_catalog_paths() -> list[Path]:
    out: list[Path] = []
    for name in _BUILTIN_CATALOGS:
        p = _CATALOG_DIR / name
        if p.is_file():
            out.append(p)
        else:
            logger.warning("MOCN catalog missing: %s", p)
    return out


def _load_json(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return raw if isinstance(raw, dict) else {}


def _normalize_digits(raw: Any) -> str:
    return str(raw or "").strip().removeprefix("+")


def _plmn_digits(mcc: Any, mnc: Any) -> str | None:
    if mcc is None or mnc is None:
        return None
    try:
        mc = abs(int(mcc))
        mn = abs(int(mnc))
    except (TypeError, ValueError):
        return None
    mcs = f"{mc:03d}"
    if mn < 100:
        return f"{mcs}{mn:02d}"
    return f"{mcs}{mn:03d}"


def _iter_rules_from_catalog(data: dict[str, Any]) -> list[dict[str, Any]]:
    rules = data.get("rules")
    if isinstance(rules, list):
        return [r for r in rules if isinstance(r, dict)]
    return []


def operators_from_legacy_rules_v1(data: dict[str, Any]) -> list[dict[str, Any]]:
    vf_ears: list[int] = []
    h3_ears: list[int] = []
    for r in _iter_rules_from_catalog(data):
        prefs = [_normalize_digits(x) for x in (r.get("home_imsi_plmn_prefixes") or [])]
        ears = [
            int(x)
            for x in (r.get("partner_home_lte_earfcns") or [])
            if str(x).lstrip("-").isdigit()
        ]
        home_op = str(r.get("home_operator") or "").lower()
        if "23415" in prefs or "vodafone" in home_op:
            h3_ears = ears
        if "23420" in prefs or "three" in home_op:
            vf_ears = ears
    return [
        {
            "id": "vodafone_uk",
            "label": "Vodafone UK",
            "registration_plmn_ids": ["23415"],
            "home_lte_earfcns": vf_ears,
        },
        {
            "id": "three_uk",
            "label": "Three UK (H3G)",
            "registration_plmn_ids": ["23420"],
            "home_lte_earfcns": h3_ears,
        },
    ]


def parse_catalog_operators(data: dict[str, Any]) -> list[dict[str, Any]]:
    ver = int(data.get("schema_version") or 0)
    if ver >= 2:
        ops = data.get("operators")
        if isinstance(ops, list):
            return [dict(x) for x in ops if isinstance(x, dict)]
        return []
    return operators_from_legacy_rules_v1(data)


def _normalize_operator_entry(raw: dict[str, Any]) -> dict[str, Any] | None:
    oid = str(raw.get("id") or "").strip()
    label = str(raw.get("label") or oid or "operator").strip()
    plmn_ids: list[str] = []
    for p in raw.get("registration_plmn_ids") or []:
        d = _normalize_digits(p)
        if d.isdigit() and len(d) >= 5:
            plmn_ids.append(d)
    ears_raw = raw.get("home_lte_earfcns") or []
    ears: list[int] = []
    if isinstance(ears_raw, list):
        for x in ears_raw:
            try:
                ears.append(int(x))
            except (TypeError, ValueError):
                continue
    if not oid or not plmn_ids or not ears:
        return None
    return {"id": oid, "label": label, "registration_plmn_ids": plmn_ids, "home_lte_earfcns": ears}


def load_mocn_bundle() -> tuple[str | None, list[dict[str, Any]], str | None]:
    paths = iter_catalog_paths()
    if not paths:
        return None, [], None
    path = paths[0]
    data = _load_json(path)
    cid = data.get("catalog_id")
    cid_s = str(cid).strip() if cid is not None else None
    raw_ops = parse_catalog_operators(data)
    out: list[dict[str, Any]] = []
    for ro in raw_ops:
        norm = _normalize_operator_entry(ro)
        if norm:
            out.append(norm)
    return cid_s, out, path.name


def mocn_eval_context() -> tuple[str | None, list[dict[str, Any]], str | None]:
    global _mocn_catalog_cache
    if _mocn_catalog_cache is None:
        _mocn_catalog_cache = load_mocn_bundle()
    return _mocn_catalog_cache


def operator_for_registration_plmn(
    plmn: str | None, operators: list[dict[str, Any]]
) -> dict[str, Any] | None:
    if not plmn:
        return None
    p = _normalize_digits(plmn)
    for op in operators:
        if p in op.get("registration_plmn_ids", []):
            return op
    return None


def layer_operator_for_earfcn(
    earfcn: int, operators: list[dict[str, Any]]
) -> dict[str, Any] | None:
    for op in operators:
        if earfcn in op.get("home_lte_earfcns", []):
            return op
    return None


def evaluate_mocn_uk_vf_h3g(
    *,
    servingcell: dict[str, Any] | None,
    catalog_operators: list[dict[str, Any]] | None = None,
    catalog_id: str | None = None,
    catalog_file: str | None = None,
) -> dict[str, Any]:
    ops = catalog_operators if catalog_operators is not None else mocn_eval_context()[1]

    lte = None
    if isinstance(servingcell, dict):
        lte_raw = servingcell.get("lte")
        lte = lte_raw if isinstance(lte_raw, dict) else None
    earfcn = lte.get("earfcn") if lte else None
    mcc = lte.get("mcc") if lte else None
    mnc = lte.get("mnc") if lte else None
    registration_plmn = _plmn_digits(mcc, mnc)

    base: dict[str, Any] = {
        "heuristic_catalog": catalog_id or "uk_vodafone_h3g_plmn_v1",
        "catalog_file": catalog_file,
        "registration_source": "qeng_lte_servingcell",
        "registration_plmn": registration_plmn,
        "registration_operator_id": None,
        "registration_operator_label": None,
        "serving_lte_earfcn": earfcn if isinstance(earfcn, int) else None,
        "layer_operator_id": None,
        "layer_operator_label": None,
        "partner_layer_possible": False,
        "partner_operator": None,
        "confidence": None,
        "explain": [],
    }

    if earfcn is None or not isinstance(earfcn, int):
        base["confidence"] = "no_lte_pcell"
        base["explain"].append("LTE PCC EARFCN not available from QENG servingcell.")
        return base

    if registration_plmn is None:
        base["confidence"] = "missing_registration_plmn"
        base["explain"].append(
            "Serving LTE MCC/MNC not available from QENG; cannot derive registration PLMN for this heuristic."
        )
        return base

    reg_op = operator_for_registration_plmn(registration_plmn, ops)
    if reg_op:
        base["registration_operator_id"] = reg_op.get("id")
        base["registration_operator_label"] = reg_op.get("label")

    if not ops:
        base["confidence"] = "no_catalog"
        base["explain"].append("MOCN operator catalog empty or not loaded.")
        return base

    layer_op = layer_operator_for_earfcn(earfcn, ops)

    if layer_op is None:
        base["confidence"] = "earfcn_outside_home_lists"
        base["explain"].append(
            f"PCC EARFCN {earfcn} is not in the configured Vodafone/Three LTE home-anchor lists."
        )
        if reg_op is None:
            base["explain"].append(
                f"Registration PLMN {registration_plmn} is outside this catalogue "
                "(expected examples: Vodafone 23415 or Three/H3G 23420)."
            )
            base["confidence"] = "unknown_registration_plmn"
        return base

    base["layer_operator_id"] = layer_op.get("id")
    base["layer_operator_label"] = layer_op.get("label")

    if reg_op is None:
        base["confidence"] = "unknown_registration_plmn"
        base["explain"].append(
            f"Registration PLMN {registration_plmn} is outside this catalogue "
            "(expected examples: Vodafone 23415 or Three/H3G 23420)."
        )
        base["explain"].append(
            f"PCC EARFCN {earfcn} matches {layer_op.get('label')} home-anchor list."
        )
        return base

    rid = reg_op.get("id")
    lid = layer_op.get("id")
    if rid == lid:
        base["confidence"] = "home_on_registered_layer"
        base["explain"].append(
            f"Registration PLMN {registration_plmn} ({reg_op.get('label')}) and PCC EARFCN {earfcn} "
            f"are on that operator's configured LTE home anchors."
        )
    else:
        base["partner_layer_possible"] = True
        base["confidence"] = "reciprocal_mocn"
        base["partner_operator"] = layer_op.get("label")
        base["explain"].append(
            f"Registration PLMN {registration_plmn} ({reg_op.get('label')}) but PCC EARFCN {earfcn} "
            f"matches {layer_op.get('label')} home anchors — reciprocal / MOCN-style access."
        )

    return base
