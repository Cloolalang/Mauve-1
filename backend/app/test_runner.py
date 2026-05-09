"""Saved test profiles, run orchestration, CSV summary, KPI JSONL, and UI snapshot helpers."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import re
import secrets
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import HTTPException

from app.kpi_service import format_qcainfo_carriers_pcc_scc

TEST_TYPES = frozenset(
    {"ping", "iperf_download", "iperf_upload", "iperf_download_upload", "volte_call_outbound"}
)
MODEM_ANTENNA_CONFIGS = frozenset({"SISO", "MIMO"})
DEFAULT_MODEM_ANTENNA_CONFIG = "SISO"


def profile_modem_antenna_config(p: dict[str, Any]) -> str:
    """Antenna configuration from profile; defaults to SISO when unset or invalid."""
    raw = str(p.get("modem_antenna_config") or "").strip().upper()
    if raw in MODEM_ANTENNA_CONFIGS:
        return raw
    return DEFAULT_MODEM_ANTENNA_CONFIG


def state_base_dir() -> str:
    if getattr(sys, "frozen", False):
        base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
        return os.path.join(base, "5GModemTestDriver")
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".state"))


def automated_tests_root() -> str:
    """``backend/automated_tests`` in dev; under app state when frozen."""
    if getattr(sys, "frozen", False):
        return os.path.join(state_base_dir(), "automated_tests")
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "automated_tests"))


def test_case_profiles_dir() -> str:
    """Bundled / repo test profiles: one JSON file per profile (merged with ``.state/test_profiles.json``)."""
    d = os.path.join(automated_tests_root(), "test_cases")
    os.makedirs(d, exist_ok=True)
    return d


def profiles_path() -> str:
    return os.path.join(state_base_dir(), "test_profiles.json")


def bundled_example_profiles_dir() -> str:
    """Alias for :func:`test_case_profiles_dir` (API field ``bundled_examples_dir``)."""
    return test_case_profiles_dir()


def _load_bundled_example_profiles() -> list[dict[str, Any]]:
    d = test_case_profiles_dir()
    out: list[dict[str, Any]] = []
    if not os.path.isdir(d):
        return out
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".json"):
            continue
        path = os.path.join(d, fn)
        try:
            with open(path, encoding="utf-8") as f:
                p = json.load(f)
        except Exception:
            continue
        if not isinstance(p, dict):
            continue
        errs = validate_profile(p)
        if errs:
            continue
        out.append(dict(p))
    return out


def registry_profile_names() -> set[str]:
    reg = load_profiles_registry()
    return {str(p.get("name") or "").strip() for p in (reg.get("profiles") or []) if isinstance(p, dict) and str(p.get("name") or "").strip()}


def list_merged_profiles() -> list[dict[str, Any]]:
    """Saved profiles from state file, plus ``automated_tests/test_cases`` JSON for any name not in the registry."""
    by_name: dict[str, dict[str, Any]] = {}
    for p in load_profiles_registry().get("profiles") or []:
        if isinstance(p, dict) and str(p.get("name") or "").strip():
            by_name[str(p.get("name")).strip()] = dict(p)
    for ex in _load_bundled_example_profiles():
        n = str(ex.get("name") or "").strip()
        if n and n not in by_name:
            by_name[n] = dict(ex)
    return list(by_name.values())


def example_only_profile_names() -> list[str]:
    """Names supplied only from ``automated_tests/test_cases/*.json`` (not overridden in ``test_profiles.json``)."""
    reg_names = registry_profile_names()
    return sorted(
        str(ex.get("name") or "").strip()
        for ex in _load_bundled_example_profiles()
        if str(ex.get("name") or "").strip() and str(ex.get("name") or "").strip() not in reg_names
    )


def test_results_root_dir() -> str:
    """Parent directory for per-run folders (``automated_tests/test_results``)."""
    d = os.path.join(automated_tests_root(), "test_results")
    os.makedirs(d, exist_ok=True)
    return d


def test_results_dir() -> str:
    """Deprecated name: returns the per-run *root* (not a specific run folder). Prefer :func:`test_results_root_dir`."""
    return test_results_root_dir()


def _run_index_path() -> str:
    return os.path.join(test_results_root_dir(), "_run_index.json")


def _load_run_index() -> dict[str, Any]:
    try:
        with open(_run_index_path(), encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("runs"), dict):
            return data
    except Exception:
        pass
    return {"runs": {}}


def _save_run_index(data: dict[str, Any]) -> None:
    path = _run_index_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def register_run_artifacts_folder(run_id: str, folder_name: str) -> None:
    """Map ``run_id`` (hex) to a subdirectory name under :func:`test_results_root_dir`."""
    rid = (run_id or "").strip().lower()
    fn = (folder_name or "").strip()
    if not re.fullmatch(r"[0-9a-f]{8}", rid) or not fn:
        return
    data = _load_run_index()
    runs = data.setdefault("runs", {})
    if not isinstance(runs, dict):
        runs = {}
        data["runs"] = runs
    runs[rid] = fn
    _save_run_index(data)


def resolve_run_artifacts_dir(run_id: str) -> str | None:
    """Directory containing ``run_<id>_summary.csv`` for this run, or ``None``."""
    rid = (run_id or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{8}", rid):
        return None
    data = _load_run_index()
    runs = data.get("runs") if isinstance(data.get("runs"), dict) else {}
    folder = runs.get(rid) if isinstance(runs, dict) else None
    if isinstance(folder, str) and folder.strip():
        p = os.path.join(test_results_root_dir(), folder.strip())
        if os.path.isdir(p):
            return p
    legacy = os.path.join(state_base_dir(), "test_results")
    if os.path.isfile(os.path.join(legacy, f"run_{rid}_summary.csv")):
        return legacy
    return None


def _sanitize_folder_component(s: str, max_len: int = 48) -> str:
    t = (s or "").strip()
    if not t:
        return "unknown"
    t = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", t)
    t = re.sub(r"\s+", "_", t).strip("._ ") or "unknown"
    return t[:max_len]


def build_run_folder_name(project_name: str, test_location: str, started_ts: float, run_id: str) -> str:
    """Per-run folder: ``<project>_<location>_<UTC datetime>_<run_id>`` (filesystem-safe)."""
    dt = datetime.fromtimestamp(started_ts, tz=timezone.utc)
    dt_part = dt.strftime("%Y-%m-%d_%H-%M-%S")
    p = _sanitize_folder_component(project_name, 40)
    loc = _sanitize_folder_component(test_location, 40)
    rid = (run_id or "").strip().lower()[:8]
    return f"{p}_{loc}_{dt_part}_{rid}"


def prepare_run_artifacts_dir(
    *,
    project_name: str,
    test_location: str,
    started_ts: float,
    run_id: str,
) -> str:
    """Create ``automated_tests/test_results/<folder>/`` and register it for downloads."""
    folder = build_run_folder_name(project_name, test_location, started_ts, run_id)
    out = os.path.join(test_results_root_dir(), folder)
    os.makedirs(out, exist_ok=True)
    register_run_artifacts_folder(run_id, folder)
    return out


def _utc_iso(ts: float | None = None) -> str:
    t = datetime.now(timezone.utc) if ts is None else datetime.fromtimestamp(ts, tz=timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def sanitize_profile_filename(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", (name or "").strip()) or "profile"
    return s[:48]


def new_run_id() -> str:
    return secrets.token_hex(4)


def load_profiles_registry() -> dict[str, Any]:
    path = profiles_path()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("profiles"), list):
            return data
    except Exception:
        pass
    return {"profiles": []}


def save_profiles_registry(data: dict[str, Any]) -> None:
    path = profiles_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def get_profile_by_name(name: str) -> dict[str, Any] | None:
    """Resolve profile from saved registry first, then ``automated_tests/test_cases/*.json``."""
    reg = load_profiles_registry()
    n = (name or "").strip()
    for p in reg.get("profiles") or []:
        if isinstance(p, dict) and str(p.get("name") or "").strip() == n:
            return dict(p)
    for ex in _load_bundled_example_profiles():
        if isinstance(ex, dict) and str(ex.get("name") or "").strip() == n:
            return dict(ex)
    return None


def validate_profile(p: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    if not isinstance(p, dict):
        return ["profile must be an object"]
    name = str(p.get("name") or "").strip()
    if not name:
        errs.append("name is required")
    if int(p.get("schema_version") or 1) != 1:
        errs.append("schema_version must be 1")
    tt = str(p.get("test_type") or "").strip()
    if tt not in TEST_TYPES:
        errs.append(f"test_type must be one of: {', '.join(sorted(TEST_TYPES))}")
    cfg = p.get("test_config")
    if not isinstance(cfg, dict):
        errs.append("test_config must be an object")
        return errs
    if tt == "ping":
        for k in ("host", "count", "timeout_ms"):
            if k not in cfg or cfg.get(k) in (None, ""):
                errs.append(f"ping.test_config.{k} is required")
        if "bind_ipv4" not in cfg:
            errs.append("ping.test_config.bind_ipv4 is required (use empty string for no interface bind)")
    elif tt in ("iperf_download", "iperf_upload", "iperf_download_upload"):
        for k in ("host", "port", "duration_sec", "protocol", "parallel_streams", "bitrate_limit_mbps", "mobile_only"):
            if k not in cfg:
                errs.append(f"iperf.test_config.{k} is required")
        cto = cfg.get("connect_timeout_sec")
        if cto is not None and cto != "":
            try:
                ctf = float(cto)
                if ctf < 1 or ctf > 120:
                    errs.append("iperf.test_config.connect_timeout_sec must be between 1 and 120 when set")
            except (TypeError, ValueError):
                errs.append("iperf.test_config.connect_timeout_sec must be a number when set")
        prm = cfg.get("port_range_max")
        if prm is not None and prm != "":
            try:
                pm = int(prm)
            except (TypeError, ValueError):
                errs.append("iperf.test_config.port_range_max must be an integer when set")
            else:
                if pm < 1 or pm > 65535:
                    errs.append("iperf.test_config.port_range_max must be 1..65535 when set")
                else:
                    try:
                        p0 = int(cfg.get("port"))
                    except (TypeError, ValueError):
                        p0 = -1
                    if p0 >= 1 and pm < p0:
                        errs.append("iperf.test_config.port_range_max must be >= port")
    elif tt == "volte_call_outbound":
        for k in ("phone_number", "call_duration_sec", "answer_wait_sec", "auto_hangup"):
            if k not in cfg:
                errs.append(f"volte.test_config.{k} is required")
    mr0 = p.get("modem_requirements")
    if mr0 is not None and not isinstance(mr0, dict):
        errs.append("modem_requirements must be an object (may be {})")
    mac = p.get("modem_antenna_config")
    if mac is not None and str(mac).strip():
        if str(mac).strip().upper() not in MODEM_ANTENNA_CONFIGS:
            errs.append("modem_antenna_config must be SISO or MIMO")
    return errs


def upsert_profile(p: dict[str, Any]) -> None:
    p = dict(p)
    if p.get("modem_requirements") is None:
        p["modem_requirements"] = {}
    if not str(p.get("modem_antenna_config") or "").strip():
        p["modem_antenna_config"] = DEFAULT_MODEM_ANTENNA_CONFIG
    errs = validate_profile(p)
    if errs:
        raise ValueError("; ".join(errs))
    reg = load_profiles_registry()
    profiles: list[dict[str, Any]] = list(reg.get("profiles") or [])
    name = str(p["name"]).strip()
    replaced = False
    out: list[dict[str, Any]] = []
    for ex in profiles:
        if isinstance(ex, dict) and str(ex.get("name") or "").strip() == name:
            out.append(dict(p))
            replaced = True
        else:
            out.append(ex)
    if not replaced:
        out.append(dict(p))
    reg["profiles"] = out
    save_profiles_registry(reg)


def delete_profile(name: str) -> bool:
    reg = load_profiles_registry()
    profiles = [x for x in (reg.get("profiles") or []) if str(x.get("name") or "").strip() != name.strip()]
    if len(profiles) == len(reg.get("profiles") or []):
        return False
    reg["profiles"] = profiles
    save_profiles_registry(reg)
    return True


_PASSWORD_KEY_HINTS = ("password", "passwd", "secret", "credential")


def redact_ui_controls(obj: Any) -> Any:
    """Remove or mask password-like fields in a nested JSON structure."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            kl = str(k).lower()
            if any(h in kl for h in _PASSWORD_KEY_HINTS):
                out[k] = "__redacted__" if v not in (None, "", False) else None
            else:
                out[k] = redact_ui_controls(v)
        return out
    if isinstance(obj, list):
        return [redact_ui_controls(x) for x in obj]
    return obj


def _json_default(o: Any) -> Any:
    try:
        json.dumps(o)
        return o
    except TypeError:
        return str(o)


def snapshot_json_copy(snap: dict[str, Any]) -> dict[str, Any]:
    try:
        return json.loads(json.dumps(snap, default=_json_default))
    except Exception:
        return {"_error": "snapshot_not_json_serializable"}


def _dig(d: Any, *path: str) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def _finite_num(x: Any) -> float | None:
    try:
        v = float(x)
        if v != v:  # nan
            return None
        return v
    except (TypeError, ValueError):
        return None


def _int_metric_str(v: Any) -> str | None:
    """String for ARFCN/PCI counters; rejects bool (``bool`` is a ``int`` subclass)."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v != v:
            return None
        if v.is_integer():
            return str(int(v))
    s = str(v).strip()
    return s or None


def _most_common_nonempty_str(values: list[str]) -> str:
    cleaned = [str(v).strip() for v in values if str(v or "").strip()]
    if not cleaned:
        return ""
    pair = Counter(cleaned).most_common(1)[0]
    return pair[0]


def _registration_state_csv_line(sample: dict[str, Any]) -> str:
    """One-line registration summary for CSV: QENG LTE PLMN, optional catalog label, EPS scope (+CEREG), UK MOCN hint."""
    reg = sample.get("registration") if isinstance(sample.get("registration"), dict) else {}
    ds = sample.get("data_service") if isinstance(sample.get("data_service"), dict) else {}
    mcn = sample.get("mocn") if isinstance(sample.get("mocn"), dict) else {}
    plmn = str(reg.get("plmn") or "").strip()
    lab = str(reg.get("operator_label") or "").strip()
    eps_scope = ds.get("eps_reg_scope")
    if eps_scope == "home":
        eps_lbl = "home"
    elif eps_scope == "roaming":
        eps_lbl = "roam"
    else:
        eps_lbl = "?"
    cf = str(mcn.get("confidence") or "").strip()
    parts: list[str] = []
    if plmn:
        parts.append(plmn)
    if lab:
        parts.append(lab)
    parts.append(f"EPS:{eps_lbl}")
    if cf == "reciprocal_mocn":
        po = str(mcn.get("partner_operator") or "").strip()
        parts.append(f"MOCN->{po}" if po else "MOCN")
    elif cf == "home_on_registered_layer":
        parts.append("layer:own")
    elif cf:
        parts.append(f"h:{cf}")
    return " ".join(parts)


# Match dashboard ``formatOperatorName`` PLMN hints (show friendly MNO name in CSV).
_UK_MNO_BY_PLMN: dict[str, str] = {
    "23415": "Vodafone",
    "23410": "VMO2",
    "23430": "EE",
    "23420": "H3G",
}


def operator_display_name(raw: str | None) -> str:
    """Human-readable operator label (e.g. ``Vodafone`` for PLMN ``23415``)."""
    s = str(raw or "").strip()
    if not s:
        return ""
    mno = _UK_MNO_BY_PLMN.get(s)
    if mno:
        return mno
    return s


def _operator_most_common_display(raw_keys: list[str]) -> str:
    cleaned = [str(v).strip() for v in raw_keys if str(v or "").strip()]
    if not cleaned:
        return ""
    winner = Counter(cleaned).most_common(1)[0][0]
    return operator_display_name(winner)


def _qcainfo_ca_status(qca: dict[str, Any]) -> str:
    """``true`` if at least one SCC carrier is reported in ``AT+QCAINFO`` parse."""
    carriers = qca.get("carriers")
    if isinstance(carriers, list):
        for c in carriers:
            if not isinstance(c, dict):
                continue
            if str(c.get("role") or "").strip().upper() == "SCC":
                return "true"
        return "false"
    ears = qca.get("earfcn_active")
    if isinstance(ears, list) and len(ears) > 1:
        return "true"
    return ""


def format_ca_policy_from_lte_band(lte_band: Any) -> str:
    """Same CA policy label as the dashboard *Read Locks* row (``applyLocks`` in ``main.py``)."""
    lte_val = str(lte_band or "").strip()
    if not lte_val:
        return ""
    lte_norm = re.sub(r"\s+", "", lte_val)
    parts = re.split(r"[,:]", lte_val)
    band_tokens = [p.strip() for p in parts if p.strip().isdigit()]
    if lte_norm == "0":
        return "ON (multi/all)"
    if len(band_tokens) > 1:
        return "ON (multi/all)"
    return "OFF (single band)"


def format_nrdc_mode_csv(raw: Any) -> str:
    """QNWPREFCFG ``nrdc_mode`` for CSV: ``0`` / ``1``, or raw string if non-numeric."""
    if raw is None:
        return ""
    s = str(raw).strip().strip('"')
    if not s:
        return ""
    if s.lower() in ("true", "on"):
        return "1"
    try:
        return "1" if int(float(s)) else "0"
    except (TypeError, ValueError):
        return s


def aggregate_snapshots(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """KPI aggregates from in-run snapshots (LTE primary RF, CA, identity, data service, RAT / EN-DC)."""
    rsrp_vals: list[float] = []
    rssi_vals: list[float] = []
    rsrq_vals: list[float] = []
    sinr_vals: list[float] = []
    ca_mhz_vals: list[float] = []
    earfcn_pci_keys: list[str] = []
    primary_cell_pci_keys: list[str] = []
    cell_id_keys: list[str] = []
    apn_keys: list[str] = []
    operator_keys: list[str] = []
    rat_keys: list[str] = []
    endc_true_ct = 0
    endc_false_ct = 0
    ca_status_keys: list[str] = []
    ca_carrier_txt_keys: list[str] = []
    lte_earfcn_resel_rates: list[float] = []
    lte_pci_resel_rates: list[float] = []
    nr_rsrp_vals: list[float] = []
    nr_rsrq_vals: list[float] = []
    nr_sinr_vals: list[float] = []
    nr_arfcn_keys: list[str] = []
    nr_pci_keys: list[str] = []
    nr_dl_bw_vals: list[float] = []
    registration_state_lines: list[str] = []

    for s in samples:
        lte = _dig(s, "servingcell", "lte") or {}
        srv = s.get("servingcell") if isinstance(s.get("servingcell"), dict) else {}
        prx = _dig(s, "qsinr", "LTE", "PRx") or _dig(s, "qsinr", "LTE", "prx")
        r = _finite_num(lte.get("rsrp"))
        rss = _finite_num(lte.get("rssi"))
        q = _finite_num(lte.get("rsrq"))
        si = _finite_num(prx if prx is not None else lte.get("sinr"))
        if si is None:
            si = _finite_num(lte.get("sinr_raw"))
        if r is not None:
            rsrp_vals.append(r)
        if rss is not None:
            rssi_vals.append(rss)
        if q is not None:
            rsrq_vals.append(q)
        if si is not None:
            sinr_vals.append(si)

        qca = s.get("qcainfo") if isinstance(s.get("qcainfo"), dict) else {}
        ca_m = _finite_num(qca.get("dl_bw_aggregate_mhz"))
        if ca_m is not None and ca_m > 0:
            ca_mhz_vals.append(ca_m)
        ca_st = _qcainfo_ca_status(qca)
        if ca_st:
            ca_status_keys.append(ca_st)
        ca_line = format_qcainfo_carriers_pcc_scc(qca)
        if ca_line:
            ca_carrier_txt_keys.append(ca_line)

        cr = s.get("carrier_reselection") if isinstance(s.get("carrier_reselection"), dict) else {}
        er = _finite_num(cr.get("primary_earfcn_reselections_per_min"))
        pr = _finite_num(cr.get("intra_freq_pci_reselections_per_min"))
        if er is not None:
            lte_earfcn_resel_rates.append(er)
        if pr is not None:
            lte_pci_resel_rates.append(pr)

        nrp = s.get("nr_rf") if isinstance(s.get("nr_rf"), dict) else {}
        prim = nrp.get("primary") if isinstance(nrp.get("primary"), dict) else {}
        nrv = _finite_num(prim.get("rsrp"))
        nrq = _finite_num(prim.get("rsrq"))
        nrs = _finite_num(prim.get("sinr"))
        if nrv is not None:
            nr_rsrp_vals.append(nrv)
        if nrq is not None:
            nr_rsrq_vals.append(nrq)
        if nrs is not None:
            nr_sinr_vals.append(nrs)
        narf_s = _int_metric_str(prim.get("arfcn"))
        if narf_s:
            nr_arfcn_keys.append(narf_s)
        npci_s = _int_metric_str(prim.get("pci"))
        if npci_s:
            nr_pci_keys.append(npci_s)
        ndbw = _finite_num(prim.get("dl_bw"))
        if ndbw is not None:
            nr_dl_bw_vals.append(ndbw)

        ear = lte.get("earfcn")
        pci = lte.get("pcid")
        if ear is not None and pci is not None:
            earfcn_pci_keys.append(f"{int(ear)}/{int(pci)}")
            primary_cell_pci_keys.append(str(int(pci)))

        cid = lte.get("cell_id_hex")
        if cid is not None and str(cid).strip():
            cell_id_keys.append(str(cid).strip())

        ds = s.get("data_service") if isinstance(s.get("data_service"), dict) else {}
        apn = ds.get("apn")
        if apn is not None and str(apn).strip():
            apn_keys.append(str(apn).strip())

        net = s.get("network") if isinstance(s.get("network"), dict) else {}
        op = net.get("operator")
        if op is not None and str(op).strip():
            operator_keys.append(str(op).strip())

        rat = lte.get("rat") if isinstance(lte.get("rat"), str) and str(lte.get("rat")).strip() else None
        if not rat:
            m = srv.get("mode")
            rat = str(m).strip() if m is not None and str(m).strip() else None
        if rat:
            rat_keys.append(rat)

        nr_nsa = srv.get("nr_nsa") if isinstance(srv.get("nr_nsa"), dict) else None
        if isinstance(nr_nsa, dict) and nr_nsa.get("pcid") is not None:
            endc_true_ct += 1
        elif isinstance(lte, dict) and lte.get("earfcn") is not None:
            endc_false_ct += 1

        registration_state_lines.append(_registration_state_csv_line(s))

    def avg(xs: list[float]) -> str:
        if not xs:
            return ""
        return str(round(sum(xs) / len(xs), 2))

    endc_state = ""
    if endc_true_ct or endc_false_ct:
        endc_state = "true" if endc_true_ct >= endc_false_ct else "false"

    return {
        "kpi_sample_count": str(len(samples)),
        "primary_rsrp_avg_dbm": avg(rsrp_vals),
        "primary_rssi_avg_dbm": avg(rssi_vals),
        "primary_rsrq_avg_db": avg(rsrq_vals),
        "primary_sinr_avg_db": avg(sinr_vals),
        "ca_aggregated_dl_bw_mhz_avg": avg(ca_mhz_vals),
        "primary_earfcn_pci_most_common": _most_common_nonempty_str(earfcn_pci_keys),
        "primary_cell_pci_most_common": _most_common_nonempty_str(primary_cell_pci_keys),
        "primary_cell_id_most_common": _most_common_nonempty_str(cell_id_keys),
        "apn_most_common": _most_common_nonempty_str(apn_keys),
        "operator_most_common": _operator_most_common_display(operator_keys),
        "rat_most_common": _most_common_nonempty_str(rat_keys),
        "registration_state_most_common": _most_common_nonempty_str(registration_state_lines),
        "endc_state": endc_state,
        "ca_status_most_common": _most_common_nonempty_str(ca_status_keys),
        "ca_carriers_pcc_scc": _most_common_nonempty_str(ca_carrier_txt_keys),
        "lte_pcell_earfcn_reselections_per_min_avg": avg(lte_earfcn_resel_rates),
        "lte_pcell_pci_reselections_per_min_avg": avg(lte_pci_resel_rates),
        "nr5g_primary_rsrp_avg_dbm": avg(nr_rsrp_vals),
        "nr5g_primary_rsrq_avg_db": avg(nr_rsrq_vals),
        "nr5g_primary_sinr_avg_db": avg(nr_sinr_vals),
        "nr5g_primary_arfcn_most_common": _most_common_nonempty_str(nr_arfcn_keys),
        "nr5g_primary_pci_most_common": _most_common_nonempty_str(nr_pci_keys),
        "nr5g_primary_dl_bw_mhz_avg": avg(nr_dl_bw_vals),
    }


def _empty_ping_tool_cols() -> dict[str, str]:
    return {k: "" for k in ("ping_host", "ping_count", "ping_loss_pct", "ping_rtt_avg_ms", "ping_rtt_min_ms", "ping_rtt_max_ms", "ping_jitter_ms")}


def _empty_iperf_tool_cols() -> dict[str, str]:
    return {
        k: ""
        for k in (
            "iperf_host",
            "iperf_port",
            "iperf_duration_sec",
            "iperf_parallel_streams",
            "iperf_connect_timeout_sec",
            "iperf_protocol",
            "iperf_bitrate_limit_mbps",
            "iperf_mobile_only",
            "iperf_direction",
            "iperf_throughput_dl_mbps",
            "iperf_throughput_ul_mbps",
        )
    }


def _empty_volte_tool_cols() -> dict[str, str]:
    return {
        k: ""
        for k in (
            "volte_number",
            "volte_connected",
            "volte_answer_delay_sec",
            "volte_call_duration_sec",
            "volte_ceer",
            "volte_modem_call_messages",
        )
    }


def _volte_modem_msgs_csv(tool_result: dict[str, Any]) -> str:
    raw = tool_result.get("call_urc_lines")
    if not isinstance(raw, list):
        return ""
    parts = []
    for x in raw:
        s = str(x).strip()
        if s:
            parts.append(s)
    return "; ".join(parts)


def ping_tool_csv_columns(cfg: dict[str, Any], tool_result: dict[str, Any]) -> dict[str, str]:
    cnt = int(cfg.get("count") or 0)
    recv = int(tool_result.get("received") or 0)
    loss = ""
    if cnt > 0:
        loss = str(round(100.0 * max(0, cnt - recv) / float(cnt), 2))
    return {
        "ping_host": str(tool_result.get("host") or cfg.get("host") or ""),
        "ping_count": str(cnt),
        "ping_loss_pct": loss,
        "ping_rtt_avg_ms": "" if tool_result.get("avg_ms") is None else str(tool_result.get("avg_ms")),
        "ping_rtt_min_ms": "" if tool_result.get("min_ms") is None else str(tool_result.get("min_ms")),
        "ping_rtt_max_ms": "" if tool_result.get("max_ms") is None else str(tool_result.get("max_ms")),
        "ping_jitter_ms": "" if tool_result.get("jitter_ms") is None else str(tool_result.get("jitter_ms")),
    }


def _fmt_mbps_str(mbps: Any) -> str:
    if isinstance(mbps, (int, float)) and float(mbps) == float(mbps):
        return str(round(float(mbps), 3)).rstrip("0").rstrip(".")
    return ""


def iperf_tool_csv_columns(cfg: dict[str, Any], tool_result: dict[str, Any], test_type: str) -> dict[str, str]:
    if test_type == "iperf_download_upload":
        direction = "download_upload"
        dl_mbps = _fmt_mbps_str(tool_result.get("throughput_mbps_dl"))
        ul_mbps = _fmt_mbps_str(tool_result.get("throughput_mbps_ul"))
    elif test_type == "iperf_download":
        direction = str(tool_result.get("direction") or "download")
        dl_mbps = _fmt_mbps_str(tool_result.get("throughput_mbps"))
        ul_mbps = ""
    elif test_type == "iperf_upload":
        direction = str(tool_result.get("direction") or "upload")
        dl_mbps = ""
        ul_mbps = _fmt_mbps_str(tool_result.get("throughput_mbps"))
    else:
        direction = str(tool_result.get("direction") or test_type.replace("iperf_", ""))
        dl_mbps = ""
        ul_mbps = _fmt_mbps_str(tool_result.get("throughput_mbps"))
    ct = tool_result.get("connect_timeout_sec")
    ct_s = ""
    if ct is not None and str(ct).strip() != "":
        try:
            ctf = float(ct)
            ct_s = str(int(ctf)) if ctf == int(ctf) else str(round(ctf, 3)).rstrip("0").rstrip(".")
        except (TypeError, ValueError):
            ct_s = str(ct)
    return {
        "iperf_host": str(tool_result.get("host") or ""),
        "iperf_port": str(tool_result.get("port") or ""),
        "iperf_duration_sec": str(tool_result.get("duration_sec") or ""),
        "iperf_parallel_streams": str(tool_result.get("parallel_streams") or ""),
        "iperf_connect_timeout_sec": ct_s,
        "iperf_protocol": str(tool_result.get("protocol") or ""),
        "iperf_bitrate_limit_mbps": "" if cfg.get("bitrate_limit_mbps") is None else str(cfg.get("bitrate_limit_mbps")),
        "iperf_mobile_only": str(bool(tool_result.get("mobile_only"))).lower(),
        "iperf_direction": direction,
        "iperf_throughput_dl_mbps": dl_mbps,
        "iperf_throughput_ul_mbps": ul_mbps,
    }


def volte_tool_csv_columns(cfg: dict[str, Any], tool_result: dict[str, Any]) -> dict[str, str]:
    setup_ms = tool_result.get("setup_time_ms")
    ans_sec = ""
    if isinstance(setup_ms, (int, float)):
        ans_sec = str(round(float(setup_ms) / 1000.0, 3))
    ceer_raw = tool_result.get("ceer")
    ceer_txt = "" if ceer_raw is None else str(ceer_raw).strip()
    return {
        "volte_number": str(tool_result.get("number") or ""),
        "volte_connected": str(bool(tool_result.get("call_connected"))).lower(),
        "volte_answer_delay_sec": ans_sec,
        "volte_call_duration_sec": str(tool_result.get("call_duration_s") or ""),
        "volte_ceer": ceer_txt,
        "volte_modem_call_messages": _volte_modem_msgs_csv(tool_result),
    }


def tool_csv_columns_for_test_type(
    test_type: str, cfg: dict[str, Any], tool_result: dict[str, Any]
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Ping / iperf / VoLTE tool columns for one iteration; inactive tool families are empty strings."""
    ep, ei, ev = _empty_ping_tool_cols(), _empty_iperf_tool_cols(), _empty_volte_tool_cols()
    if test_type == "ping":
        ep = ping_tool_csv_columns(cfg, tool_result)
    elif test_type in ("iperf_download", "iperf_upload", "iperf_download_upload"):
        ei = iperf_tool_csv_columns(cfg, tool_result, test_type)
    elif test_type == "volte_call_outbound":
        ev = volte_tool_csv_columns(cfg, tool_result)
    return ep, ei, ev


def build_csv_row(
    *,
    project_name: str,
    test_location: str,
    engineer: str,
    modem_antenna_config: str,
    note: str,
    run_started_utc: str,
    run_ended_utc: str,
    profile_name: str,
    test_type: str,
    run_success: bool,
    run_error: str,
    run_duration_ms: int,
    test_config_json: str,
    test_iteration_index: int,
    test_iterations_total: int,
    test_iteration_delay_sec: str,
    ping_cols: dict[str, str],
    iperf_cols: dict[str, str],
    volte_cols: dict[str, str],
    agg: dict[str, str],
) -> list[str]:
    """Column order: lab metadata, iteration settings, active tool results, then RF KPI aggregates."""
    return [
        project_name or "",
        test_location or "",
        engineer or "",
        modem_antenna_config or DEFAULT_MODEM_ANTENNA_CONFIG,
        note or "",
        run_started_utc,
        run_ended_utc,
        profile_name,
        test_type,
        "true" if run_success else "false",
        run_error or "",
        str(run_duration_ms),
        test_config_json,
        str(int(test_iteration_index)),
        str(int(test_iterations_total)),
        test_iteration_delay_sec or "0",
        ping_cols.get("ping_host", ""),
        ping_cols.get("ping_count", ""),
        ping_cols.get("ping_loss_pct", ""),
        ping_cols.get("ping_rtt_avg_ms", ""),
        ping_cols.get("ping_rtt_min_ms", ""),
        ping_cols.get("ping_rtt_max_ms", ""),
        ping_cols.get("ping_jitter_ms", ""),
        iperf_cols.get("iperf_host", ""),
        iperf_cols.get("iperf_port", ""),
        iperf_cols.get("iperf_duration_sec", ""),
        iperf_cols.get("iperf_parallel_streams", ""),
        iperf_cols.get("iperf_connect_timeout_sec", ""),
        iperf_cols.get("iperf_protocol", ""),
        iperf_cols.get("iperf_bitrate_limit_mbps", ""),
        iperf_cols.get("iperf_mobile_only", ""),
        iperf_cols.get("iperf_direction", ""),
        iperf_cols.get("iperf_throughput_dl_mbps", ""),
        iperf_cols.get("iperf_throughput_ul_mbps", ""),
        volte_cols.get("volte_number", ""),
        volte_cols.get("volte_connected", ""),
        volte_cols.get("volte_answer_delay_sec", ""),
        volte_cols.get("volte_call_duration_sec", ""),
        volte_cols.get("volte_ceer", ""),
        volte_cols.get("volte_modem_call_messages", ""),
        agg.get("kpi_sample_count", ""),
        agg.get("primary_rsrp_avg_dbm", ""),
        agg.get("primary_rssi_avg_dbm", ""),
        agg.get("primary_rsrq_avg_db", ""),
        agg.get("primary_sinr_avg_db", ""),
        agg.get("primary_earfcn_pci_most_common", ""),
        agg.get("primary_cell_pci_most_common", ""),
        agg.get("primary_cell_id_most_common", ""),
        agg.get("lock_rat_mode", ""),
        agg.get("lock_lte_bands", ""),
        agg.get("lock_ca_policy", ""),
        agg.get("lock_nr_bands", ""),
        agg.get("lock_nrdc", ""),
        agg.get("apn_most_common", ""),
        agg.get("operator_most_common", ""),
        agg.get("rat_most_common", ""),
        agg.get("registration_state_most_common", ""),
        agg.get("endc_state", ""),
        agg.get("ca_status_most_common", ""),
        agg.get("ca_carriers_pcc_scc", ""),
        agg.get("ca_aggregated_dl_bw_mhz_avg", ""),
        agg.get("lte_pcell_earfcn_reselections_per_min_avg", ""),
        agg.get("lte_pcell_pci_reselections_per_min_avg", ""),
        agg.get("nr5g_primary_rsrp_avg_dbm", ""),
        agg.get("nr5g_primary_rsrq_avg_db", ""),
        agg.get("nr5g_primary_sinr_avg_db", ""),
        agg.get("nr5g_primary_arfcn_most_common", ""),
        agg.get("nr5g_primary_pci_most_common", ""),
        agg.get("nr5g_primary_dl_bw_mhz_avg", ""),
    ]


CSV_HEADER = [
    "project_name",
    "test_location",
    "engineer",
    "modem_antenna_config",
    "note",
    "run_started_utc",
    "run_ended_utc",
    "profile_name",
    "test_type",
    "run_success",
    "run_error",
    "run_duration_ms",
    "test_config_json",
    "test_iteration_index",
    "test_iterations_total",
    "test_iteration_delay_sec",
    "ping_host",
    "ping_count",
    "ping_loss_pct",
    "ping_rtt_avg_ms",
    "ping_rtt_min_ms",
    "ping_rtt_max_ms",
    "ping_jitter_ms",
    "iperf_host",
    "iperf_port",
    "iperf_duration_sec",
    "iperf_parallel_streams",
    "iperf_connect_timeout_sec",
    "iperf_protocol",
    "iperf_bitrate_limit_mbps",
    "iperf_mobile_only",
    "iperf_direction",
    "iperf_throughput_dl_mbps",
    "iperf_throughput_ul_mbps",
    "volte_number",
    "volte_connected",
    "volte_answer_delay_sec",
    "volte_call_duration_sec",
    "volte_ceer",
    "volte_modem_call_messages",
    "kpi_sample_count",
    "primary_rsrp_avg_dbm",
    "primary_rssi_avg_dbm",
    "primary_rsrq_avg_db",
    "primary_sinr_avg_db",
    "primary_earfcn_pci_most_common",
    "primary_cell_pci_most_common",
    "primary_cell_id_most_common",
    "lock_rat_mode",
    "lock_lte_bands",
    "lock_ca_policy",
    "lock_nr_bands",
    "lock_nrdc",
    "apn_most_common",
    "operator_most_common",
    "rat_most_common",
    "registration_state",
    "endc_state",
    "ca_status_most_common",
    "ca_carriers_pcc_scc",
    "ca_aggregated_dl_bw_mhz_avg",
    "lte_pcell_earfcn_reselections_per_min_avg",
    "lte_pcell_pci_reselections_per_min_avg",
    "nr5g_primary_rsrp_avg_dbm",
    "nr5g_primary_rsrq_avg_db",
    "nr5g_primary_sinr_avg_db",
    "nr5g_primary_arfcn_most_common",
    "nr5g_primary_pci_most_common",
    "nr5g_primary_dl_bw_mhz_avg",
]


def write_summary_csv(path: str, row: list[str]) -> None:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    w.writerow(CSV_HEADER)
    w.writerow(row)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(buf.getvalue())


def append_summary_csv_row(path: str, row: list[str]) -> None:
    """Append one data row to an existing summary CSV (same header as :func:`write_summary_csv`)."""
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        w.writerow(row)


async def kpi_jsonl_writer_task(
    *,
    path: str,
    kpi_lock: asyncio.Lock,
    get_snapshot: Callable[[], dict[str, Any]],
    interval_sec: float,
    stop: asyncio.Event,
) -> list[dict[str, Any]]:
    """Append one JSON line per sample until stop is set. Returns collected snapshots.

    Always writes at least one line: if ``stop`` is already set when this task first
    runs (e.g. very fast ``execute_test`` or HTTP gate raised before the writer was
    scheduled), a plain ``while not stop`` loop would skip the body and leave an
    empty file.
    """
    collected: list[dict[str, Any]] = []
    with open(path, "w", encoding="utf-8") as jf:
        while True:
            async with kpi_lock:
                snap = dict(get_snapshot()) if isinstance(get_snapshot(), dict) else {}
            line_obj = {"t": _utc_iso(), "snapshot": snapshot_json_copy(snap)}
            jf.write(json.dumps(line_obj, default=_json_default) + "\n")
            jf.flush()
            collected.append(snap)
            if stop.is_set():
                break
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_sec)
            except asyncio.TimeoutError:
                pass
    return collected


async def run_with_kpi_sampling(
    *,
    kpi_jsonl_path: str,
    kpi_lock: asyncio.Lock,
    get_snapshot: Callable[[], dict[str, Any]],
    interval_sec: float,
    execute_test: Callable[[], Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    """Returns kpi_pre, kpi_post, tool_result, samples_collected."""
    async with kpi_lock:
        kpi_pre = snapshot_json_copy(get_snapshot())
    stop = asyncio.Event()
    samples_collected: list[dict[str, Any]] = []

    async def writer() -> None:
        nonlocal samples_collected
        samples_collected = await kpi_jsonl_writer_task(
            path=kpi_jsonl_path,
            kpi_lock=kpi_lock,
            get_snapshot=get_snapshot,
            interval_sec=interval_sec,
            stop=stop,
        )

    writer_task = asyncio.create_task(writer())
    tool_result: dict[str, Any]
    try:
        tool_result = await execute_test()
    except HTTPException as he:
        # Keep the test-run pipeline alive so CSV / ui.json / kpi jsonl are still written.
        tool_result = {"ok": False, "error": str(he.detail)}
    finally:
        stop.set()
        await writer_task
    async with kpi_lock:
        kpi_post = snapshot_json_copy(get_snapshot())
    return kpi_pre, kpi_post, tool_result, samples_collected
