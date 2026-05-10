"""Pydantic request-body models shared across API route handlers."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class SendAtBody(BaseModel):
    command: str = Field(min_length=1, description="AT command without CRLF")
    timeout_sec: float = Field(default=2.0, ge=0.2, le=30.0)


class ReopenBody(BaseModel):
    port: str = Field(min_length=1)
    baudrate: int = Field(default=115200, ge=300, le=4000000)


class KpiPollBody(BaseModel):
    """KPI sampling is fixed at 2 Hz; `poll_hz` must be 2.0 (accepted for API compatibility)."""

    poll_hz: Literal[2.0] = Field(default=2.0, description="Fixed at 2.0 Hz")


class CopsSetBody(BaseModel):
    mode: int = Field(description="AT+COPS mode (0=auto register, 2=deregister)")


class MnoSelectBody(BaseModel):
    profile: str = Field(description="One of: vodafone, vmo2, ee, h3g, auto")
    cops_manual_registration: int = Field(
        default=4,
        description="For named profiles: AT+COPS mode — 1 = manual (stay on selected PLMN); 4 = manual with automatic fallback (default). Ignored for profile=auto.",
    )
    deregister_before_apply: bool = Field(
        default=True,
        description="For named profiles: if True, send AT+COPS=2 (deregister from network) before manual PLMN selection so switches complete quickly on many routers/modems.",
    )


class DataGateBody(BaseModel):
    inhibit: bool = Field(description="True=inhibit packet data, False=allow packet data")
    password: str | None = Field(default=None, description="Required when inhibit=false")


class ApnSetBody(BaseModel):
    apn: str = Field(min_length=1, max_length=100, description="PDP APN string for AT+CGDCONT")
    cid: int = Field(default=1, ge=1, le=15, description="PDP context ID (typically 1)")
    pdp_type: str = Field(
        default="IP",
        description='PDP type passed to AT+CGDCONT, e.g. "IP", "IPV6", "IPV4V6"',
    )
    password: str | None = Field(default=None, description="Unlock password (same as data allow)")
    pdp_auth_type: int = Field(
        default=0,
        ge=0,
        le=3,
        description="3GPP +CGAUTH / Quectel +QICSGP: 0=none, 1=PAP, 2=CHAP, 3=PAP or CHAP",
    )
    pdp_username: str | None = Field(default=None, max_length=64, description="PDP username (optional)")
    pdp_password: str | None = Field(
        default=None,
        max_length=64,
        description="PDP password for PAP/CHAP; omit or null for empty. Not the dashboard unlock password.",
    )
    reactivate: bool = Field(
        default=True,
        description="After CGDCONT, reattach data (CGATT/QIACT when needed). Disable to only write CGDCONT (+QICSGP) if the context is inactive.",
    )


class LockSetBody(BaseModel):
    rat_mode: str | None = Field(default=None, description='QNWPREFCFG mode_pref, e.g. AUTO/LTE/NR5G')
    lte_band: str | None = Field(default=None, description='QNWPREFCFG lte_band string')
    nr5g_band: str | None = Field(default=None, description='QNWPREFCFG nr5g_band or nsa_nr5g_band string')
    nrdc_mode: int | None = Field(default=None, description='QNWPREFCFG nrdc_mode (0=off,1=on)')


class VolteTestBody(BaseModel):
    number: str = Field(min_length=3, max_length=40, description="Dial number, e.g. +447700900123")
    hold_sec: int = Field(default=10, ge=1, le=120, description="Call hold duration before hangup")
    connect_timeout_sec: int = Field(
        default=120,
        ge=20,
        le=300,
        description="Max seconds to wait for CLCC active/held (voice) after dial",
    )
    password: str | None = Field(default=None, description="Unlock password (same as data allow password)")


class VoiceHangupBody(BaseModel):
    password: str | None = Field(default=None, description="Unlock password (same as VoLTE / data allow)")


class VoiceAnswerBody(BaseModel):
    password: str | None = Field(default=None, description="Unlock password (same as VoLTE / data allow)")


class AutoAnswerSetBody(BaseModel):
    enabled: bool = Field(description="False → ATS0=0 (no auto-answer); True → ATS0=rings")
    rings: int = Field(
        default=2,
        ge=1,
        le=255,
        description="Rings before auto-answer (only when enabled=True)",
    )
    password: str | None = Field(default=None, description="Unlock password (same as data allow / VoLTE test)")


class HostAutoAnswerBody(BaseModel):
    """Enable/disable background watcher that sends ``ATA`` after N rings (VoLTE-friendly)."""

    enabled: bool
    rings: int = Field(default=2, ge=1, le=255)
    password: str | None = Field(default=None, description="Required when enabled=True")


class IperfTestBody(BaseModel):
    host: str = Field(default="iperf.as42831.net", min_length=1)
    port: int = Field(default=5361, ge=1, le=65535)
    port_range_max: int | None = Field(
        default=None,
        ge=1,
        le=65535,
        description=(
            "When set, the client picks one TCP port uniformly at random in [port, port_range_max] "
            "(inclusive) for this run. Omit for a fixed port."
        ),
    )
    duration_sec: int = Field(default=1, ge=1, le=300)
    direction: str = Field(default="download", description="download=server->client, upload=client->server")
    protocol: str = Field(default="tcp", description="Traffic mode: 'tcp' or 'udp'.")
    mobile_only: bool = Field(default=True, description="Bind iperf to mobile data interface/IP only.")
    bind_ip: str | None = Field(default=None, description="Optional local IPv4 to bind using iperf -B.")
    bitrate_limit_mbps: float | None = Field(
        default=None,
        ge=0,
        description=(
            "Target bitrate for iperf -b (Mbit/s). "
            "TCP: pacing hint only (Linux-effective); UDP: hard cap, defaults to iperf3's 1 Mbit/s if unset. "
            "0 or None = use iperf3 default."
        ),
    )
    parallel_streams: int = Field(
        default=10,
        ge=1,
        le=64,
        description="iperf3 parallel streams (-P), 1–64.",
    )
    connect_timeout_sec: float = Field(
        default=10.0,
        ge=1.0,
        le=120.0,
        description=(
            "iperf3 control-connection startup budget in seconds (maps to --connect-timeout in ms when the binary supports it). "
            "Default 10. Bundled iperf 3.1.1 omits the flag but the subprocess wall-clock still allows this headroom."
        ),
    )


class IcmpPingSweepBody(BaseModel):
    host: str = Field(default="8.8.8.8", min_length=1, max_length=253)
    count: int = Field(default=10, ge=1, le=100)
    bind_ipv4: str | None = Field(default=None, description="Windows: ping -S source IPv4 (optional).")
    timeout_ms: int | None = Field(
        default=None,
        ge=500,
        le=60000,
        description="Windows: per-reply timeout for ping -w (ms). Default 3000 when omitted.",
    )


class TestRunBody(BaseModel):
    profile_name: str = Field(min_length=1, max_length=120)
    project_name: str = Field(default="", max_length=200)
    test_location: str = Field(default="", max_length=400)
    engineer: str = Field(default="", max_length=200)
    note: str = Field(default="", max_length=4000, description="Optional free-text note stored on the run (CSV + UI snapshot).")
    ping_bind_ipv4_override: str | None = Field(
        default=None,
        description="ping profiles only: set to force bind (-S on Windows). Empty string = OS default route (no bind). Omit to use profile test_config.bind_ipv4.",
    )
    include_ui_snapshot: bool = True
    ui_controls: dict[str, Any] | None = Field(
        default=None,
        description="Optional client dashboard control values; password-like keys are redacted server-side.",
    )
    unlock_password: str | None = Field(
        default=None,
        description="Required for every test run; must match the dashboard Allow Data unlock password.",
    )
    test_iterations: int = Field(default=1, ge=1, le=100, description="Run the profile tool this many times; CSV gets one row per iteration.")
    test_iteration_delay_sec: float = Field(
        default=10.0,
        ge=10.0,
        le=3600.0,
        description="Seconds to wait between iterations (minimum 10; not applied after the last).",
    )


class TestCancelBody(BaseModel):
    run_id: str | None = Field(
        default=None,
        max_length=32,
        description="If set, must match the active test run id or the cancel request is rejected.",
    )
