Shipped test profiles (one JSON file per profile).

The backend merges these with profiles saved in backend/.state/test_profiles.json
(saved entries override the same profile name).

Optional top-level field ``modem_antenna_config``: ``SISO`` (default) or ``MIMO``.
Omitted keys are treated as SISO. Saved profiles get ``SISO`` written when the field
is left empty on POST /api/test/profiles.

Edit JSON here for repo defaults; use the UI or POST /api/test/profiles to persist
overrides under .state without editing these files.

``smoke_tcp_connect`` uses ``test_type`` ``tcp_connect`` (modem ``AT+QIOPEN`` → ``+QIOPEN``
URC setup time; post-close skipped on ``+QIOPEN:0,0``). Requires active PDP
(``modem_requirements.require_packet_data``).

``smoke_iperf_dlul`` uses ``test_type`` ``iperf_download_upload`` (TCP download then
upload on the same chosen port). Iperf smoke profiles (smoke_iperf_dl / smoke_iperf_ul)
set ``test_config.port`` and
optional ``port_range_max`` so each run picks a random TCP port in that inclusive range
(same as POST /api/tools/iperf-test). They also set ``connect_timeout_sec`` and optional
``omit_sec`` (iperf3 ``-O`` warmup exclusion; default 0 when omitted). If your
iperf server listens on a single port only, set ``port`` and ``port_range_max`` to that
value (or omit ``port_range_max`` for a fixed port).
