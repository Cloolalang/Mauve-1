Shipped test profiles (one JSON file per profile).

The backend merges these with profiles saved in backend/.state/test_profiles.json
(saved entries override the same profile name).

Optional top-level field ``modem_antenna_config``: ``SISO`` (default) or ``MIMO``.
Omitted keys are treated as SISO. Saved profiles get ``SISO`` written when the field
is left empty on POST /api/test/profiles.

Edit JSON here for repo defaults; use the UI or POST /api/test/profiles to persist
overrides under .state without editing these files.
