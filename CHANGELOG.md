2026-02-11 - API

- `GET /stats`:
  - The `days` query parameter no longer has a hard-coded 10-year cap.
  - An optional environment variable `MAX_STATS_DAYS` may be set to an integer
    number of days to enforce an upper-bound for safety (when unset or empty,
    no cap is applied).
