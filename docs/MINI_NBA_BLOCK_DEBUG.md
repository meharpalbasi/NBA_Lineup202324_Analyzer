# Mac mini → stats.nba.com "block" — RESOLVED (it was never a block)

_Context dump from the debugging session that closed this out. The pipeline
fetches `stats.nba.com` via `nba_api`. It was believed to work on the **laptop**
but not the **Mac mini**. The mini turned out to be fine — the "block" was a
**false negative in the test command**, not a network problem._

## TL;DR (RESOLVED 2026-06-03)
1. **Feb 2026 NBA change (still true):** stats.nba.com sits behind Akamai Bot
   Manager, which since ~Feb 2026 drops requests whose **TLS/JA3 fingerprint**
   isn't a real browser's. Fix: route `nba_api` through **`curl_cffi`** with
   `impersonate="chrome"` — see `pipeline/nba_http_patch.py`.
2. **"Mac mini block" — was a mis-diagnosis.** stats.nba.com is fully reachable
   from the mini. The connectivity "gate test" everyone was running is **invalid**:
   it issues a bare `curl_cffi` GET to a `/stats/*` endpoint **with no NBA headers**.
   The `/stats/*` endpoints **silently hold the connection open (0 bytes → ~20s
   timeout) when the required headers are missing** — on *any* machine, laptop
   included. The real `nba_api` pipeline sets those headers, so it works fine here.

   The required headers are: `Referer: https://www.nba.com/`,
   `Origin: https://www.nba.com`, `x-nba-stats-origin: stats`,
   `x-nba-stats-token: true` (plus a browser `User-Agent`/`Accept`). `nba_api`'s
   `STATS_HEADERS` includes all of them; the gate one-liner included none.

## Evidence that disproved the network/SNI/router theory
Run on the mini (Wi-Fi `en1`, gw `192.168.1.254`, same home network as the laptop):

| Test | Result | Conclusion |
|---|---|---|
| `openssl s_client -connect 104.103.196.132:443 -servername stats.nba.com` | handshake **completes**, returns `*.nba.com` cert, `Verify return code: 0` | SNI=stats.nba.com is **not** dropped on the path |
| `curl -4 -v https://stats.nba.com/` (root) | **301 in 0.04s** | host fully reachable over the same TLS/SNI |
| `curl -4 https://stats.nba.com/stats/scoreboardv2?...` (no NBA headers) | handshake OK, GET sent, **0 bytes → timeout** | the `/stats/*` path stalls a header-less request |
| same `/stats/scoreboardv2` **with** NBA headers, `curl_cffi` chrome | **200, 13 KB JSON** | adding the headers fixes it — not a network issue |
| `nba_api` ScoreboardV2/CommonAllPlayers via the curl_cffi patch | **200 JSON / 139 players** | real pipeline path works |
| `python -m pipeline.main --supplementary-only --dry-run` | **health check: 2000 rows** | full pipeline wiring works on the mini |

A network/router/SNI filter targeting `stats.nba.com` **cannot** let `/` through
(301) while dropping `/stats/*` — they ride the same TLS connection. So the
earlier "TCP connects but SNI-TLS is dropped" clue was a misread: the handshake
*does* complete; only the header-less `/stats/*` request gets no response.

## Also confirmed clean (so future sessions don't re-check)
- Active path: Wi-Fi `en1` → home router `192.168.1.254` (Ethernet `en0` inactive).
  The earlier "hotspot" test never left the home network (gw was still `192.168.1.254`),
  which is why it looked like a false confirmation.
- DNS = the router (`192.168.1.254`); `stats.nba.com` resolves normally to
  `e8017.dsci.akamaiedge.net → 104.103.196.132`. No pinned filtering resolver.
- No pf rules; pf effectively off. No proxy, VPN, /etc/hosts entry, system
  extension, MDM/profile, or content-filter app.

## A VALID connectivity test (use this, not the header-less one-liner)
```bash
# Exercises the real path: nba_api + curl_cffi patch + required headers.
./venv/bin/python -m pipeline.main --supplementary-only --dry-run
# Expect: "✓ Health check passed — got NNNN rows."
```
The old `r.get('.../stats/scoreboardv2?...', impersonate='chrome')` one-liner
**will hang even when everything is fine** — it omits the NBA headers. Don't use
it as a health signal.

## Status of the migration (done 2026-06-03)
- ✅ Confirmed stats.nba.com reachable from the mini (evidence above).
- ✅ Installed the weekly schedule:
  `cp scripts/com.nbalineup.supplementary.mini.plist ~/Library/LaunchAgents/com.nbalineup.supplementary.plist`
  then `launchctl load -w …`. Runs Mondays 08:00 (next wake if asleep).
- ⏳ **On the laptop**, remove its LaunchAgent so the two don't both push and race:
  `launchctl unload ~/Library/LaunchAgents/com.nbalineup.supplementary.plist`
  (must be done on the laptop — can't be done from the mini).

## Key files
- `pipeline/nba_http_patch.py` — the curl_cffi (Chrome TLS) patch, imported by `pipeline/main.py`.
- `requirements-pipeline.txt` — minimal runtime deps (incl. `curl_cffi`).
- `scripts/run_supplementary.sh` — fetch + commit/push (uses `--supplementary-only`).
- `scripts/com.nbalineup.supplementary.mini.plist` (mini paths) / `…supplementary.plist` (laptop paths).
- Railway still independently keeps the legacy 5-man lineup CSV fresh.
