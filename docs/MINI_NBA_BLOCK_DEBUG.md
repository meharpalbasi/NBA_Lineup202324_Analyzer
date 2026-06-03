# Mac mini → stats.nba.com block — debug notes

_Context dump for a future debugging session. The data pipeline fetches
`stats.nba.com` via `nba_api`. It works on the **laptop** but **not the Mac mini**,
despite the two being indistinguishable on every metric we checked._

## TL;DR
1. **Feb 2026 NBA change (SOLVED):** stats.nba.com sits behind Akamai Bot Manager,
   which since ~Feb 12–19 2026 drops requests whose **TLS/JA3 fingerprint** isn't a
   real browser's. Plain `requests`/urllib3/`curl` complete the TLS handshake then get
   **no response** (read timeout). Fix: route `nba_api` through **`curl_cffi`** with
   `impersonate="chrome"` — see `pipeline/nba_http_patch.py`. Verified working on the laptop.
   (Ref: nba_api issues #633, #652, #678.)
2. **Mac mini block (UNSOLVED):** with the *identical* stack, the mini still times out on
   `stats.nba.com` only. Non-NBA HTTPS works. Cause not yet found. **Workaround: the
   pipeline runs on the laptop instead.**

## Environments
| | Laptop (WORKS) | Mac mini (BLOCKED) |
|---|---|---|
| Repo | `/Users/meharpalbasi/Documents/nbalineup_backend` | `/Users/meharpal/Documents/NBA_Lineup202324_Analyzer` |
| Python | 3.12.x | 3.12.13 (Homebrew) |
| curl_cffi | 0.15.0 | 0.15.0 |
| Public IP | 86.188.96.240 | **86.188.96.240 (same)** |
| Default route | en0 → 192.168.1.254 | en1 → 192.168.1.254 (same gw) |

## Symptom
Every HTTPS request to `stats.nba.com` from the mini fails with
`curl: (28) Operation timed out ... 0 bytes received` after ~20s. Affects:
`curl_cffi` (all `chromeNNN` fingerprints), **forced IPv4 *and* IPv6**, and plain `curl`.
Non-NBA HTTPS is fine (`api.ipify.org` via curl_cffi returns the IP).

Connectivity check (expect `200`, hangs on the mini):
```bash
./venv/bin/python -c "from curl_cffi import requests as r; print(r.get('https://stats.nba.com/stats/scoreboardv2?DayOffset=0&LeagueID=00&GameDate=2026-01-15', impersonate='chrome', timeout=20).status_code)"
```

## Ruled out (with evidence)
- **Software / Python / curl_cffi version** — mini matched to laptop exactly (3.12.13 + curl_cffi 0.15.0); still fails.
- **TLS fingerprint** — `impersonate="chrome"` works on laptop; `chrome/131/124/120/116/110` all fail on mini.
- **Public IP / NAT** — identical `86.188.96.240` both machines.
- **DNS** — both resolve `stats.nba.com → e8017.dsci.akamaiedge.net → 104.103.196.132`.
- **IP version** — `CurlOpt.IPRESOLVE` forced to IPv4 *and* IPv6 both time out on mini; both return 200 on laptop.
- **Routing** — `route -n get 104.103.196.132` → direct via en1 → 192.168.1.254 (no `utun`/VPN hijack).
- **Raw TCP** — `nc -vz 104.103.196.132 443` **SUCCEEDS** on the mini (and laptop).
- **/etc/hosts** — no nba entry. **Proxy** — `scutil --proxy` none. **VPN** — `scutil --nc list` empty.
- **Filter apps** — none (Little Snitch / LuLu / AdGuard / etc. not installed).
- **System extensions** — `systemextensionsctl list` empty. **MDM/profiles** — none.

## The key clue
Raw TCP to the Akamai IP **connects** (`nc` succeeds), but a TLS handshake carrying
**SNI=`stats.nba.com`** is silently dropped (0 bytes). That signature = **SNI/hostname-based
filtering** that triggers on the nba.com hostname — but no on-device filter was found via CLI,
and the laptop on the same public IP is unaffected.

## Leading hypotheses (not yet checked)
1. **Router per-device filtering** (by the mini's MAC) — parental controls / SNI filter on the
   home router targeting the mini but not the laptop. **Most likely given everything on the mini is clean.**
2. **Screen Time → Content & Privacy → Content Restrictions** web filter (GUI-only; unchecked).
3. A **NetworkExtension content filter / DNS proxy** not surfaced by the name grep.
4. **PF firewall** rule on the mini.

## Next steps (in priority order)
1. **Phone-hotspot test (most decisive, cheapest):** connect the mini to a phone hotspot and run
   the connectivity check.
   - Returns `200` → it's the **home router filtering the mini's MAC**. Fix in the router admin
     (remove the mini from any kids/filtered device profile), or give the mini a different MAC/IP.
   - Still hangs → the block is **on the mini** → continue below.
2. **Screen Time:** System Settings → Screen Time → Content & Privacy → Content Restrictions →
   Web Content. Turn off / unrestrict.
3. **Filters/extensions:** System Settings → Privacy & Security (look for Network/Content filters
   or DNS proxies); `pluginkit -mAvvv 2>/dev/null | grep -i filter`; full `systemextensionsctl list`.
4. **Firewall:** `sudo pfctl -sr` and `sudo pfctl -s nat`.
5. **TLS-layer pinpoint:** `openssl s_client -connect 104.103.196.132:443 -servername stats.nba.com`
   on the mini — does the handshake complete (→ response-drop) or stall (→ SNI-drop)?

## Once fixed
- Install the mini schedule: `cp scripts/com.nbalineup.supplementary.mini.plist ~/Library/LaunchAgents/com.nbalineup.supplementary.plist && launchctl load ...`
- **Remove the laptop's LaunchAgent** (`launchctl unload ~/Library/LaunchAgents/com.nbalineup.supplementary.plist`) so both don't push and race.

## Key files
- `pipeline/nba_http_patch.py` — the curl_cffi (Chrome TLS) patch, imported by `pipeline/main.py`.
- `requirements-pipeline.txt` — minimal runtime deps (incl. `curl_cffi`).
- `scripts/run_supplementary.sh` — fetch + commit/push (uses `--supplementary-only`).
- `scripts/com.nbalineup.supplementary.plist` (laptop paths) / `…mini.plist` (mini paths).
- Railway still independently keeps the legacy 5-man lineup CSV fresh.
