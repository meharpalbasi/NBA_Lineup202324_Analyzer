"""Self-computed Regularized Adjusted Plus-Minus (RAPM) for the current season.

RAPM is the public backbone the brand-name impact metrics (EPM, DARKO, LEBRON)
are built on: a ridge regression that solves for each player's per-100-possession
effect on offense and defense, *controlling for the other nine players on the
floor*. We compute our own — no proprietary data is re-hosted.

Why this is a from-scratch subsystem
------------------------------------
There is no pre-built lineup feed we can use for 2025-26:

* ``stats.nba.com/stats/playbyplayv2`` now returns an empty ``{}`` (deprecated);
  ``playbyplayv3`` is the live source but carries **no on-court lineups** and
  does **not** log inter-period substitutions.
* ``pbpstats`` / ``nba_on_court`` are both pinned to the dead v2 schema, and the
  public bulk PBP dumps stop at 2024-25.

So we reconstruct the on-court five-man units ourselves, per period, from v3
play-by-play + the v3 box score, and validate the result against box-score
minutes (matches to < 0.5s per player). The recipe:

  1. Enumerate Regular-Season final game ids (LeagueGameLog).
  2. Per game, fetch ``playbyplayv3`` + ``boxscoretraditionalv3`` (raw JSON,
     disk-cached so reruns are free).
  3. Per period, seed the starting five for each team:
       * Period 1 — the five players flagged with a ``position`` in the box score.
       * Later periods — players whose first chronological involvement is *not*
         a substitution-in (v3 subs are keyed by the **outgoing** player id, with
         the incoming player named only in the description, so we resolve names
         against the roster). If that yields != 5 (an eventless starter), fall
         back to a per-period box score (``RangeType=1``) for the period's player
         list and subtract the subbed-in players.
     Then walk the period's events in (elapsed, actionNumber) order — v3
     action numbers are *not* monotonic in game clock — splitting into stints
     at each substitution.
  4. Per stint, tally each team's offensive possessions
     (``FGA + 0.44*FTA - OREB + TOV``) and points (running-score deltas), and
     accumulate them per unique (offensive five, defensive five) matchup.
  5. Ridge regression (``RidgeCV``) on the sparse design matrix → O-RAPM /
     D-RAPM / total RAPM per player.
  6. Save a slim ``rapm_{season}.csv``.

HTTP goes through ``curl_cffi`` (Chrome TLS impersonation) because stats.nba.com's
Akamai bot manager silently drops plain ``requests``. We hit the raw endpoints
directly rather than via ``nba_api`` because its V3 parsing reshapes the exact
JSON fields we depend on (``actionType``/``subType``/``description``/scores).
"""

from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from . import config

logger = logging.getLogger("pipeline.fetch_rapm")

# ---------------------------------------------------------------------------
# HTTP (curl_cffi, Chrome-impersonated) + raw-JSON disk cache
# ---------------------------------------------------------------------------
_STATS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
}

CACHE_DIR: Path = config.DATA_DIR / "rapm_cache"

# Per-game PBP/box GETs are far lighter than the league-dash dashboards, so we
# pace them a little faster than the global default (still polite enough to stay
# under Akamai's radar from a residential IP).
RAPM_CALL_DELAY: float = 1.0

# The possession estimator's free-throw weight (league-standard).
_FT_POSS_WEIGHT = 0.44

# Players below this many (offensive + defensive) possessions are too noisy to
# surface; their ridge estimate is shrunk toward zero but we still flag them.
MIN_POSSESSIONS = 200


def _get_json(endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """GET a stats.nba.com endpoint as JSON via curl_cffi, with backoff retry."""
    from curl_cffi import requests as cffi

    url = f"https://stats.nba.com/stats/{endpoint}"
    last_exc: Optional[Exception] = None
    for attempt in range(config.API_RETRIES):
        try:
            resp = cffi.get(
                url,
                params=params,
                headers=_STATS_HEADERS,
                impersonate="chrome",
                timeout=config.API_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json()
            last_exc = RuntimeError(f"HTTP {resp.status_code}")
        except Exception as exc:  # pragma: no cover - network
            last_exc = exc
        wait = config.API_BASE_DELAY * (config.API_BACKOFF_MULTIPLIER ** attempt)
        logger.warning(
            "%s attempt %d/%d failed (%s) — retrying in %.1fs",
            endpoint, attempt + 1, config.API_RETRIES, str(last_exc)[:160], wait,
        )
        time.sleep(wait)
    raise RuntimeError(f"{endpoint} failed after {config.API_RETRIES} attempts: {last_exc}")


def _cached(name: str, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Return cached raw JSON for ``name`` or fetch + persist it."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{name}.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            logger.warning("Corrupt cache %s — refetching", path)
    data = _get_json(endpoint, params)
    # Write atomically (temp + replace) so an interrupted run never leaves a
    # half-written file that the next run would read as corrupt JSON.
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(path)
    time.sleep(RAPM_CALL_DELAY)
    return data


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
_CLOCK_RE = re.compile(r"PT(\d+)M([\d.]+)S")
_MIN_RE = re.compile(r"(\d+):(\d+)")
_SUB_RE = re.compile(r"SUB:\s*(.+?)\s+FOR\s+(.+)$")


def _clock_seconds(clock: str) -> float:
    """Seconds *remaining* in the period, from an ISO-8601 'PT..M..S' clock."""
    m = _CLOCK_RE.match(clock or "")
    return int(m.group(1)) * 60 + float(m.group(2)) if m else 0.0


def _period_len(period: int) -> float:
    """Length of a period in seconds (12:00 regulation, 5:00 overtime)."""
    return 720.0 if period <= 4 else 300.0


def _minutes_to_seconds(mmss: str) -> float:
    m = _MIN_RE.match(mmss or "")
    return int(m.group(1)) * 60 + int(m.group(2)) if m else 0.0


def _norm_name(s: str) -> str:
    """Accent-fold + lowercase a surname for matching.

    Substitution *descriptions* strip diacritics ("Jokic", "Valanciunas") while
    box-score ``familyName`` keeps them ("Jokić", "Valančiūnas"), so we normalise
    both sides before comparing — otherwise every accented player's sub-in fails
    to resolve and the on-court five drifts.
    """
    folded = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in folded if not unicodedata.combining(c)).lower().strip()


# ---------------------------------------------------------------------------
# Per-game lineup reconstruction → stint records
# ---------------------------------------------------------------------------
class _GameRosters:
    """Per-game roster lookups derived from the full-game box score."""

    def __init__(self, box: Dict[str, Any]):
        self.home_id: int = box["homeTeamId"]
        self.away_id: int = box["awayTeamId"]
        self.team_ids: Tuple[int, int] = (self.home_id, self.away_id)
        self.name_by_pid: Dict[int, str] = {}
        self.team_by_pid: Dict[int, int] = {}
        self.abbr_by_team: Dict[int, str] = {}
        # Per team: accent-folded familyName -> [(pid, norm firstName)], plus a
        # firstName index, for resolving substitution-in names (see resolve_in).
        self.roster: Dict[int, Dict[str, List[Tuple[int, str]]]] = {}
        self.by_first: Dict[int, Dict[str, List[int]]] = {}
        self.starters_p1: Dict[int, List[int]] = {}
        self.box_seconds: Dict[int, float] = {}

        for side in ("homeTeam", "awayTeam"):
            team = box[side]
            tid = team["teamId"]
            self.abbr_by_team[tid] = team.get("teamTricode", str(tid))
            self.roster[tid] = defaultdict(list)
            self.by_first[tid] = defaultdict(list)
            self.starters_p1[tid] = []
            for p in team["players"]:
                pid = p["personId"]
                fam = p.get("familyName", "") or ""
                first = _norm_name(p.get("firstName", "") or "")
                self.name_by_pid[pid] = p.get("nameI") or fam or str(pid)
                self.team_by_pid[pid] = tid
                self.roster[tid][_norm_name(fam)].append((pid, first))
                if first:
                    self.by_first[tid][first].append(pid)
                if p.get("position"):  # non-empty position == starter
                    self.starters_p1[tid].append(pid)
                self.box_seconds[pid] = _minutes_to_seconds(
                    (p.get("statistics") or {}).get("minutes", "0:00")
                )

    def resolve_in(self, name: str, tid: int, oncourt: set) -> Optional[int]:
        """Map a substitution's incoming-player *name* to a player id.

        v3 sub descriptions name the incoming player by surname, but with two
        wrinkles we must handle or the on-court five drifts:
          * Same-surname collisions get a first-initial prefix — "Jay. Williams"
            (Jaylin) vs "Jal. Williams" (Jalen) vs "K. Williams" (Kenrich).
          * A few international players are referred to by the *other* name part
            than the box's ``familyName`` ("Hansen" ↔ box familyName "Yang").
        We match surname first (folding accents), disambiguate collisions by the
        first-initial prefix, fall back to a first-name match, and finally use
        off-court status to break any remaining tie.
        """
        roster = self.roster.get(tid, {})
        q = _norm_name(name)

        cands: List[Tuple[int, str]] = list(roster.get(q, []))
        if not cands:
            # Split a leading "<initials>. " prefix off, e.g. "jay. williams".
            m = re.match(r"([a-z]+)\.\s+(.+)$", q)
            prefix, surname = (m.group(1), m.group(2)) if m else (None, q)
            cands = list(roster.get(surname, []))
            if not cands:  # fuzzy surname (suffixes / partials)
                for fam, lst in roster.items():
                    if surname == fam or surname in fam or fam in surname:
                        cands.extend(lst)
            if prefix and len(cands) > 1:  # disambiguate collision by first name
                pref = [c for c in cands if c[1].startswith(prefix)]
                if pref:
                    cands = pref
            if not cands:  # last resort: the name is actually a first name
                cands = [(pid, "") for pid in self.by_first.get(tid, {}).get(q, [])]

        pids = [pid for pid, _ in cands]
        off = [p for p in pids if p not in oncourt]
        if len(off) == 1:
            return off[0]
        return off[0] if off else (pids[0] if pids else None)


def _period_starters(
    period: int,
    pacts: List[Dict[str, Any]],
    rosters: _GameRosters,
    game_id: str,
) -> Dict[int, set]:
    """The five players on court for each team at the start of ``period``."""
    if period == 1:
        return {tid: set(rosters.starters_p1[tid]) for tid in rosters.team_ids}

    # Classify each player by their FIRST substitution event only — robust to
    # stray out-of-order non-sub events (v3 action numbers aren't monotonic in
    # game clock, so a player's shot can appear before the sub that brought them
    # in). A first sub of "in" means they started on the bench; "out" (or no sub
    # at all, but they touched the ball) means they were on court at tip-off.
    first_sub: Dict[int, str] = {}
    acted: set = set()
    tmp_oncourt: Dict[int, set] = {tid: set() for tid in rosters.team_ids}
    for a in pacts:
        tid = a["teamId"]
        if a["actionType"] == "Substitution":
            out_pid = a["personId"]
            mm = _SUB_RE.match(a.get("description", ""))
            in_pid = rosters.resolve_in(mm.group(1).strip(), tid, tmp_oncourt[tid]) if mm else None
            first_sub.setdefault(out_pid, "out")
            if in_pid is not None:
                first_sub.setdefault(in_pid, "in")
            tmp_oncourt[tid].discard(out_pid)
            if in_pid is not None:
                tmp_oncourt[tid].add(in_pid)
        else:
            pid = a.get("personId")
            if pid and pid in rosters.name_by_pid:
                acted.add(pid)

    involved = acted | set(first_sub)
    starters: Dict[int, set] = {tid: set() for tid in rosters.team_ids}
    for pid in involved:
        tid = rosters.team_by_pid.get(pid)
        if tid in starters and first_sub.get(pid) != "in":
            starters[tid].add(pid)

    # Fallback: an eventless starter (played the whole period with no recorded
    # stat) is invisible to the pass above. Use the per-period box score for the
    # period's true player list, then keep those whose first sub wasn't an "in".
    if any(len(starters[tid]) != 5 for tid in rosters.team_ids):
        try:
            pbox = _cached(
                f"box_{game_id}_p{period}",
                "boxscoretraditionalv3",
                {"GameID": game_id, "StartPeriod": period, "EndPeriod": period,
                 "StartRange": 0, "EndRange": 28800, "RangeType": 1},
            )["boxScoreTraditional"]
            for side in ("homeTeam", "awayTeam"):
                tid = pbox[side]["teamId"]
                played = {
                    p["personId"] for p in pbox[side]["players"]
                    if _minutes_to_seconds((p.get("statistics") or {}).get("minutes", "0:00")) > 0
                }
                starters[tid] = {pid for pid in played if first_sub.get(pid) != "in"}
        except Exception as exc:  # pragma: no cover - network/shape
            logger.warning("per-period box fallback failed for %s P%d: %s", game_id, period, exc)

    for tid in rosters.team_ids:
        if len(starters[tid]) != 5:
            logger.warning(
                "%s P%d %s: reconstructed %d starters (expected 5)",
                game_id, period, rosters.abbr_by_team.get(tid), len(starters[tid]),
            )
    return starters


def reconstruct_game(game_id: str) -> Tuple[List[Dict[str, Any]], "_GameRosters"]:
    """Return per-stint matchup records for one game (plus its rosters).

    Each record: ``{off: (5 ids), def: (5 ids), poss: float, pts: int}`` — one
    for each team's offensive share of every contiguous-lineup stint.
    """
    pbp = _cached(f"pbp_{game_id}", "playbyplayv3",
                  {"GameID": game_id, "StartPeriod": 0, "EndPeriod": 14})
    box = _cached(f"box_{game_id}", "boxscoretraditionalv3",
                  {"GameID": game_id, "StartPeriod": 0, "EndPeriod": 14,
                   "StartRange": 0, "EndRange": 28800, "RangeType": 0})
    rosters = _GameRosters(box["boxScoreTraditional"])
    home_id, away_id = rosters.home_id, rosters.away_id
    actions = pbp["game"]["actions"]

    records: List[Dict[str, Any]] = []
    # v3 scores are cumulative game totals, so the running score must carry
    # across periods (resetting it would give the first stint of each period a
    # bogus point delta).
    score = {home_id: 0, away_id: 0}
    periods = sorted({a["period"] for a in actions})
    for period in periods:
        pacts = sorted(
            [a for a in actions if a["period"] == period and a["actionType"] != "period"],
            key=lambda a: (_period_len(period) - _clock_seconds(a["clock"]), a["actionNumber"]),
        )
        starters = _period_starters(period, pacts, rosters, game_id)
        oncourt = {tid: set(starters[tid]) for tid in rosters.team_ids}

        def _new_stint() -> Dict[str, Any]:
            return {
                "home": frozenset(oncourt[home_id]),
                "away": frozenset(oncourt[away_id]),
                "fga": {home_id: 0, away_id: 0},
                "fta": {home_id: 0, away_id: 0},
                "tov": {home_id: 0, away_id: 0},
                "oreb": {home_id: 0, away_id: 0},
                "score0": dict(score),
            }

        def _close_stint(st: Dict[str, Any]) -> None:
            for off_tid, def_tid in ((home_id, away_id), (away_id, home_id)):
                poss = (st["fga"][off_tid] + _FT_POSS_WEIGHT * st["fta"][off_tid]
                        - st["oreb"][off_tid] + st["tov"][off_tid])
                pts = score[off_tid] - st["score0"][off_tid]
                if poss > 0:
                    records.append({
                        "off": tuple(sorted(st["home"] if off_tid == home_id else st["away"])),
                        "def": tuple(sorted(st["away"] if off_tid == home_id else st["home"])),
                        "poss": float(poss),
                        "pts": int(pts),
                    })

        cur = _new_stint()
        last_shot_team: Optional[int] = None
        for a in pacts:
            # Keep the running score current from every event that carries one.
            for key, fld in ((home_id, "scoreHome"), (away_id, "scoreAway")):
                v = a.get(fld)
                if v not in (None, ""):
                    try:
                        score[key] = int(v)
                    except (TypeError, ValueError):
                        pass

            atype = a["actionType"]
            tid = a["teamId"]
            if atype == "Substitution":
                _close_stint(cur)
                out_pid = a["personId"]
                mm = _SUB_RE.match(a.get("description", ""))
                in_pid = rosters.resolve_in(mm.group(1).strip(), tid, oncourt[tid]) if mm else None
                if tid in oncourt:
                    oncourt[tid].discard(out_pid)
                    if in_pid is not None:
                        oncourt[tid].add(in_pid)
                cur = _new_stint()
                last_shot_team = None
            elif tid in (home_id, away_id):
                if atype in ("Made Shot", "Missed Shot", "Heave"):
                    cur["fga"][tid] += 1
                    last_shot_team = tid
                elif atype == "Free Throw":
                    # Technical (and other non-shooting-foul) free throws don't
                    # consume a possession, so they're excluded from the FTA term
                    # of the estimator. A missed *shooting* free throw can be
                    # offensive-rebounded, so it must update last_shot_team too —
                    # otherwise the rebound is wrongly credited to the prior FG's
                    # team.
                    sub = (a.get("subType") or "")
                    desc = (a.get("description") or "")
                    is_technical = "Technical" in sub or "Technical" in desc
                    if not is_technical:
                        cur["fta"][tid] += 1
                        last_shot_team = tid
                elif atype == "Turnover":
                    cur["tov"][tid] += 1
                elif atype == "Rebound" and tid == last_shot_team:
                    cur["oreb"][tid] += 1
        _close_stint(cur)

    return records, rosters


# ---------------------------------------------------------------------------
# Ridge regression
# ---------------------------------------------------------------------------
def compute_rapm(
    matchups: Dict[Tuple[Tuple[int, ...], Tuple[int, ...]], Dict[str, float]],
    name_by_pid: Dict[int, str],
    team_by_pid: Dict[int, str],
    season: str,
    season_type: str,
) -> pd.DataFrame:
    """Solve ridge RAPM from accumulated (offense, defense) matchups."""
    from scipy import sparse
    from sklearn.linear_model import RidgeCV

    players = sorted({pid for off, dfn in matchups for pid in (*off, *dfn)})
    idx = {pid: i for i, pid in enumerate(players)}
    n_players = len(players)
    logger.info("RAPM: %d players, %d unique matchups", n_players, len(matchups))

    rows, cols, vals = [], [], []
    y, weights = [], []
    poss_off = defaultdict(float)
    poss_def = defaultdict(float)
    for r, (key, agg) in enumerate(matchups.items()):
        off, dfn = key
        poss, pts = agg["poss"], agg["pts"]
        for pid in off:
            rows.append(r); cols.append(idx[pid]); vals.append(1.0)
            poss_off[pid] += poss
        for pid in dfn:
            rows.append(r); cols.append(idx[pid] + n_players); vals.append(-1.0)
            poss_def[pid] += poss
        y.append(100.0 * pts / poss)
        weights.append(poss)

    X = sparse.csr_matrix((vals, (rows, cols)), shape=(len(matchups), 2 * n_players))
    y = np.asarray(y)
    weights = np.asarray(weights)

    # rd11490's lambda→alpha scaling: alpha = lambda * n / 2 (n = total possessions).
    total_poss = float(weights.sum())
    lambdas = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0]
    alphas = [l * total_poss / 2.0 for l in lambdas]
    model = RidgeCV(alphas=alphas, fit_intercept=True, cv=5)
    model.fit(X, y, sample_weight=weights)
    logger.info("RAPM: chosen alpha=%.1f (intercept/lg-avg ORtg=%.1f)",
                model.alpha_, model.intercept_)

    coef = model.coef_
    # Round the components, then derive the total from them so the published
    # columns are exactly additive (O_RAPM + D_RAPM == RAPM) — the profile shows
    # all three as separate bars, so they must reconcile.
    o_rapm = np.round(coef[:n_players], 2)
    d_rapm = np.round(coef[n_players:], 2)
    out = pd.DataFrame({
        "PLAYER_ID": players,
        "PLAYER_NAME": [name_by_pid.get(p, str(p)) for p in players],
        "TEAM_ABBREVIATION": [team_by_pid.get(p, "") for p in players],
        "O_RAPM": o_rapm,
        "D_RAPM": d_rapm,
        "RAPM": np.round(o_rapm + d_rapm, 2),
        "POSS": [round(poss_off[p] + poss_def[p]) for p in players],
        "SEASON": season,
        "SEASON_TYPE": season_type,
    })
    return out.sort_values("RAPM", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Game-id enumeration + orchestration
# ---------------------------------------------------------------------------
def fetch_game_ids(season: str, season_type: str) -> List[str]:
    """Final game ids for a season via LeagueGameLog (deduped, one per game)."""
    data = _cached(
        f"schedule_{season}_{season_type.replace(' ', '')}",
        "leaguegamelog",
        {"Counter": 0, "Direction": "ASC", "LeagueID": "00", "PlayerOrTeam": "T",
         "Season": season, "SeasonType": season_type, "Sorter": "DATE"},
    )
    rs = data["resultSets"][0]
    headers = rs["headers"]
    gi = headers.index("GAME_ID")
    return sorted({row[gi] for row in rs["rowSet"]})


def fetch_rapm(
    season: str = config.SEASON,
    season_type: str = "Regular Season",
    max_games: Optional[int] = None,
) -> pd.DataFrame:
    """Build the RAPM table for a season and return it (also written to CSV)."""
    game_ids = fetch_game_ids(season, season_type)
    if max_games:
        game_ids = game_ids[:max_games]
    logger.info("RAPM: reconstructing %d games for %s %s", len(game_ids), season, season_type)

    matchups: Dict[Tuple[Tuple[int, ...], Tuple[int, ...]], Dict[str, float]] = defaultdict(
        lambda: {"poss": 0.0, "pts": 0}
    )
    name_by_pid: Dict[int, str] = {}
    team_by_pid: Dict[int, str] = {}
    failed = 0
    for n, gid in enumerate(game_ids, 1):
        try:
            records, rosters = reconstruct_game(gid)
        except Exception as exc:
            failed += 1
            logger.warning("game %s failed: %s", gid, exc)
            continue
        name_by_pid.update(rosters.name_by_pid)
        for pid, tid in rosters.team_by_pid.items():
            team_by_pid[pid] = rosters.abbr_by_team.get(tid, "")
        for rec in records:
            key = (rec["off"], rec["def"])
            agg = matchups[key]
            agg["poss"] += rec["poss"]
            agg["pts"] += rec["pts"]
        if n % 100 == 0:
            logger.info("RAPM: %d/%d games (%d matchups, %d failed)",
                        n, len(game_ids), len(matchups), failed)

    df = compute_rapm(matchups, name_by_pid, team_by_pid, season, season_type)
    out_path = config.DATA_DIR / f"rapm_{season}.csv"
    df.to_csv(out_path, index=False)
    logger.info("Saved %d players → %s (%d games, %d failed)",
                len(df), out_path, len(game_ids) - failed, failed)
    return df


if __name__ == "__main__":  # pragma: no cover - manual/sample runs
    import os
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    mg = os.getenv("RAPM_MAX_GAMES")
    df = fetch_rapm(max_games=int(mg) if mg else None)
    shown = df[df["POSS"] >= MIN_POSSESSIONS]
    print("\n=== TOP 25 RAPM (POSS >= %d) ===" % MIN_POSSESSIONS)
    print(shown.head(25).to_string(index=False))
    print("\n=== BOTTOM 10 ===")
    print(shown.tail(10).to_string(index=False))
