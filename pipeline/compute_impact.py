"""Box Plus/Minus (BPM 2.0, Daniel Myers) + VORP — computed OFFLINE from the
already-published per-game player_stats + team_stats. No new API calls.

Coefficients are the BPM 2.0 values (current Basketball-Reference version),
transcribed from the spec and cross-checked against two independent open-source
implementations (gerti1991/Basketball_Prediction and zfdupont/wnba-stats, which
agree to 3 decimals). Notes:
  - FGA/FTA coefficients interpolate on OFFENSIVE ROLE; all other stats on the
    estimated position.
  - Team adjustment uses the 2.0 "lead bonus" (NOT the 1.0 x1.20).
  - VORP scaling uses %Min = MP / (TeamMP / 5) so a full-season star lands ~4-8.
  - We have no position labels (the dash endpoints don't carry them), so the
    minutes-weight pull defaults to neutral (3 for position, 4 for off-role).
    Rank + scale stay faithful; values won't match BR to the decimal. Validate
    by spot-checking elite players.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger("pipeline.compute_impact")

FT = 0.44
REPLACEMENT = -2.0
PT_THRESHOLD = -0.33

# (Pos1, Pos5) — interpolated by estimated position (or offensive role for FGA/FTA).
BPM_COEF = {
    "AdjPt": (0.860, 0.860), "FGA": (-0.560, -0.780), "FTA": (-0.246, -0.343),
    "FG3": (0.389, 0.389), "AST": (0.580, 1.034), "TOV": (-0.964, -0.964),
    "ORB": (0.613, 0.181), "DRB": (0.116, 0.181), "TRB": (0.0, 0.0),
    "STL": (1.369, 1.008), "BLK": (1.327, 0.703), "PF": (-0.367, -0.367),
}
OBPM_COEF = {
    "AdjPt": (0.605, 0.605), "FGA": (-0.330, -0.472), "FTA": (-0.145, -0.208),
    "FG3": (0.477, 0.477), "AST": (0.476, 0.476), "TOV": (-0.579, -0.882),
    "ORB": (0.606, 0.422), "DRB": (-0.112, 0.103), "TRB": (0.0, 0.0),
    "STL": (0.177, 0.294), "BLK": (0.725, 0.097), "PF": (-0.439, -0.439),
}
POS = dict(INT=2.130, TRB=8.668, STL=-2.486, PF=0.992, AST=-3.536, BLK=1.667)
ROLE = dict(INT=6.000, AST=-6.642, THRESH=-8.544)
POS_CONST_1_BPM, POS_CONST_1_OBPM = -0.818, -1.698
OFFROLE_SLOPE_BPM, OFFROLE_SLOPE_OBPM = 1.387, 0.43

_RESULT_COLS = ["PLAYER_ID", "SEASON_TYPE", "OBPM", "DBPM", "BPM", "VORP"]


def _interp(coef, p):
    lo, hi = coef
    return ((5.0 - p) * lo + (p - 1.0) * hi) / 4.0


def _center(raw, mp, default, target=3.0, iters=25):
    """Minutes-weight a raw 1-5 estimate toward `default`, clamp to [1,5], and
    iterate so the minutes-weighted team mean ≈ `target` (3.0)."""
    arr = (raw * mp + default * 50.0) / (50.0 + mp)
    w = mp
    for _ in range(iters):
        trim = np.clip(arr, 1.0, 5.0)
        m = np.average(trim, weights=w) if w.sum() > 0 else target
        if abs(m - target) <= 0.005:
            break
        arr = arr - (m - target)
    return np.clip(arr, 1.0, 5.0)


def _share(stat_total, team_total, pct_min):
    if team_total == 0:
        return np.zeros(len(stat_total))
    return np.where(pct_min > 0, (stat_total / team_total) / pct_min, 0.0)


def _team_bpm(g, pace, net, ortg, lg_ortg, team_games):
    g = g[(g["GP"] > 0) & (g["MIN"] > 0)].copy()
    if g.empty:
        return None
    mp = (g["MIN"] * g["GP"]).to_numpy(dtype=float)  # season-total minutes
    poss = pace * mp / 48.0
    keep = poss > 0
    g, mp, poss = g[keep].copy(), mp[keep], poss[keep]
    if len(g) == 0:
        return None

    tot = {c: (g[c] * g["GP"]).to_numpy(dtype=float) for c in
           ["FGA", "FTA", "FG3M", "AST", "TOV", "OREB", "DREB", "REB", "STL", "BLK", "PF", "PTS"]}
    team_mp = mp.sum()
    pct_min = mp / (team_mp / 5.0)

    tsa = tot["FGA"] + FT * tot["FTA"]
    team_pts_per_tsa = tot["PTS"].sum() / tsa.sum() if tsa.sum() > 0 else 0.0
    pt_per_tsa = np.divide(tot["PTS"], tsa, out=np.zeros_like(tsa, dtype=float), where=tsa > 0)
    adjpt = ((pt_per_tsa - team_pts_per_tsa) + 1.0) * tsa
    thresh_pts = tsa * (pt_per_tsa - (team_pts_per_tsa + PT_THRESHOLD))

    per100 = {
        "AdjPt": adjpt / poss * 100.0, "FGA": tot["FGA"] / poss * 100.0, "FTA": tot["FTA"] / poss * 100.0,
        "FG3": tot["FG3M"] / poss * 100.0, "AST": tot["AST"] / poss * 100.0, "TOV": tot["TOV"] / poss * 100.0,
        "ORB": tot["OREB"] / poss * 100.0, "DRB": tot["DREB"] / poss * 100.0, "TRB": tot["REB"] / poss * 100.0,
        "STL": tot["STL"] / poss * 100.0, "BLK": tot["BLK"] / poss * 100.0, "PF": tot["PF"] / poss * 100.0,
    }

    p_trb = _share(tot["REB"], tot["REB"].sum(), pct_min)
    p_stl = _share(tot["STL"], tot["STL"].sum(), pct_min)
    p_pf = _share(tot["PF"], tot["PF"].sum(), pct_min)
    p_ast = _share(tot["AST"], tot["AST"].sum(), pct_min)
    p_blk = _share(tot["BLK"], tot["BLK"].sum(), pct_min)
    p_thr = _share(thresh_pts, thresh_pts.sum(), pct_min)

    est_pos = _center(POS["INT"] + POS["TRB"] * p_trb + POS["STL"] * p_stl + POS["PF"] * p_pf
                      + POS["AST"] * p_ast + POS["BLK"] * p_blk, mp, 3.0)
    off_role = _center(ROLE["INT"] + ROLE["AST"] * p_ast + ROLE["THRESH"] * p_thr, mp, 4.0)

    def assemble(coefs, posconst1, slope):
        total = np.zeros(len(g))
        for stat, vals in per100.items():
            pp = off_role if stat in ("FGA", "FTA") else est_pos
            total = total + vals * _interp(coefs[stat], pp)
        posc = slope * (off_role - 3.0) + np.where(est_pos < 3.0, (3.0 - est_pos) / 2.0 * posconst1, 0.0)
        return total + posc

    raw_bpm = assemble(BPM_COEF, POS_CONST_1_BPM, OFFROLE_SLOPE_BPM)
    raw_obpm = assemble(OBPM_COEF, POS_CONST_1_OBPM, OFFROLE_SLOPE_OBPM)

    lead_bonus = 0.175 * (net * pace / 100.0 / 2.0)
    adj_tm = net + lead_bonus
    adj_ortg = (ortg - lg_ortg) + lead_bonus / 2.0
    tm_adj = (adj_tm - (raw_bpm * pct_min).sum()) / 5.0
    otm_adj = (adj_ortg - (raw_obpm * pct_min).sum()) / 5.0
    bpm = raw_bpm + tm_adj
    obpm = raw_obpm + otm_adj

    return pd.DataFrame({
        "PLAYER_ID": g["PLAYER_ID"].to_numpy(),
        "SEASON_TYPE": g["SEASON_TYPE"].to_numpy(),
        "OBPM": np.round(obpm, 2),
        "DBPM": np.round(bpm - obpm, 2),
        "BPM": np.round(bpm, 2),
        "VORP": np.round((bpm - REPLACEMENT) * pct_min * (team_games / 82.0), 2),
    })


def compute_bpm_vorp(player_stats: pd.DataFrame, team_stats: pd.DataFrame) -> pd.DataFrame:
    """Return one row per (PLAYER_ID, SEASON_TYPE) with OBPM/DBPM/BPM/VORP."""
    frames = []
    for stype, pdf in player_stats.groupby("SEASON_TYPE"):
        tdf = team_stats[team_stats["SEASON_TYPE"] == stype]
        if tdf.empty:
            continue
        lg_ortg = float(tdf["OFF_RATING"].mean())
        ctx = tdf.set_index("TEAM_ID")
        for tid, g in pdf.groupby("TEAM_ID"):
            if tid not in ctx.index:
                continue
            row = ctx.loc[tid]
            if isinstance(row, pd.DataFrame):  # guard duplicate team rows
                row = row.iloc[0]
            res = _team_bpm(g, float(row["PACE"]), float(row["NET_RATING"]),
                            float(row["OFF_RATING"]), lg_ortg, float(row["GP"]))
            if res is not None:
                frames.append(res)
    if not frames:
        logger.warning("compute_bpm_vorp produced no rows")
        return pd.DataFrame(columns=_RESULT_COLS)
    df = pd.concat(frames, ignore_index=True)[_RESULT_COLS]
    logger.info("✓ BPM/VORP: %d player-rows", len(df))
    return df
