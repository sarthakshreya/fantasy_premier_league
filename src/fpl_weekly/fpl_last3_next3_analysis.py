#!/usr/bin/env python3
"""
fpl_last3_next3_analysis.py  — Last-3 / Next-3 team form and player shortlists
--------------------------------------------------------------------------------
Reads the **transformed** output from the FPL pipeline (orchestrate → transform)
and produces:
1) team_last3_next3.csv  — LAST3 form + NEXT3 fixture difficulty (with data_timestamp)
2) player_shortlist_per_team.csv — uniform player columns for top-N teams, M players per team
3) player_shortlist_topK.csv — uniform player columns for global top-K players, sorted by position

Expects --in to point at the **transformed** folder (e.g. ./fpl_dump/transformed)
containing: players.csv, teams.csv, fixtures.csv (or fixtures.json for legacy).

Usage:
  python fpl_last3_next3_analysis.py --in ./fpl_dump/transformed --out ./fpl_analysis \
    --top-teams 6 --per-team 3 --top-players 40 --diff-threshold 10 --temp-threshold 50
"""

import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

POS_MAP = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}

# Columns to keep in team_last3_next3.csv (reduced set)
TEAM_TABLE_COLS = [
    "team_id", "team", "blend_score_z", "form_score_z", "fixture_score_z",
    "last3_points", "upcoming_next3_count", "next3_avg_difficulty",
    "next3_opponents", "next3_home_away", "data_timestamp",
    "form_score", "fixture_score", "blend_score",
]

def load_inputs(in_dir: Path):
    """Load from transformed folder: players.csv, teams.csv, and fixtures (CSV or JSON)."""
    in_dir = Path(in_dir)
    teams = pd.read_csv(in_dir / "teams.csv")
    players = pd.read_csv(in_dir / "players.csv")

    fixtures_path_csv = in_dir / "fixtures.csv"
    fixtures_path_json = in_dir / "fixtures.json"
    if fixtures_path_csv.exists():
        fx_df = pd.read_csv(fixtures_path_csv)
        if "kickoff_time" in fx_df.columns:
            fx_df["kickoff_time"] = pd.to_datetime(fx_df["kickoff_time"], errors="coerce")
        fixtures = fx_df.to_dict(orient="records")
    elif fixtures_path_json.exists():
        fixtures = json.loads(fixtures_path_json.read_text(encoding="utf-8"))
    else:
        raise FileNotFoundError(
            f"Need fixtures.csv or fixtures.json in {in_dir.resolve()}. "
            "Point --in at the **transformed** folder (e.g. ./fpl_dump/transformed)."
        )
    return fixtures, teams, players

def compute_team_form(fixtures, teams_df, data_ts_str):
    fx = pd.DataFrame(fixtures)
    if fx.empty:
        raise SystemExit("Fixtures data appears empty.")
    # time & finished
    if "kickoff_time" in fx.columns:
        fx["kickoff_time"] = pd.to_datetime(fx["kickoff_time"], errors="coerce")
    finished = fx["finished"].fillna(False).astype(bool)
    played_df = fx[finished].copy()
    upcoming_df = fx[~finished].copy()

    team_map = dict(zip(teams_df["id"], teams_df["name"]))
    rows = []
    for tid in teams_df["id"].tolist():
        # last 3
        p = played_df[(played_df["team_h"]==tid) | (played_df["team_a"]==tid)].copy()
        p = p.sort_values("kickoff_time").tail(3)
        pts = 0; gf=0; ga=0; cs=0
        for _,r in p.iterrows():
            is_home = r["team_h"]==tid
            gf_r = r["team_h_score"] if is_home else r["team_a_score"]
            ga_r = r["team_a_score"] if is_home else r["team_h_score"]
            gf += (gf_r or 0); ga += (ga_r or 0)
            if (ga_r or 0)==0: cs += 1
            if (gf_r or 0) > (ga_r or 0): pts += 3
            elif (gf_r or 0) == (ga_r or 0): pts += 1
        gd = gf - ga
        n_played = len(p) if len(p)>0 else 1  # avoid div by zero
        avg_gf = gf / n_played
        avg_ga = ga / n_played
        cs_pct = cs / n_played

        # next 3
        u = upcoming_df[(upcoming_df["team_h"]==tid) | (upcoming_df["team_a"]==tid)].copy()
        u = u.sort_values("kickoff_time").head(3)
        diffs = []
        home_away = []
        opp_names = []
        for _,r in u.iterrows():
            if r["team_h"]==tid:
                diffs.append(r.get("team_h_difficulty", None))
                home_away.append("H")
                opp_names.append(team_map.get(r["team_a"], r["team_a"]))
            else:
                diffs.append(r.get("team_a_difficulty", None))
                home_away.append("A")
                opp_names.append(team_map.get(r["team_h"], r["team_h"]))
        diffs_series = pd.Series(diffs, dtype="float")
        avg_diff = diffs_series.mean() if diffs_series.notna().sum() > 0 else None

        rows.append({
            "team_id": tid,
            "team": team_map.get(tid, tid),
            "played_last3_count": int(len(p)),
            "last3_points": pts,
            "last3_goals_for": gf,
            "last3_goals_against": ga,
            "last3_goal_diff": gd,
            "last3_avg_goals_for": round(avg_gf,2),
            "last3_avg_goals_against": round(avg_ga,2),
            "last3_clean_sheets": cs,
            "last3_clean_sheet_pct": round(cs_pct,2),
            "upcoming_next3_count": int(len(u)),
            "next3_avg_difficulty": round(avg_diff, 2) if avg_diff is not None else None,
            "next3_opponents": ", ".join(opp_names),
            "next3_home_away": "".join(home_away),
            "data_timestamp": data_ts_str,
        })
    df = pd.DataFrame(rows)
    # scores
    df["form_score"] = df["last3_points"] + 0.5*df["last3_goal_diff"]
    # lower difficulty better: (6 - avg_diff). If NaN, treat as 0
    df["fixture_score"] = df["next3_avg_difficulty"].rsub(6)
    df["fixture_score"] = df["fixture_score"].fillna(0)
    # Compute z-score for form_score
    df["form_score_z"] = pd.to_numeric(((df["form_score"] - df["form_score"].mean()) / df["form_score"].std()).round(2), downcast='float')
    df["fixture_score_z"] = pd.to_numeric(((df["fixture_score"] - df["fixture_score"].mean()) / df["fixture_score"].std()).round(2), downcast='float')
    df["blend_score"] = df["form_score"] + df["fixture_score"]
    df["blend_score_z"] = df["form_score_z"] + df["fixture_score_z"]
    return df.sort_values(["blend_score_z","blend_score"], ascending=False)

def enrich_players(players_df, teams_df, diff_threshold, temp_threshold):
    # Map team names
    team_map = dict(zip(teams_df["id"], teams_df["name"]))
    short_map = teams_df.set_index("id")["short_name"].to_dict() if "short_name" in teams_df.columns else {}
    df = players_df.copy()
    df["team"] = df["team"].map(team_map)
    if short_map:
        df["team_short"] = players_df["team"].map(short_map)
    else:
        # fallback: first 3 letters of team
        df["team_short"] = df["team"].fillna("").str.replace(r"[^A-Za-z]", "", regex=True).str[:3].str.upper()

    df["position"] = df["element_type"].map(POS_MAP)
    df["now_cost_m"] = df["now_cost"] / 10.0
    
    # numeric casts
    for c in ["minutes","points_per_game","form","selected_by_percent","event_points",
              "cost_change_event","cost_change_start"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # ownership label
    def own_label(pct):
        if pd.isna(pct): return "unknown"
        if pct < diff_threshold: return "differential"
        if pct > temp_threshold: return "template"
        return "mid-owned"
    df["ownership_label"] = df["selected_by_percent"].apply(own_label)

    # handy columns
    df["last_gw_points"] = df.get("event_points", pd.Series([None]*len(df)))
    df["price_change_gw"] = df.get("cost_change_event", pd.Series([None]*len(df))) / 10.0
    df["price_change_season"] = df.get("cost_change_start", pd.Series([None]*len(df))) / 10.0

    # Availability label (simple)
    def availability_label(status, c_this, c_next):
        # FPL statuses: a=available, d=doubtful, i=injured, s=suspended, u=unavailable, n=NA
        if status == "a": return "available"
        if status == "d": return f"doubtful ({c_next or c_this or ''}%)"
        if status == "s": return "suspended"
        if status == "i": return "injured"
        if status in ("u","n"): return "unavailable"
        return "unknown"

    df["availability"] = df.apply(
        lambda r: availability_label(r.get("status"), r.get("chance_of_playing_this_round"), r.get("chance_of_playing_next_round")),
        axis=1
    )
    # keep only active/available-ish players for shortlists
    base = df[(df["minutes"] >= 0) & (df["status"].isin(["a","d"]))].copy()
    
    return base

def select_uniform_columns(df, data_ts_str):
    cols = [
        "player_id", "web_name", "team", "team_short", "position",
        "blend_score_z", "form_score_z", "fixture_score_z",
        "now_cost_m", "selected_by_percent", "form", "points_per_game",
        "last_gw_points", "price_change_gw", "price_change_season", "ownership_label",
        "availability", "shortlist_rank", "is_best_pick", "data_timestamp",
    ]
    df = df.copy()
    df["data_timestamp"] = data_ts_str
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]

def shortlist_per_team(players_df, team_rank_df, top_n_teams, per_team, data_ts_str):
    keep_teams = team_rank_df.head(top_n_teams)["team"].tolist()
    cand = players_df[players_df["team"].isin(keep_teams)].copy()

    # Simple value/order metric
    cand["ppg"] = pd.to_numeric(cand["points_per_game"], errors="coerce").fillna(0)
    cand["frm"] = pd.to_numeric(cand["form"], errors="coerce").fillna(0)
    # Prefer likely starters: sort by PPG, form, cheaper first to surface value
    cand = cand.sort_values(["team","position","ppg","frm","now_cost_m"],
                             ascending=[True, True, False, False, True])
    cand = cand[cand['form']>0]
    cand = cand[cand['points_per_game']>1]
    picks = []
    for team in keep_teams:
        
        sub = cand[cand["team"]==team]
        
        # Prefer 1 DEF + 2 attackers
        def_sub = sub[sub["position"]=="DEF"].head(5)
        mid_sub = sub[sub["position"]=="MID"].head(5)
        atk_sub = sub[sub["position"]=="FWD"].head(5)
        pick = pd.concat([def_sub, mid_sub, atk_sub]).sort_values("points_per_game", ascending = False)
        pick = pick.head(per_team)
        if pick.empty and not sub.empty:
            pick = sub.head(per_team)
        pick = pick.copy()
        pick["is_best_pick"] = True
        picks.append(pick)

    res = pd.concat(picks) if picks else pd.DataFrame(columns=cand.columns)
    # rank per team block for readability
    res["shortlist_rank"] = res.groupby("team").cumcount() + 1
    # add team score columns (blend_score_z, form_score_z, fixture_score_z) from team_rank_df
    team_scores = team_rank_df[["team", "blend_score_z", "form_score_z", "fixture_score_z"]].drop_duplicates("team")
    res = res.merge(team_scores, on="team", how="left")
    res = select_uniform_columns(res, data_ts_str)
    return res

def shortlist_topK(players_df, team_rank_df, top_k, data_ts_str):
    # Score = team_blend + player PPG & form; downweight long injuries/suspensions ('i','s')
    teams_scores = team_rank_df.set_index("team")["blend_score"]
    df = players_df.copy()
    df["team_blend"] = df["team"].map(teams_scores)
    df["ppg"] = pd.to_numeric(df["points_per_game"], errors="coerce").fillna(0)
    df["frm"] = pd.to_numeric(df["form"], errors="coerce").fillna(0)
    df["comp_score"] = df["ppg"]*1.2 + df["frm"]*0.8 + df["team_blend"].fillna(0)*0.3
    df.loc[df["status"].isin(["i","s"]), "comp_score"] *= 0.5
    base = df[(df["minutes"] >= 0) & (df["status"].isin(["a","d","n"]))].copy()
    ranked = base.sort_values(["comp_score"], ascending=[False]).copy()
    ranked["is_best_pick"] = False
    # global rank within position groups
    ranked["shortlist_rank"] = ranked.groupby("position").cumcount() + 1
    ranked = ranked.head(top_k)
    # add team score columns (blend_score_z, form_score_z, fixture_score_z) from team_rank_df
    team_scores = team_rank_df[["team", "blend_score_z", "form_score_z", "fixture_score_z"]].drop_duplicates("team")
    ranked = ranked.merge(team_scores, on="team", how="left")
    ranked = select_uniform_columns(ranked, data_ts_str)
    # Re-order for readability: GK, DEF, MID, FWD then shortlist_rank
    pos_order = {"GK":0,"DEF":1,"MID":2,"FWD":3}
    ranked["pos_order"] = ranked["position"].map(pos_order)
    ranked = ranked.sort_values(["pos_order","shortlist_rank"]).drop(columns=["pos_order"])
    return ranked

def main(in_dir: str, out_dir: str, top_n_teams: int, per_team: int, top_k_players: int,
         diff_threshold: float, temp_threshold: float):
    in_p = Path(in_dir)
    out_p = Path(out_dir); out_p.mkdir(parents=True, exist_ok=True)
    fixtures, teams, players = load_inputs(in_p)

    data_ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

    team_table = compute_team_form(fixtures, teams, data_ts_str)
    players_en = enrich_players(players, teams, diff_threshold, temp_threshold)

    # Write team table (reduced columns only)
    team_path = out_p / "team_last3_next3.csv"
    team_table[TEAM_TABLE_COLS].to_csv(team_path, index=False)

    # Shortlists
    per_team_df = shortlist_per_team(players_en, team_table, top_n_teams, per_team, data_ts_str)
    per_team_path = out_p / "player_shortlist_per_team.csv"
    per_team_df.to_csv(per_team_path, index=False)

    topk_df = shortlist_topK(players_en, team_table, top_k_players, data_ts_str)
    topk_path = out_p / "player_shortlist_topK.csv"
    topk_df.to_csv(topk_path, index=False)

    print("Saved:")
    print(f"- {team_path}")
    print(f"- {per_team_path}")
    print(f"- {topk_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_dir", required=True,
                    help="Transformed folder (e.g. ./fpl_dump/transformed) with players.csv, teams.csv, fixtures.csv")
    ap.add_argument("--out", dest="out_dir", default="./fpl_analysis", help="Output folder")
    ap.add_argument("--top-teams", dest="top_teams", type=int, default=10, help="Number of top teams to target")
    ap.add_argument("--per-team", dest="per_team", type=int, default=5, help="Players per team for shortlist")
    ap.add_argument("--top-players", dest="top_players", type=int, default=40, help="Global top-K player shortlist")
    ap.add_argument("--diff-threshold", dest="diff_th", type=float, default=10.0, help="Differential ownership threshold (%)")
    ap.add_argument("--temp-threshold", dest="temp_th", type=float, default=50.0, help="Template ownership threshold (%)")
    args = ap.parse_args()
    # FIX: pass positional args without the mistaken keyword
    main(args.in_dir, args.out_dir, args.top_teams, args.per_team, args.top_players, args.diff_th, args.temp_th)
