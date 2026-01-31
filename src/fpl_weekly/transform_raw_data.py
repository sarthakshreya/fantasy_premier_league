import json
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional

import pandas as pd
from util import *

COLS_PLAYERS = [
    "id", "first_name", "second_name", "web_name", "team", "element_type",
    "now_cost", "selected_by_percent", "status", "form", "points_per_game",
    "minutes", "goals_scored", "assists", "clean_sheets", "goals_conceded",
    "yellow_cards", "red_cards", "expected_goals", "expected_assists", "expected_goal_involvements",
    "news", "news_added",
    "chance_of_playing_this_round", "chance_of_playing_next_round",
    "event_points", "cost_change_event", "cost_change_start",
]

COLS_FIXTURES =  [
    "code", "event", "finished", "finished_provisional", "id",
   "kickoff_time", "minutes", "provisional_start_time", "started",
   "team_a", "team_a_score", "team_h", "team_h_score",
   "team_h_difficulty", "team_a_difficulty", "pulse_id"
]

def spark_session():
    from pyspark.sql import SparkSession

    # Only create SparkSession if it doesn't already exist (like in Databricks)
    try:
        spark  # check if spark already exists (Databricks provides it)
    except NameError:
        spark = SparkSession.builder.appName("FPL Analysis").getOrCreate()

    return spark

def to_csv_players(in_dir: Path, out_dir: Path) -> None:
    
    bootstrap_file = define_bootstrap_static_path(in_dir)
    bootstrap = read_json(bootstrap_file)
    
    elements = pd.DataFrame(bootstrap.get("elements", []))
    teams = pd.DataFrame(bootstrap.get("teams", []))
    events = pd.DataFrame(bootstrap.get("events", []))

    # Helpful subsets
    players = elements.loc[:, [c for c in COLS_PLAYERS if c in elements.columns]].copy()

    # Map team id -> name
    if not teams.empty:
        team_map = dict(zip(teams["id"], teams["name"]))
        players["team_name"] = players["team"].map(team_map)

    # Nice derived columns
    players["now_cost_m"] = players["now_cost"] / 10.0  # FPL stores cost as 10x
    players.rename(columns={"id": "player_id"}, inplace=True)

    # Write CSVs
    players.to_csv(out_dir / "players.csv", index=False)
    teams.to_csv(out_dir / "teams.csv", index=False)
    events.to_csv(out_dir / "events.csv", index=False)
    
def to_table_players(in_dir: Path) -> None:
    from pyspark.sql import functions as F

    spark = spark_session()

    bootstrap_file = define_bootstrap_static_path(in_dir)
    bootstrap_df = spark.read.json(str(bootstrap_file))

    ts = F.current_timestamp()
    
    teams_df = bootstrap_df.select(F.explode("teams").alias("t")).withColumn("ingest_ts", ts)

    players_df = bootstrap_df.select(F.explode("elements").alias("e"))
    players_df = players_df.filter(F.col("_corrupt_record").isNull())
    players_df = players_df.withColumn("e.now_cost_m", F.col("e.now_cost") / 10.0) # FPL stores cost as 10x
    players_df = players_df.join(teams_df.select("t.team_id", "t.team_name"), left_on = "e.team", right_on = "t.team_id", how = "left")
    players_df = players_df.withColumn("ingest_ts", ts)
    
    events_df = bootstrap_df.select(F.explode("events").alias("ev")).withColumn("ingest_ts", ts)
    
    spark.sql("CREATE SCHEMA IF NOT EXISTS fpl")
    
    players_df.write.mode("append").format("delta").saveAsTable("fpl.players")
    teams_df.write.mode("append").format("delta").saveAsTable("fpl.teams")
    events_df.write.mode("append").format("delta").saveAsTable("fpl.events")
    
def to_csv_fixtures(in_dir: Path, out_dir: Path) -> None:
    
    fixture_file = define_fixture_path(in_dir)
    fixtures_json = read_json(fixture_file)
    
    fixtures_raw = pd.DataFrame(fixtures_json)
    
    # Helpful subsets

    fixtures = fixtures_raw.loc[:, [c for c in COLS_FIXTURES if c in fixtures_raw.columns]].copy()
    if not fixtures.empty:
        # Parse kickoff_time if present
        if "kickoff_time" in fixtures.columns:
            fixtures["kickoff_time"] = pd.to_datetime(fixtures["kickoff_time"], errors="coerce")
    fixtures.to_csv(out_dir / "fixtures.csv", index=False)
    
def to_table_fixtures(in_dir: Path) -> None:
    from pyspark.sql import functions as F

    spark = spark_session()

    fixture_file = define_fixture_path(in_dir)

    fixture_df = spark.read.json(str(fixture_file))
    fixture_df = fixture_df.filter(F.col("_corrupt_record").isNull())
    fixture_df = fixture_df.select(COLS_FIXTURES)
    fixture_df = fixture_df.withColumn("ingest_ts", F.current_timestamp())
    
    spark.sql("CREATE SCHEMA IF NOT EXISTS fpl")
    
    fixture_df.write.mode("append").format("delta").saveAsTable("fpl.fixtures")
    
    
def transformed_data_to_csv(input_dir: Path) -> None:
    
    output_dir = ensure_dir(input_dir.parent.joinpath("transformed"))

    print(f"Reading from: {input_dir.resolve()}")

    print("Fetching players, teams, events ...")
    to_csv_players(input_dir, output_dir)
    print("Fetching fixtures ...")
    to_csv_fixtures(input_dir, output_dir)
    
    print(f"Saved to: {output_dir.resolve()}")

def transformed_data_to_table(input_dir: Path) -> None:
    
    output_dir = ensure_dir(input_dir.parent.joinpath("transformed"))

    print(f"Reading from: {input_dir.resolve()}")

    print("Fetching players, teams, events ...")
    to_table_players(input_dir)
    print("Fetching fixtures ...")
    to_table_fixtures(input_dir)
    
    print(f"Saved to: {output_dir.resolve()}")

def main(raw_dir: str, is_spark_job: Optional[bool] = False) -> None:
    input_dir = Path(raw_dir)
    
    if not is_spark_job:
        transformed_data_to_csv(input_dir)
    else:
        transformed_data_to_table(input_dir)
    
    print("Done!")
	
#main()