# fantasy_premier_league
Repository to fetch, clean, analyze fantasy football data to come up with personal recommender solution.

## Run locally

1. From the project root, create a virtualenv and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Run the full pipeline and analysis (raw → transform → last3/next3 analysis) in one go:
   ```bash
   ./run_pipeline_and_analysis.sh
   ```
   Or run steps separately:
   - Pipeline only: `python src/fpl_weekly/orchestrate.py`
   - Analysis only: `python src/fpl_weekly/fpl_last3_next3_analysis.py --in ./fpl_dump/transformed --out ./fpl_analysis`

3. Output is written under the current directory:
   - **Raw:** `./fpl_dump/raw/` (JSON from FPL API)
   - **Transformed:** `./fpl_dump/transformed/` (players, teams, events, fixtures CSVs)
   - **Analysis** (when using the script above): `./fpl_analysis/` (team_last3_next3.csv, player shortlists)

4. To run only the last-3 / next-3 analysis on existing transformed data:
   ```bash
   python src/fpl_weekly/fpl_last3_next3_analysis.py --in ./fpl_dump/transformed --out ./fpl_analysis
   ```
   This produces `team_last3_next3.csv`, `player_shortlist_per_team.csv`, and `player_shortlist_topK.csv` in `./fpl_analysis`.
