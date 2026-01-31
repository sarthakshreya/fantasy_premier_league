import json
from pathlib import Path
from typing import List, Optional

import requests
import pandas as pd
from util import *

def main(out: Optional[str] = None) -> None:
    out_dir = ensure_dir(Path(out) if out else Path("./fpl_dump/raw"))

    print(f"Saving to: {out_dir.resolve()}")

    print("Fetching bootstrap-static ...")
    bootstrap = fetch_json(ENDPOINTS["bootstrap"])
    write_json(define_bootstrap_static_path(out_dir), bootstrap)
    #to_csv_players(bootstrap, out_dir)
    
    print("Fetching fixtures ...")
    fixtures = fetch_json(ENDPOINTS["fixtures"])
    write_json(define_fixture_path(out_dir), fixtures)
    #to_csv_fixtures(fixtures, out_dir)

    print("Done!")
    
    return out_dir
	
#main()