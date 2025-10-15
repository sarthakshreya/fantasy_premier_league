import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional

import requests
import pandas as pd


BASE = "https://fantasy.premierleague.com/api"

ENDPOINTS = {
    "bootstrap": f"{BASE}/bootstrap-static/",
    "fixtures": f"{BASE}/fixtures/",
    # event_live requires a GW number, format set later
    "event_live": f"{BASE}/event/{{gw}}/live/",
    # element_summary requires player id
    "element_summary": f"{BASE}/element-summary/{{id}}/",
}


def fetch_json(url: str) -> dict:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def read_json(path: Path) -> dict:
    with open(path, 'r') as rf:
        data = json.load(rf)
        rf.close()
    return data

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p
    
def define_bootstrap_static_path(input_path: Path) -> Path:
    bootstrap_path = input_path / "bootstrap_static.json"
    return bootstrap_path
    
def define_fixture_path(input_path: Path) -> Path:
    fixture_path = input_path / "fixtures.json"
    return fixture_path