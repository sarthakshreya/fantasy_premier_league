from pathlib import Path

import extract_raw_data as ext
import transform_raw_data as tr
from util import *

cwd = Path.cwd()

print(f"Executing from: {cwd.resolve()}")

data_dir = None
is_remote_job = False  # True for Databricks

try:
    if is_remote_job:
        data_dir = "/databricks/driver/"
        dbutils.fs.mkdirs(data_dir + "fpl_dump/raw")
    else:
        # Local: write under current working directory
        data_dir = ensure_dir(Path(cwd).joinpath("fpl_dump").joinpath("raw"))
except (NameError, AttributeError):
    data_dir = ensure_dir(Path(data_dir) if data_dir else Path(cwd).joinpath("fpl_dump").joinpath("raw"))

output_dir = ext.main(data_dir)
tr.main(output_dir, is_remote_job)