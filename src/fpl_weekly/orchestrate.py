from pathlib import Path
from datetime import datetime, timezone

import extract_raw_data as ext
import transform_raw_data as tr
from util import *

cwd = Path.cwd()

print(f"Executing from: {cwd.resolve()}")

data_dir = None
ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
is_remote_job = True

try:
    if is_remote_job:
        path = "dbfs:/mnt/mydata"
    else:
        data_dir = cwd.parent.parent.parent.parent
    data_dir = data_dir.joinpath(f"fpl_dump_{ts}").joinpath("raw")
except:
    data_dir = ensure_dir(Path(data_dir) if data_dir else Path(f"./fpl_dump_{ts}/raw"))

output_dir = ext.main(data_dir)
tr.main(output_dir, is_remote_job)