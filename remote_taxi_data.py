from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.feather as feather
import pyarrow.parquet as pq
from pyarrow import fs
from rich.progress import track

s3 = fs.S3FileSystem(anonymous=True)
taxi_dataset = ds.dataset("nyc-tlc/trip data/", format="parquet", filesystem=s3)

pq_files = [pqf for pqf in taxi_dataset.files if pqf.endswith("parquet")]
for pq_file in track(pq_files, description="Reading parquet files..."):

    table = pq.read_table(pq_file, filesystem=s3)
    feather.write_feather(
        table, f"feather/{Path(pq_file).name}.feather", compression="uncompressed"
    )
