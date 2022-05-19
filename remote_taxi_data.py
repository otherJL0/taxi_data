from multiprocessing import Pool
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.feather as feather
import pyarrow.parquet as pq
from pyarrow import fs

S3 = fs.S3FileSystem(anonymous=True)


def download_parquet_file(pq_file: Path) -> None:
    feather.write_feather(
        pq.read_table(pq_file, filesystem=S3),
        f"feather/{pq_file.name}.feather",
        compression="uncompressed",
    )


if __name__ == "__main__":
    taxi_dataset = ds.dataset("nyc-tlc/trip data/", format="parquet", filesystem=S3)

    pq_files = [Path(pqf) for pqf in taxi_dataset.files if pqf.endswith("parquet")]
    with Pool() as p:
        p.map(download_parquet_file, pq_files)
