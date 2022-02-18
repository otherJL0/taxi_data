import asyncio
from itertools import product, starmap
from typing import NamedTuple
from os import mkdir

from aiohttp import ClientConnectionError, ClientTimeout, request
from aiomultiprocess import Pool
from tqdm import tqdm

BASE_URL: str = "https://s3.amazonaws.com/nyc-tlc/trip+data"
CHUNK_SIZE = 10 * 1_000_000  # TODO test with different sized chunks


# TODO Better class name?
class MetaData(NamedTuple):
    """Generalized container class to pass data between functions"""

    path: str
    url: str
    size: int


def generate_file_path(taxi: str, year: str, month: str) -> str:
    """Generate target file path"""
    return f"{taxi}_tripdata_{year}-{month}.csv"


def generate_candidate_file_paths() -> list[str]:
    """Generate candidate file paths from NYC TLC Trip record data:
    https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    """
    taxis: list[str] = ["green", "yellow", "fhv", "fhvhv"]
    taxis: list[str] = ["fhv"]  # NOTE remove when downloading all files
    years: list[str] = [str(year) for year in range(2009, 2022)]
    months: list[str] = [f"{month:02}" for month in range(1, 13)]

    return list(starmap(generate_file_path, product(taxis, years, months)))


async def validate_endpoint(file_path: str) -> MetaData | None:
    """Ping file url to validate endpoint"""
    try:
        url = f"{BASE_URL}/{file_path}"
        async with request("GET", url) as response:
            if response.status == 200 and response.content_length is not None:
                return MetaData(file_path, url, response.content_length)
    except ClientConnectionError:
        return None


async def validate_candidate_endpoints() -> list[MetaData]:
    """ """

    candidate_file_paths: list[str] = generate_candidate_file_paths()

    # Asynchronously and concurrenctly ping each candidate url
    valid_results: list[MetaData] = []

    pbar = tqdm(
        total=len(candidate_file_paths),
        position=0,
        desc=f"Validating {len(candidate_file_paths)} endpoints: ",
    )
    async with Pool() as p:
        async for call in p.map(validate_endpoint, candidate_file_paths):
            pbar.update(1)
            if call:
                valid_results.append(call)
    print(f"{len(valid_results)} files found")
    return valid_results


async def download_file(metadata: MetaData) -> int:
    """Given a verified file url, download the target file in chunks"""
    try:
        # HACK increased timeout for large files, is there a better way?
        async with request(
            "GET", metadata.url, timeout=ClientTimeout(total=100 * 60)
        ) as response:

            # TODO consider changing downloaded file path
            with open(f"data/{metadata.path}", "wb") as file_dump, tqdm(
                total=metadata.size,
                desc=metadata.path,
                unit_scale=True,
                # bar_format="{lbar}{r_bar}",
                # position=8,
                leave=True,
            ) as pbar:
                async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                    pbar.update(file_dump.write(chunk))
    except ClientConnectionError:
        pass
    return metadata.size


async def download_files(metadata: list[MetaData]) -> None:
    """Asynchronously and concurrenctly download all files"""
    total_files_pbar = tqdm(total=len(metadata), position=1, desc="File Count")
    total_size_pbar = tqdm(
        total=sum(x.size for x in metadata),
        position=2,
        desc="Downloaded",
        unit_scale=True,
    )

    # PERF experiment with parameters
    async with Pool(
        processes=2, maxtasksperchild=4, queuecount=2, childconcurrency=4
    ) as pool:
        async for downloaded in pool.map(download_file, metadata):
            total_files_pbar.update(1)
            total_size_pbar.update(downloaded)


if __name__ == "__main__":
    try:
        mkdir("data")
    except FileExistsError:
        pass
    valid_url_metadata = asyncio.run(validate_candidate_endpoints())
    asyncio.run(download_files(valid_url_metadata))
