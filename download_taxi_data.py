import asyncio
import itertools as it
from typing import NamedTuple

from aiohttp import ClientConnectionError, ClientSession, ClientTimeout, request
from aiomultiprocess import Pool
from tqdm import tqdm

BASE_URL: str = "https://s3.amazonaws.com/nyc-tlc/trip+data"
CHUNK_SIZE = 10 * 1_000_000  # TODO test with different sized chunks


class MetaData(NamedTuple):
    path: str
    url: str
    size: int


async def check_url(file_path: str) -> MetaData | None:
    """Ping file url to verify exisitance and grab size data"""
    try:
        url = f"{BASE_URL}/{file_path}"
        async with request("GET", url) as resp:
            if resp.status == 200 and resp.content_length is not None:
                return MetaData(file_path, url, resp.content_length)
    except ClientConnectionError:
        return None


async def download_file(metadata: MetaData):
    """Given a verified file url, download the target file in chunks"""
    async with request(
        "GET", metadata.url, timeout=ClientTimeout(total=100 * 60)
    ) as resp:
        # TODO introduce error handling instead of aborting
        if resp.status != 200:
            print(f"{metadata.path} not valid")
            return

        # TODO consider changing downloaded file path
        with open(f"data/{metadata.path}", "wb") as fd:
            with tqdm(total=metadata.size) as pbar:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    pbar.update(fd.write(chunk))
        print(f"{metadata.path} downloaded!")


def generate_file_url(taxi: str, year: str, month: str) -> str:
    """Generate target file url"""
    return f"{taxi}_tripdata_{year}-{month}.csv"


async def download_files(metadata: list[MetaData]) -> None:
    # TODO experiment with pool parameters
    async with Pool(
        processes=2, maxtasksperchild=8, queuecount=2, childconcurrency=4
    ) as pool:
        _ = await pool.map(download_file, metadata)


async def collect_valid_urls() -> list[MetaData]:
    # taxis: list[str] = ["green", "yellow", "fhv", "fhvhv"]
    taxis: list[str] = ["fhv"]
    years: list[str] = [str(year) for year in range(2009, 2022)]
    months: list[str] = [f"{month:02}" for month in range(1, 13)]

    file_urls: list[str] = list(
        it.starmap(generate_file_url, it.product(taxis, years, months))
    )
    result: list[MetaData] = []
    async with Pool() as pool:
        with tqdm(total=len(file_urls)) as pbar:
            async for mapping in pool.map(check_url, file_urls):
                pbar.update(1)
                if mapping:
                    result.append(mapping)
    return result


if __name__ == "__main__":
    valid_url_metadata: list[MetaData] = asyncio.run(collect_valid_urls())
    print(f"{len(valid_url_metadata)} files found")
    asyncio.run(download_files(valid_url_metadata))
