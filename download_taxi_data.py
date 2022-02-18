import asyncio
import itertools as it
from typing import NamedTuple

import aiohttp
import yarl
from aiomultiprocess import Pool
from tqdm import tqdm

BASE_URL: str = "https://s3.amazonaws.com/nyc-tlc/trip+data"
CHUNK_SIZE = 10 * 1_000_000  # TODO test with different sized chunks


class MetaData(NamedTuple):
    url: yarl.URL
    size: int


async def check_url(file_url: yarl.URL) -> MetaData | None:
    """Ping file url to verify exisitance and grab size data"""
    try:
        async with aiohttp.request("GET", file_url) as resp:
            if resp.status == 200 and resp.content_length is not None:
                return MetaData(file_url, resp.content_length)
    except aiohttp.ClientConnectionError:
        return None


async def download_file(metadata: MetaData):
    """Given a verified file url, download the target file in chunks"""
    async with aiohttp.request(
        "GET", metadata.url, timeout=aiohttp.ClientTimeout(total=100 * 60)
    ) as resp:
        # TODO introduce error handling instead of aborting
        if resp.status != 200:
            print(f"{metadata.url.path} not valid")
            return

        # TODO consider changing downloaded file path
        with open(f"data/{metadata.url.path}", "wb") as fd:
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                _ = fd.write(chunk)
        print(f"{metadata.url.path} downloaded!")


def generate_file_url(taxi: str, year: str, month: str) -> yarl.URL:
    """Generate target file url"""
    return yarl.URL(f"{BASE_URL}/{taxi}_tripdata_{year}-{month}.csv")


async def download_files(metadata: list[MetaData]) -> None:
    async with Pool() as pool:
        _ = await pool.map(download_file, metadata)


async def collect_valid_urls() -> list[MetaData]:
    taxis: list[str] = ["green", "yellow", "fh", "fhvhv"]
    years: list[str] = [str(year) for year in range(2009, 2022)]
    months: list[str] = [f"{month:02}" for month in range(1, 13)]

    file_urls: list[yarl.URL] = list(
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
    for metadata in valid_url_metadata:
        print(f"{metadata.url.path}:\t{metadata.size}")
    print(f"{len(valid_url_metadata)} files found")
    print(f"total size {sum(valid_url_metadata)}")
    # asyncio.run(download_files(valid_files))
