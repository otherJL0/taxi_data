import asyncio

# import aioitertools.itertools as it
import itertools as it

from aiohttp import ClientTimeout, request
from aiomultiprocess import Pool

BASE_URL: str = "https://s3.amazonaws.com/nyc-tlc/trip+data"
CHUNK_SIZE = 10 * 1_000_000


async def check_url(taxi: str, year: str, month: str) -> dict[str, int] | None:
    file_name = f"{taxi}_tripdata_{year}-{month}.csv"
    async with request("GET", f"{BASE_URL}/{file_name}") as resp:
        if resp.status == 200 and resp.content_length is not None:
            return {file_name: resp.content_length}


async def download_file(file_name: str):
    async with request(
        "GET", f"{BASE_URL}/{file_name}", timeout=ClientTimeout(total=100 * 60)
    ) as resp:
        if resp.status != 200:
            print(f"{file_name} not valid")
            return
        with open(f"data/{file_name}", "wb") as fd:
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                _ = fd.write(chunk)
        print(f"{file_name} downloaded!")


def generate_file_name(taxi: str, year: str, month: str) -> str:
    return f"{taxi}_tripdata_{year}-{month}.csv"


async def download_files(files: list[str]) -> None:
    async with Pool() as pool:
        _ = await pool.map(download_file, files)


async def calculate_total_size() -> dict[str, int]:
    taxis: list[str] = ["green", "yellow", "fh", "fhvhv"]
    years: list[str] = [str(year) for year in range(2009, 2022)]
    months: list[str] = [f"{month:02}" for month in range(1, 13)]

    result: dict[str, int] = {}
    async with Pool() as pool:
        combinations = list(it.product(taxis, years, months))
        async for mapping in pool.starmap(check_url, combinations):
            if mapping:
                result = result | mapping
    return result


if __name__ == "__main__":
    valid_files = asyncio.run(calculate_total_size())
    print(valid_files)
    # asyncio.run(download_files(valid_files))
