import asyncio

# import aioitertools.itertools as it
import itertools as it
import sys

from aiohttp import ClientTimeout, request
from aiomultiprocess import Pool

BASE_URL: str = "https://s3.amazonaws.com/nyc-tlc/trip+data"
CHUNK_SIZE = 10 * 1_000_000


async def check_url(file_name: str) -> str:
    async with request("GET", f"{BASE_URL}/{file_name}") as resp:
        if resp.status == 200 and resp.content_length is not None:
            return file_name
        return ""


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


async def main(files: list[str]) -> None:
    async with Pool() as pool:
        _ = await pool.map(download_file, files)


async def calculate_total_size(taxi: str, year: int) -> list[str]:
    taxis: list[str] = [taxi]
    years: list[str] = [str(year) for year in range(year, year + 1)]
    months: list[str] = [f"{month:02}" for month in range(1, 13)]

    files = it.starmap(generate_file_name, it.product(taxis, years, months))
    async with Pool() as pool:
        files = await pool.map(check_url, files)
        return [x for x in files if x]


if __name__ == "__main__":
    taxi = sys.argv[1]
    year = int(sys.argv[2])
    valid_files = asyncio.run(calculate_total_size(taxi, year))
    asyncio.run(main(valid_files))
