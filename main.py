import asyncio
import logging
from io import BytesIO
from typing import Any

from PIL import Image

import gspread
import httpx
from oauth2client.service_account import ServiceAccountCredentials

from utils import time_counter, log_settings

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
AMOUNT_LINKS = 1000

log_settings()


def create_client():
    """
    Create google client
    Authorization to Google sheet
    """
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", SCOPES)
    # authorize the clientsheet
    client = gspread.authorize(creds)
    return client


def records_data(client) -> list:
    """Getting values from a table"""
    # get the instance of the Spreadsheet
    sheet = client.open("Parser_ImageSize")
    # get the first sheet of the Spreadsheet
    sheet_instance = sheet.get_worksheet(0)
    record_data = sheet_instance.col_values(1)[1 : AMOUNT_LINKS + 1]
    print(f"records: {len(record_data)}")
    return record_data


async def get_data(content) -> str:
    """Getting the size of image"""
    image = Image.open(BytesIO(content))
    width, height = image.size
    return f"{width}x{height}"


async def get_img_data(link, client) -> list:
    try:
        response = await client.get(link, timeout=5)
        if response.status_code == 200:
            property_data = await get_data(response.content)
            return [link, property_data]
        return [link, "Error"]
    except httpx.ConnectTimeout:
        logging.error("Error: ConnectTimeout")
    except httpx.PoolTimeout:
        logging.error("Error: PoolTimeout")
    except Exception:
        logging.error("Error")


async def create_async_task(links: list) -> Any:
    async with httpx.AsyncClient(headers={"user-agent": USER_AGENT}) as client:
        tasks = [get_img_data(link, client) for link in links]
        results = await asyncio.gather(*tasks)
    return results


def save_data_to_google(client, value) -> None:
    sheet2 = client.open("test")
    sheet_instance2 = sheet2.get_worksheet(0)
    sheet_instance2.update(range_name="A2", values=value)
    sheet_instance2.format(
        "B", {"backgroundColor": {"red": 1.0, "green": 0.6, "blue": 0.0}}
    )


@time_counter
def main() -> None:
    client_google = create_client()
    logging.info("Got google client")

    records = records_data(client_google)
    logging.info("Got records from google sheet")

    list_link_size = []
    for i in range(0, len(records), 200):
        list_link_size += asyncio.run(create_async_task(links=records[i : i + 200]))

    logging.info("Saving result...")
    save_data_to_google(client_google, list_link_size)
    logging.info("Finish")


if __name__ == "__main__":
    main()
