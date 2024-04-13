import asyncio
import time
from io import BytesIO

from PIL import Image

import gspread
import httpx
from oauth2client.service_account import ServiceAccountCredentials

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

AMOUNT_LINKS = 500

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
    record_data = sheet_instance.col_values(1)[1:AMOUNT_LINKS + 1]
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
    except httpx.ConnectTimeout as error:
        print(error)
    except httpx.PoolTimeout as error:
        print(error)


async def create_async_task(links: list) -> list:
    async with httpx.AsyncClient(headers={"user-agent": USER_AGENT}) as client:
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(get_img_data(link, client)) for link in links]
    return [task.result() for task in tasks]


def save_data_to_google(client, value) -> None:
    sheet2 = client.open("test")
    sheet_instance2 = sheet2.get_worksheet(0)
    sheet_instance2.update(range_name="A2", values=value)
    sheet_instance2.format("B", {"backgroundColor": {"red": 1.0, "green": 0.6, "blue": 0.0}})


def main() -> None:
    client_google = create_client()
    records = records_data(client_google)
    list_link_size = []
    for i in range(0, len(records), 200):
        list_link_size += asyncio.run(create_async_task(links=records[i:i + 200]))
        print(len(list_link_size))

    save_data_to_google(client_google, list_link_size)


if __name__ == "__main__":
    print("start")
    start = time.perf_counter()

    main()

    duration = time.perf_counter() - start
    print(f"Duration: {duration}")
