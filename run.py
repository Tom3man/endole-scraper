import concurrent.futures
import json
import logging
import os
import random
from threading import Lock
from typing import List, Optional, Union

import click
import orb.spinner.utils as orb_utils
import pandas as pd
from orb.spinner.core.driver import OrbDriver
from selenium.webdriver.chrome.webdriver import WebDriver
from tqdm import tqdm

from endole_scraper import REPO_PATH
from endole_scraper.common.extract_data import extract_all_data
from endole_scraper.common.format_data import DataFrameFormatter
from endole_scraper.tables import Endole

log = logging.getLogger(__name__)


BASE_URL = 'https://suite.endole.co.uk/explorer/postcode'

ip_change_lock = Lock()


def manage_browser_settings(driver: WebDriver, orb: OrbDriver):
    """
    Randomly changes browser settings to mimic human behavior and avoid detection.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
        orb (OrbDriver): Custom driver class instance used to manage settings.
    """
    if random.randint(1, 3) == 2:
        orb_utils.change_viewport_size(driver=driver)

    if random.randint(1, 10) == 5:
        with ip_change_lock:
            orb.change_ip_address()

    if random.randint(1, 10) == 5:
        driver.close()
        orb.set_user_agent()
        driver = orb.get_webdriver()


def extract_data_for_postcode(
        driver: WebDriver, postcode: str) -> Union[pd.DataFrame, None]:
    """
    Attempts to extract data for a given postcode using a WebDriver.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
        postcode (str): The postcode to extract data for.

    Returns:
        pd.DataFrame: Extracted data formatted as a DataFrame, or None if extraction fails.
    """
    url = f"{BASE_URL}/{postcode}".lower()
    driver.get(url=url)

    try:
        return extract_all_data(driver=driver)
    except Exception as e:
        log.error(f"Error extracting {postcode}: {e}")
        return None


def process_postcode(outward_code: str, inward_codes: List[str], database_path: str):
    """
    Processes a list of postcode values, extracts data for each unique postcode,
    and stores formatted data in a database.

    This function initializes necessary drivers, checks existing data to avoid duplication,
    and manages dynamic interactions with web pages to extract data.

    Args:
        key (str): The key prefix to append to each value in the postcode list.
        values (list): A list of postcode suffixes to process.
    """
    orb = OrbDriver()
    orb.set_user_agent()

    endole = Endole(database_path=database_path)
    endole.create_table()

    postcodes = endole.execute_query(
        f"SELECT DISTINCT POSTCODE FROM {endole.DEFAULT_PATH}").values

    driver = orb.get_webdriver()

    for inward_code in tqdm(
        inward_codes,
        desc=f"Processing {len(inward_codes)} postcodes for outward code {outward_code}"
    ):
        postcode = f"{outward_code}-{inward_code}".upper()
        if postcode in postcodes:
            log.info(f"Data for postcode {postcode} already exists! Skipping.")
            continue

        log.info(f"Extracting data for postcode {postcode}.")
        manage_browser_settings(driver=driver, orb=orb)

        df = extract_data_for_postcode(driver=driver, postcode=postcode)
        if df is not None:
            df['POSTCODE'] = postcode

            formatter = DataFrameFormatter(dataframe=df)
            df_format = formatter.format_dataframe()

            endole.ingest_dataframe(df=df_format)


@click.command(
    help="Endole webscraper"
)
@click.option("--database-path", type=click.STRING, required=False)
def main(database_path: Optional[str] = None):

    if not database_path:
        # Set database_path using an environment variable if available; otherwise, use REPO_PATH
        database_path = os.getenv('DATABASE_PATH', REPO_PATH)

    with open('postcodes.json', 'r') as file:
        postcode_dict = json.load(file)

    # Determine how many threads you want to use
    num_threads = 5

    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Schedule the process_postcode function to run for each key in the postcode_dict
        futures = {executor.submit(
            process_postcode, key, postcode_dict[key], database_path
        ): key for key in postcode_dict.keys()}

        # Optionally, retrieve and log results as tasks complete (useful for error handling)
        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            try:
                future.result()  # If needed, you can handle results or exceptions here
            except Exception as exc:
                log.error(f'{key} generated an exception: {exc}')


if __name__ == "__main__":
    main()
