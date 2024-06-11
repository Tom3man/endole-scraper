import concurrent.futures
import json
import os
import random
from itertools import islice
from threading import Lock
from typing import Optional, Union

import click
import orb.spinner.utils as orb_utils
import pandas as pd
from orb.common.vpn import PiaVpn
from orb.spinner.core.driver import OrbDriver
from selenium.webdriver.chrome.webdriver import WebDriver

from endole_scraper import REPO_PATH, log
from endole_scraper.common.extract_data import (extract_all_data,
                                                get_company_count)
from endole_scraper.common.format_data import DataFrameFormatter
from endole_scraper.tables import Endole

BASE_URL = 'https://suite.endole.co.uk/explorer/postcode'

ip_change_lock = Lock()


def manage_browser_settings(driver: WebDriver, orb: OrbDriver) -> WebDriver:
    """
    Randomly changes browser settings to mimic human behavior and avoid detection.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
        orb (OrbDriver): Custom driver class instance used to manage settings.
    """
    if random.randint(1, 3) == 2:
        orb_utils.change_viewport_size(driver=driver)

    if random.randint(1, 40) == 2:
        with ip_change_lock:
            PiaVpn().rotate_vpn()
            driver = orb.refresh_driver()

    if random.randint(1, 20) == 2:
        driver = orb.refresh_driver()

    return driver


def extract_data_for_postcode(postcode: str) -> Union[pd.DataFrame, None]:
    """
    Attempts to extract data for a given postcode using a WebDriver.

    Args:
        postcode (str): The postcode to extract data for.

    Returns:
        pd.DataFrame: Extracted data formatted as a DataFrame, or None if extraction fails.
    """

    url = f"{BASE_URL}/{postcode}".lower()
    company_count = get_company_count(url=url)

    if company_count == 0:
        log.info(f"No companies were found at {postcode}, moving on.")
        return None

    orb = OrbDriver()
    driver = orb.get_webdriver(url=url)

    driver = manage_browser_settings(driver=driver, orb=orb)

    try:
        df = extract_all_data(driver=driver, company_count=company_count)

        df['POSTCODE'] = postcode
        formatter = DataFrameFormatter(dataframe=df)
        df = formatter.format_dataframe()

    except Exception as e:
        log.error(f"Error extracting {postcode}: {e}")
        df = None

    driver.quit()
    return df


def process_postcode(
    postcode: str, endole: Endole
):
    """
    Processes a list of postcode values, extracts data for each unique postcode,
    and stores formatted data in a database.

    Args:
        outward_code (str): The outward part of the postcode.
        inward_code (str): The inward part of the postcode.
        endole (Endole): An instance of the Endole class to interact with the database.
    """

    postcode = postcode.upper()

    log.info(f"Extracting data for postcode {postcode}.")

    df = extract_data_for_postcode(postcode=postcode)
    if df is not None:

        log.info(f"Postcode {postcode} extracted with {len(df)} rows.")
        endole.ingest_dataframe(df=df)

    return


@click.command(
    help="Endole webscraper"
)
@click.option("--database-path", type=click.STRING, required=False)
def main(database_path: Optional[str] = None):

    if not database_path:
        # Set database_path using an environment variable if available; otherwise, use REPO_PATH
        database_path = f"{os.getenv('DATABASE_PATH', REPO_PATH)}/endole"

    endole = Endole(database_path=database_path)
    endole.create_table()

    with open('postcodes.json', 'r') as file:
        postcode_dict = json.load(file)

    postcode_list = []
    for key in postcode_dict.keys():
        for value in postcode_dict[key]:
            postcode_list.append(f'{key}-{value}')

    postcodes = endole.execute_query(
        f"SELECT DISTINCT POSTCODE FROM {endole.DEFAULT_PATH}").values.flatten()
    postcodes_set = set(postcodes)

    to_scrape = [i for i in postcode_list if i.upper() not in postcodes_set]

    max_workers = 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_postcode, postcode, endole)
            for postcode in to_scrape
        ]

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
