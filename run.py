import concurrent.futures
import json
import os
import random
import time
from itertools import islice
from threading import Lock
from typing import Optional, Union

import click
import orb.spinner.utils as orb_utils
import pandas as pd
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
        log.info("Changing viewport size")
        orb_utils.change_viewport_size(driver=driver)

    if random.randint(1, 50) == 10:
        with ip_change_lock:
            log.info("Changing VPN")
            orb.change_ip_address()
            time.sleep(7)

    if random.randint(1, 25) == 8:
        log.info("Refreshing Driver")
        driver.close()
        driver = orb.get_webdriver()

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
        outward_code: str, inward_code: str, endole: Endole
):
    """
    Processes a list of postcode values, extracts data for each unique postcode,
    and stores formatted data in a database.

    Args:
        outward_code (str): The outward part of the postcode.
        inward_code (str): The inward part of the postcode.
        endole (Endole): An instance of the Endole class to interact with the database.
    """

    postcode = f"{outward_code}-{inward_code}".upper()

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
@click.option("--no-outward-codes", type=click.INT, required=False)
def main(database_path: Optional[str] = None, no_outward_codes: Optional[int] = None):

    if not database_path:
        # Set database_path using an environment variable if available; otherwise, use REPO_PATH
        database_path = f"{os.getenv('DATABASE_PATH', REPO_PATH)}/endole"

    endole = Endole(database_path=database_path)
    endole.create_table()

    with open('postcodes.json', 'r') as file:
        postcode_dict = json.load(file)

    postcodes = endole.execute_query(
        f"SELECT DISTINCT POSTCODE FROM {endole.DEFAULT_PATH}").values.flatten()
    postcodes_set = set(postcodes)

    # Determine how many threads you want to use
    num_threads = round(os.cpu_count() * 0.50)

    # Randomly shuffle the keys (outward codes) in the dictionary to get a list
    outward_codes = list(postcode_dict.keys())
    random.shuffle(outward_codes)

    if no_outward_codes:
        outward_codes = outward_codes[:no_outward_codes]
    for outward_code in outward_codes:

        # Check to ensure this postcode hasn't already been extracted
        inward_codes = postcode_dict[outward_code]
        inward_codes = [i for i in inward_codes if f"{outward_code}-{i}".upper() not in postcodes_set]

        log.info(f"There are {len(inward_codes)} inward codes for outward code {outward_code}")
        if len(inward_codes) == 0:
            continue

        chk_size = 25
        lst_it = iter(inward_codes)
        inward_chunks = list(iter(lambda: tuple(islice(lst_it, chk_size)), ()))

        log.info(f"Splitting into {len(inward_chunks)} chunks each roughly of size {len(inward_chunks[0])}")

        for inward_codes_chunked in inward_chunks:

            # Create a ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                # Schedule the process_postcode function to run for each key in the postcode_dict

                futures = {
                    executor.submit(
                        process_postcode, outward_code, inward_code, endole
                    ): f"{outward_code}-{inward_code}" for inward_code in inward_codes_chunked
                }

                for future in concurrent.futures.as_completed(futures):
                    key = futures[future]
                    try:
                        future.result()  # If needed, you can handle results or exceptions here
                    except Exception as exc:
                        log.error(f'{key} generated an exception: {exc}')


if __name__ == "__main__":
    main()
