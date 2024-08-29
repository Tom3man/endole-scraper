import json
import os
from typing import Optional, Union

import click
import pandas as pd
from orb.spinner.core.driver import OrbDriver

from endole_scraper import REPO_PATH, log
from endole_scraper.common.extract_data import (extract_all_data,
                                                get_company_count)
from endole_scraper.tables import Endole
from endole_scraper.utils.utils import manage_browser_settings

BASE_URL = 'https://suite.endole.co.uk/explorer/postcode'


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

    driver = manage_browser_settings(
        driver=driver, orb=orb,
        viewport_change=3,
        driver_change=20,
        vpn_change=20,
    )

    try:
        df = extract_all_data(
            driver=driver, company_count=company_count,
        )

    except Exception as e:
        log.error(f"Error extracting {postcode}: {e}")
        df = None

    driver.quit()
    return df


def process_postcode(
        endole: Endole, outward_code: str, inward_code: Optional[str] = None,
):
    """
    Processes a list of postcode values, extracts data for each unique postcode,
    and stores formatted data in a database.

    Args:
        outward_code (str): The outward part of the postcode.
        inward_code (str): The inward part of the postcode.
        endole (Endole): An instance of the Endole class to interact with the database.
    """
    if inward_code:
        postcode = f"{outward_code}-{inward_code}".upper()
    else:
        postcode = outward_code.upper()

    log.info(f"Extracting data for outbound code {postcode}.")

    df = extract_data_for_postcode(postcode=postcode)
    if df is not None:

        log.info(f"Postcode {postcode} extracted with {len(df)} rows.")
        endole.ingest_dataframe(df=df)

    return


def scrape_and_process(index, outbound_code, endole, total):
    print(f"scraping {index}/{total}")
    process_postcode(endole=endole, outward_code=outbound_code)


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
    all_outward_codes = set([i.split('-')[0].upper() for i in postcode_list])

    all_outward_codes = sorted(all_outward_codes, reverse=True)

    all_outward_codes = all_outward_codes[all_outward_codes.index('SE14'):]

    for index, outbound_code in enumerate(all_outward_codes):

        print(f"Extracting outbound code {index}/{len(all_outward_codes)}")
        process_postcode(endole=endole, outward_code=outbound_code)


if __name__ == "__main__":
    main()
