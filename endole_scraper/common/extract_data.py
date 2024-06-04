import io
import logging
import random
import time
from typing import List

import orb.spinner.utils as orb_utils
import pandas as pd
from bs4 import BeautifulSoup
from orb.scraper.utils import spoof_request
from selenium.common.exceptions import (StaleElementReferenceException,
                                        TimeoutException)
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

log = logging.getLogger(__name__)


def find_element_with_retry(driver, locator, retries=3):
    for attempt in range(retries):
        try:
            element = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located(locator)
            )
            return element
        except StaleElementReferenceException:
            if attempt < retries - 1:
                continue
            else:
                raise


def get_company_count(url: str) -> int:
    """
    Extracts the number of companies listed on a webpage from a specified element.

    This function fetches the webpage at the provided URL, finds an element by its class name
    that contains a header with the company count, extracts the text, and parses it to retrieve
    an integer count of companies.

    Parameters:
        url (str): The URL of the webpage from which to extract the company count.

    Returns:
        int: The number of companies extracted from the page.

    Raises:
        ValueError: If the required elements are not found or do not contain the expected text format.
    """
    # Fetch the webpage content
    response = spoof_request(url=url, use_proxies=False)
    if response.status_code != 200:
        raise ValueError("Failed to retrieve content from URL")

    soup = BeautifulSoup(response.content, 'html.parser')

    # Locate the header element that contains the company count
    elements = soup.find(class_='explorer-header')
    if elements is None:
        raise ValueError("Header element containing company count not found.")

    # Find the 'h2' tag within the 'explorer-header' element
    company_header = elements.find('h2')
    if company_header is None:
        raise ValueError("Sub-header element containing company count not found.")

    # Extract the number from the header text, assuming the format '123 Companies'
    try:
        company_count_text = company_header.text
        company_count = int(company_count_text.split(' ')[0])
    except ValueError:
        raise ValueError("Failed to parse company count from header text.")

    return company_count


def expand_all_columns(driver: WebDriver):
    """
    Expands all columns in a dynamic web interface by interacting with various UI elements.

    This function changes the viewport size, interacts with buttons to show and expand columns,
    and switches to an iframe to manage column visibility.

    Parameters:
        driver (WebDriver): The Selenium WebDriver used to interact with the webpage.

    Raises:
        ValueError: If any expected web element is not found or cannot be interacted with.
    """
    # Spoofing: Change viewport size for better visibility of elements
    orb_utils.change_viewport_size(driver=driver)

    # Click the show columns button twice to ensure expansion
    for _ in range(2):
        try:
            element = find_element_with_retry(
                driver=driver, locator=(By.CLASS_NAME, "show-columns-button")
            )
            time.sleep(random.uniform(0.01, 0.05))
            orb_utils.human_clicking(driver=driver, target_element=element)
        except TimeoutException:
            raise ValueError("The show columns button could not be found and clicked.")

    # Switch to the newly opened iframe where column settings are available
    try:
        iframe = WebDriverWait(driver, 2).until(
            ec.visibility_of_element_located((By.ID, "iframe2"))
        )
        driver.switch_to.frame(iframe)
    except TimeoutException:
        raise ValueError("Show column iframe pop up could not be identified")

    # Collect all elements used to add filters (i.e., expand columns)
    column_list = driver.find_elements(By.CLASS_NAME, "add-filter")
    if not column_list:
        raise ValueError("Filter list does not exist or no elements found.")

    # Click on each filter to expand columns
    for filter in column_list:
        time.sleep(random.uniform(0.01, 0.05))
        filter.click()

    try:
        # Find and click the apply columns button
        apply_columns = driver.find_elements(By.TAG_NAME, "a")[-1]

    except TimeoutException:
        raise ValueError("Apply columns button could not be clicked or was not visible.")

    if isinstance(apply_columns, WebElement):
        orb_utils.human_clicking(
            driver=driver,
            target_element=apply_columns,
            random_clicking=False,
        )
    else:
        raise ValueError("Apply columns button could not be clicked.")


def extract_page_table(driver: WebDriver) -> pd.DataFrame:
    """
    Extracts a table from the current page of a WebDriver instance and returns it as a pandas DataFrame.

    This function fetches the current URL's content, parses it into BeautifulSoup for HTML processing,
    then extracts the first table and converts it into a DataFrame.

    Parameters:
        driver (WebDriver): The Selenium WebDriver instance used to fetch the current page.

    Returns:
        pd.DataFrame: The extracted table converted into a pandas DataFrame.

    Raises:
        ValueError: If the page content cannot be parsed or if no tables are found.
    """
    # Request and parse the page content using BeautifulSoup
    response = spoof_request(url=driver.current_url, use_proxies=False)
    if not response:
        raise ValueError("Failed to retrieve content from driver's current URL")

    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all table elements in the parsed HTML
    tables = soup.find_all('table')
    if not tables:
        raise ValueError("Failed to extract any tables from the page content")

    # Convert the first table found to a string and wrap it in StringIO for reading into pandas
    html_string = tables[0].prettify()
    html_io = io.StringIO(html_string)

    # Read the HTML table into a DataFrame
    try:
        df = pd.read_html(html_io, header=0, flavor='bs4')[0].iloc[:, 1:-1]
    except Exception as e:
        raise ValueError(f"Failed to parse table into DataFrame: {e}")

    return df


def obtain_drop_down_buttons(driver: WebDriver) -> List[WebElement]:
    """
    Finds and returns a list of clickable dropdown elements from the header row of a table.

    This function searches the first header row ('tr') of a table for any dropdown triggers ('a' tags within 'td' elements),
    and returns them if they exist.

    Parameters:
        driver (WebDriver): The Selenium WebDriver used to interact with the webpage.

    Returns:
        List[WebElement]: A list of WebElement objects representing dropdown triggers.

    Raises:
        ValueError: If no header row is found or if the first row cannot be properly interpreted as a header.
    """
    # Attempt to find the header row in the table
    try:
        header_row = driver.find_element(By.TAG_NAME, "table").find_element(By.TAG_NAME, "tr")
    except Exception as e:
        raise ValueError(f"Failed to extract the header row for filter toggling: {str(e)}")

    drop_down_filters = []

    # Find all 'td' elements in the header row, and within those, find 'a' tags representing dropdowns
    for cell in header_row.find_elements(By.TAG_NAME, "td"):
        drop_down_items = cell.find_elements(By.TAG_NAME, "a")
        if drop_down_items:
            drop_down_filters.append(drop_down_items[0])

    return drop_down_filters


def change_order_of_column(driver: WebDriver, filter_no: int, order: bool):
    """
    Clicks on a dropdown menu to change the order of columns in a web table.

    The function locates a specific dropdown button associated with a table column,
    clicks it to reveal ordering options, and selects an order based on the 'order' flag.

    Parameters:
        driver (WebDriver):
            The Selenium WebDriver used to interact with the webpage.
        filter_no (int):
            The index of the dropdown button to interact with.
        order (bool):
            Determines the order of the column:
            True for ascending, False for descending.

    Raises:
        TimeoutException: If the dropdown or order buttons are not found within the specified time.
    """
    # Obtain dropdown buttons
    drop_down_buttons = obtain_drop_down_buttons(driver=driver)
    # Click the specified dropdown button
    drop_down_buttons[filter_no].click()

    # Wait for the order menu to be visible and obtain the order links
    try:
        order_menu = find_element_with_retry(
            driver=driver, locator=(By.ID, "column_menu")
        )

        order_buttons = order_menu.find_elements(By.TAG_NAME, "a")
    except TimeoutException:
        raise TimeoutException("Order menu was not found within the timeout period.")

    # Slight random delay to mimic human interaction
    time.sleep(random.uniform(0.01, 0.05))

    # Click the appropriate order button based on `order` value
    # Assuming the first link is for ascending and the second for descending
    order_buttons[int(order)].click()


def extract_all_data(driver: WebDriver, company_count: int) -> pd.DataFrame:
    """
    Extract data iteratively from a dynamic table using Selenium, showing progress with tqdm.

    Parameters:
        driver (WebDriver): The Selenium WebDriver object.
        company_count (int): Count of the companies at that postcode (url).

    Returns:
        pd.DataFrame: A DataFrame containing all extracted data.
    """

    # Change the viewport size to emulate different devices
    orb_utils.change_viewport_size(driver=driver)

    expand_all_columns(driver=driver)

    # Extract the current page table
    full_df = extract_page_table(driver=driver)

    if len(full_df) == company_count:
        return full_df

    drop_down_buttons = obtain_drop_down_buttons(driver=driver)

    cycles = 0
    for index, _ in enumerate(drop_down_buttons):
        # Two iterations: True and False
        for order in [True, False]:
            if cycles == 10 or len(full_df) == company_count:
                log.info(f"Terminating early after {cycles} cycles, extracted {len(full_df)}/{company_count} records.")
                break

            log.info(f"Cycle {cycles}: {len(full_df)}/{company_count} records extracted so far.")

            # Change the order of the columns based on the current cycle
            change_order_of_column(driver=driver, filter_no=index, order=order)

            # Extract the current page table
            df_cycle = extract_page_table(driver=driver)

            full_df = pd.concat([full_df, df_cycle]).drop_duplicates(
                subset=['Company']).reset_index(drop=True)

            cycles += 1

    return full_df
