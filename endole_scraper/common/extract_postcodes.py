import random
import time
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from orb.scraper.utils import spoof_request
from tqdm import tqdm


class GetPostcodes:

    BASE_URL = 'https://suite.endole.co.uk/explorer/browse/postcodes/'
    URL_SUFFIX = 'https://suite.endole.co.uk/'

    @staticmethod
    def get_soup(url: str) -> BeautifulSoup:
        """
        Retrieves and parses the HTML content from the specified URL, incorporating random delays
        to mimic human browsing patterns.

        Args:
            url (str): The URL from which to fetch the HTML content.

        Returns:
            BeautifulSoup: Parsed HTML content of the page.

        Raises:
            ValueError: If the content cannot be retrieved successfully.
        """
        time.sleep(random.uniform(0.01, 0.05))

        # Fetch the webpage content
        response = spoof_request(url=url, use_proxies=False)
        if response.status_code != 200:
            print(url)
            raise ValueError("Failed to retrieve content from URL")

        return BeautifulSoup(response.content, 'html.parser')

    def get_folders(self, url: str = BASE_URL, depth: Optional[int] = 4) -> List[str]:
        """
        Recursively fetches URLs of folders from a specified URL up to a given depth.

        Args:
            url (str): The URL to start fetching folders from.
            depth (Optional[int]): The depth to which the function should recurse.

        Returns:
            List[str]: A list of folder URLs found at the deepest specified depth.
        """
        if depth == 0:
            return []

        soup = self.get_soup(url)
        folders = soup.find(class_='folders')
        if folders is None:
            return []

        all_folders = folders.find_all('a')
        all_urls = [i['href'] if 'href' in i.attrs else self.BASE_URL for i in all_folders]
        all_urls = [url if url.startswith(self.URL_SUFFIX) else self.URL_SUFFIX + url for url in all_urls]

        # Collect only the URLs at the deepest level specified
        results = []
        if depth == 1:
            return all_urls

        # Use tqdm to show progress bar for the recursion
        for url in tqdm(all_urls, desc=f"Recursing into depth {depth} for {url.split('/')[-2]}"):
            results.extend(self.get_folders(url, depth - 1))

        return results

    @staticmethod
    def build_postcode_dictionary(url_list: List[str]) -> Dict[str, List[str]]:
        """
        Constructs a dictionary from a list of URLs, where each URL contains a postcode.
        The dictionary maps outbound postcode parts to a list of their associated inbound parts.

        Args:
            url_list (List[str]): A list of URLs containing postcode information.

        Returns:
            Dict[str, List[str]]: A dictionary where each key is an outbound postcode
                                and each value is a list of associated inbound postcode parts.

        Note:
            This function assumes that the postcodes are in the format 'outbound-inbound'
            immediately following the substring 'postcode/' in the URLs.
        """
        postcodes_dict = {}

        # Process each URL with a progress bar
        for url in tqdm(url_list, desc="Building postcode dictionary"):
            # Extract the postcode part after 'postcode/'
            start_index = url.find("postcode/") + len("postcode/")
            if start_index == -1 + len("postcode/"):  # If 'postcode/' is not found, skip this URL
                continue

            # The part after 'postcode/' up to the next slash or end of URL
            end_index = url.find('/', start_index)
            postcode = url[start_index:end_index if end_index != -1 else None]

            try:
                outbound_code, inbound_code = postcode.split('-')
                if outbound_code in postcodes_dict:
                    postcodes_dict[outbound_code].append(inbound_code)
                else:
                    postcodes_dict[outbound_code] = [inbound_code]
            except ValueError:
                print(f"Unexpected postcode format in URL: {url}")

        return postcodes_dict
