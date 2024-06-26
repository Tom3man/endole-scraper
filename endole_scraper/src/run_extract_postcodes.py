import json
from typing import Optional

import click

from endole_scraper import REPO_PATH
from endole_scraper.common.extract_postcodes import GetPostcodes


@click.command(
    help="Endole Postcode Dictionary Builder"
)
@click.option("--file-path", type=click.STRING, required=False)
def main(file_path: Optional[str]):

    if not file_path:
        file_path = REPO_PATH

    extract_postcodes = GetPostcodes()

    all_urls = extract_postcodes.get_folders(depth=4)

    postcode_dict = extract_postcodes.build_postcode_dictionary(url_list=all_urls)

    try:
        with open(file_path + '/all_postcodes.json', 'w') as file:
            json.dump(postcode_dict, file, indent=4)
    except IOError as e:
        print(f"Error writing to all_postcodes.json to {file_path}: {e}")


if __name__ == "__main__":
    main()
