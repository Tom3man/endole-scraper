from endole_scraper.common.extract_data import get_company_count


def test_company_count_type():

    BASE_URL = 'https://suite.endole.co.uk/explorer/postcode'
    postcode = 'al2-3bj'

    url = f"{BASE_URL}/{postcode}".lower()
    company_count = get_company_count(url=url)

    assert isinstance(company_count, int)
