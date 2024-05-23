from typing import Dict, List

from sqlite_forge.database import SqliteDatabase


class Endole(SqliteDatabase):

    DEFAULT_PATH: str = "ENDOLE"
    PRIMARY_KEY: List[str] = ["COMPANY"]
    DEFAULT_SCHEMA: Dict[str, str] = {
        "COMPANY": "VARCHAR(255)",
        "STATUS": "VARCHAR(50)",
        "NET_ASSETS": "INT",
        "TURNOVER": "INT",
        "NAME": "VARCHAR(255)",
        "REG_NO": "VARCHAR(50)",
        "TYPE": "VARCHAR(50)",
        "SIZE": "VARCHAR(50)",
        "EMPLOYEES": "VARCHAR(50)",
        "ADVERSITY": "FLOAT",
        "ACCOUNTS": "FLOAT",
        "DIRECTORS": "VARCHAR(255)",
        "INCORPORATION": "DATE",
        "ACCOUNTS_YEAR_END": "DATE",
        "ACCOUNTS_DUE_BY": "DATE",
        "ACCOUNTS_LAST_MADE": "DATE",
        "WEBSITE": "VARCHAR(255)",
        "ADDRESS": "VARCHAR(255)",
        "COUNTY": "VARCHAR(50)",
        "SIC_CODE": "VARCHAR(50)",
        "CURRENT_ASSETS": "INT",
        "TOTAL_ASSETS": "INT",
        "CURRENT_LIABILITIES": "INT",
        "TOTAL_LIABILITIES": "INT",
        "CURRENT_ASSETS_PERC": "FLOAT",
        "FIXED_ASSETS_PERC": "FLOAT",
        "TOTAL_ASSETS_PERC": "FLOAT",
        "NET_ASSETS_PERC": "FLOAT",
        "CURRENT_LIABILITIES_PERC": "FLOAT",
        "LONG_TERM_LIABILITIES_PERC": "FLOAT",
        "TOTAL_LIABILITIES_PERC": "FLOAT",
        "TURNOVER_PERC": "FLOAT",
        "POSTCODE": "VARCHAR(50)"
    }
