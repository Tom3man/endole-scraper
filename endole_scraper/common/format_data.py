import logging
import string
from datetime import datetime

import pandas as pd

log = logging.getLogger(__name__)


class DataFrameFormatter:

    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def _replace_hyphens_with_none(self):
        """Replace hyphens with None types."""
        for column in self.dataframe.columns:
            self.dataframe[column] = self.dataframe[column].apply(
                lambda x: None if x == '–' else x)

    @staticmethod
    def _format_currency(currency: str) -> int:
        """Format currency values."""
        multipliers = {'k': 1000, 'm': 1000000}
        if currency:
            numerals = ''.join(
                char for char in currency if char in '0123456789.-')
            alpha_chars = ''.join(
                char for char in currency if char in string.ascii_letters)
            if alpha_chars:
                numerals = float(numerals) * int(
                    multipliers[alpha_chars.strip()])
            return int(numerals)
        else:
            return currency

    @staticmethod
    def _format_date(date_str: str) -> str:
        """Format date values into a SQLite-compatible string format."""
        if date_str:
            try:
                # Parse the date string into a datetime object
                date_obj = datetime.strptime(date_str, '%d %b %Y')
                # Format the datetime object into a string that SQLite can understand
                return date_obj.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                # Log the error and return the original date string if parsing fails
                log.debug(f"{e}: Invalid datetime.")
                return date_str
        else:
            return date_str

    @staticmethod
    def _format_percentages(perc_str: str) -> float:
        """Format percentage values."""
        if perc_str and perc_str != "" and isinstance(perc_str, str):
            numerals = ''.join(
                char for char in perc_str if char in '0123456789.-')
            if numerals:
                return round(float(numerals) / 100, 4)
        return perc_str

    def format_dataframe(self) -> pd.DataFrame:
        """Format the entire DataFrame."""

        self.dataframe.drop(columns=['Telephone'], inplace=True)

        columns_mapping = {
            'Company': 'COMPANY',
            'Status': 'STATUS',
            'Net Assets': 'NET_ASSETS',
            'Turnover': 'TURNOVER',
            'Name': 'NAME',
            'Reg. No.': 'REG_NO',
            'Type': 'TYPE',
            'Size': 'SIZE',
            'Adversity': 'ADVERSITY',
            'Accounts': 'ACCOUNTS',
            'Employees': 'EMPLOYEES',
            'Directors': 'DIRECTORS',
            'Incorporation': 'INCORPORATION',
            'Acc. Year End': 'ACCOUNTS_YEAR_END',
            'Acc. Due By': 'ACCOUNTS_DUE_BY',
            'Acc. Last Made': 'ACCOUNTS_LAST_MADE',
            'Website': 'WEBSITE',
            'Address': 'ADDRESS',
            'County': 'COUNTY',
            'SIC Code': 'SIC_CODE',
            'Current Assets': 'CURRENT_ASSETS',
            'Total Assets': 'TOTAL_ASSETS',
            'Current Liab.': 'CURRENT_LIABILITIES',
            'Total Liab.': 'TOTAL_LIABILITIES',
            'Current Assets %': 'CURRENT_ASSETS_PERC',
            'Fixed Assets %': 'FIXED_ASSETS_PERC',
            'Total Assets %': 'TOTAL_ASSETS_PERC',
            'Net Assets %': 'NET_ASSETS_PERC',
            'Current Liab. %': 'CURRENT_LIABILITIES_PERC',
            'Long Term Liab. %': 'LONG_TERM_LIABILITIES_PERC',
            'Total Liab. %': 'TOTAL_LIABILITIES_PERC',
            'Turnover %': 'TURNOVER_PERC',
            'POSTCODE': 'POSTCODE',
        }

        self.dataframe.rename(columns=columns_mapping, inplace=True)
        self._replace_hyphens_with_none()

        currency_cols = [
            'NET_ASSETS', 'TURNOVER', 'CURRENT_ASSETS', 'TOTAL_ASSETS',
            'CURRENT_LIABILITIES', 'TOTAL_LIABILITIES',
        ]

        for col in currency_cols:
            self.dataframe[col] = self.dataframe[col].apply(
                lambda x: self._format_currency(x))

        date_cols = [
            'INCORPORATION', 'ACCOUNTS_YEAR_END',
            'ACCOUNTS_DUE_BY', 'ACCOUNTS_LAST_MADE'
        ]

        for col in date_cols:
            self.dataframe[col] = self.dataframe[col].apply(
                lambda x: self._format_date(x))

        perc_cols = [
            'CURRENT_ASSETS_PERC', 'FIXED_ASSETS_PERC',
            'TOTAL_ASSETS_PERC', 'NET_ASSETS_PERC',
            'CURRENT_LIABILITIES_PERC', 'LONG_TERM_LIABILITIES_PERC',
            'TOTAL_LIABILITIES_PERC', 'TURNOVER_PERC',
        ]

        for col in perc_cols:
            self.dataframe[col] = self.dataframe[col].apply(
                lambda x: self._format_percentages(x))

        self.dataframe[
            ['COMPANY', 'ADDRESS']] = self.dataframe['COMPANY'].str.split(
                '  ', expand=True)

        return self.dataframe
