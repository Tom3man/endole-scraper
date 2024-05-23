# Endole Suite Postcode Scraper

## Overview

This Python project is designed to automate data extraction from the Endole Suite, focusing on business information based on postcode searches. The scraper is built using Selenium WebDriver and integrates advanced features such as dynamic IP changing and user-agent rotation to minimize detection risks. It supports multi-threaded execution to enhance the scraping speed and efficiency.

## Features

- **Dynamic Browser Management**: Automatically adjusts browser settings, including viewport size and user agent, to mimic human behavior.
- **IP Rotation**: Changes IP addresses dynamically to avoid blocking (will require a subscription to Private Internet Access VPN).
- **Concurrency**: Uses Python's `concurrent.futures` for multithreading to speed up the data extraction process.
- **Data Extraction**: Extracts and formats business data from the Endole Suite website into structured pandas DataFrames.
- **Database Integration**: Seamlessly stores extracted data into a SQLite database, ensuring data persistence and easy access.

## Installation

Before you can run the scraper, you need to set up your Python environment and install necessary dependencies:

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/yourusername/endole-scraper.git
    cd endole-scraper
    ```

2. **Set Up Python Environment**:
    - Ensure Python 3.6+ is installed on your system.
    - It is recommended to use a virtual environment:
      ```bash
      python -m venv venv
      source venv/bin/activate  # On Windows use `venv\Scripts\activate`
      ```

3. **Install Dependencies**:
    - Install required Python packages:
      ```bash
      pip install -r requirements.txt
      ```

## Usage

### Running the Scraper

To start the scraper, ensure that you have the `postcodes.json` file in your project directory with the required structure (i.e., outward codes mapped to lists of inward codes). Then execute the main script:

```python
poetry run python run.py
```

## Understanding the Output

The script logs its progress and any issues encountered during execution.
Extracted data is formatted and ingested into a local SQLite database, specified by the DATABASE_PATH in the Endole class (default is the 'databases' folder within the repository).
