# Endole Suite Postcode Scraper

## Overview

This Python project automates the extraction of business information from the Endole Suite based on postcode searches. Built with Selenium WebDriver, the scraper includes advanced features such as dynamic IP rotation and user-agent switching to minimize detection and blocking risks. It supports multi-threaded execution for enhanced scraping speed and efficiency.

## Features

- **Dynamic Browser Management**: Automatically adjusts browser settings, including viewport size and user-agent, to simulate human behavior and avoid detection.
- **IP Rotation**: Dynamically changes IP addresses to avoid blocking, utilizing Private Internet Access VPN (subscription required).
- **Concurrency**: Leverages Python's `concurrent.futures` for multithreading, significantly speeding up the data extraction process.
- **Data Extraction**: Extracts and organizes business data from the Endole Suite website into structured pandas DataFrames.
- **Database Integration**: Stores extracted data in a SQLite database for easy access and data persistence.

## Installation

To run the scraper, follow these steps to set up your Python environment and install the necessary dependencies:

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/yourusername/endole-scraper.git
    cd endole-scraper
    ```

2. **Set Up Python Environment**:
    - Ensure Python 3.12+ is installed on your system.
    - This repository uses Poetry for dependency management. To create the lock file:
      ```bash
      poetry lock
      ```

3. **Install Dependencies**:
    - Install the dependencies from the lock file:
      ```bash
      poetry install --no-cache
      ```

## Configuration

### Database Path

The scraper requires a database path to store extracted data. You can specify this path in several ways:

- **Command-Line Option**: Pass the database path directly when running the scraper using the `--database-path` option:
    ```bash
    poetry run python run.py --database-path /path/to/your/database
    ```

- **Environment Variable**: If the `--database-path` option is not provided, the scraper will look for an environment variable named `DATABASE_PATH`. You can set this variable in your shell or include it in a startup script:
    ```bash
    export DATABASE_PATH=/path/to/your/database
    ```

- **Default Repository Path**: If neither the command-line option nor the environment variable is set, the scraper defaults to a path relative to the repository (REPO_PATH), providing a fallback location for the database.

## Usage

There are three primary scripts available for execution:

1. `run_extract_postcodes.py` - Extracts available postcodes and generates a `.json` file.
2. `run_outbound_codes.py` - Processes data at the outbound code level using the `.json` output from the first script.
3. `run_full_postcodes.py` - Extracts all data at the full postcode level using the `.json` output from the first script.

### Postcode Extract

This script extracts all available postcodes from the Endole website and generates a `.json` file in the format: `outbound: [inbound codes]`.

To run the script:

```bash
poetry run python run_extract_postcodes.py
```

Optionally, you can specify the output file path:

```bash
poetry run python run_extract_postcodes.py --file-path /path/to/your/output.json
```

### Outbound Code Extract

This script processes data at the outbound code level, iterating through all inbound codes associated with an outbound code and extracting the relevant data.

Before running this script, ensure you have the postcodes.json file in your project directory with the required structure (outward codes mapped to lists of inward codes). Then execute:

```bash
poetry run python run_outbound_codes.py
```

Optionally, you can specify the databse path:

```bash
poetry run python run_outbound_codes.py --databse-path /path/to/your/output.json
```

### Postcode Level Extract

This script processes data at the full postcode level, iterating through all postcodes listed in the .json file and extracting the corresponding data. It checks the current database for previously scraped postcodes to avoid duplication.

Before running this script, ensure you have the postcodes.json file with the required structure. Then execute:

```bash
poetry run python run_full_postcodes.py
```

Optionally, you can specify the databse path:

```bash
poetry run python run_full_postcodes.py --databse-path /path/to/your/output.json
```


### Understanding the Output

The scraper logs its progress and any issues encountered during execution. Extracted data is formatted and ingested into a local SQLite database, specified by the DATABASE_PATH environment variable or the command-line option. If no path is specified, the default is the databases folder within the repository.

All data will be logged to a SQLite local database, the schema for this can be found in the /tables folder within this repository (we use the sqlite-forger repository).


## Further Reading

Blog posts explaining the code and outputs further can be found here:

[Part 1](https://medium.com/dev-genius/robin-hood-data-diaries-scraping-endole-part-1-an-introduction-3f043ba57aac)
[Part 2](https://medium.com/dev-genius/robin-hood-data-diaries-scraping-endole-part-2-getting-targets-fdf0ee8d917e)
[Part 3](https://medium.com/dev-genius/robin-hood-data-diaries-scraping-endole-part-3-08908bf7ff26)
[Part 4](https://medium.com/dev-genius/robin-hood-data-diaries-scraping-endole-part-4-extracting-t-e5da594a8f04)