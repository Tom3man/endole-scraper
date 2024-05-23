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
## Configuration

### Database Path

The scraper requires a database path to store extracted data. There are several ways you can specify this path:

*Command-Line Option*: You can directly pass the database path when you run the scraper using the --database-path option:

```python
poetry run python run.py --database-path /path/to/your/database
```

*Environment Variable*: If the --database-path option is not used, the scraper will look for an environment variable named DATABASE_PATH. You can set this variable in your shell or include it in a startup script:

```bash
export DATABASE_PATH=/path/to/your/database
```

*Default Repository Path*: If neither the command-line option nor the environment variable is set, the scraper defaults to using a path relative to the repository (REPO_PATH). This ensures that the scraper has a fallback location to store the database.


## Usage

### Running the Scraper

To start the scraper, ensure that you have the `postcodes.json` file in your project directory with the required structure (i.e., outward codes mapped to lists of inward codes). Then execute the main script:

```python
poetry run python run.py
```

Optionally, you can specify the database path as mentioned above:

```python
poetry run python run.py --database-path /path/to/your/database
```

## Understanding the Output

The script logs its progress and any issues encountered during execution.
Extracted data is formatted and ingested into a local SQLite database, specified by the DATABASE_PATH in the Endole class (default is the 'databases' folder within the repository).
