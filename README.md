# Crypto Prices Scraper

This project scrapes cryptocurrency prices and stores them in BigQuery for analysis.

## Setup

1.  **Install Poetry:**
    If you don't have Poetry installed, follow the instructions on the [official Poetry website](https://python-poetry.org/docs/#installation).

2.  **Install Dependencies:**
    Navigate to the project directory and run the following command to create a virtual environment and install the required dependencies:
    ```bash
    poetry install
    ```

## Running the Scraper

To run the scraper and populate the BigQuery table, use the following command:

```bash
poetry run python src/crypto_prices/scraper.py
```
