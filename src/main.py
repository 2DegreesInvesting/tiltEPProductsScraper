from utils import *
import subprocess
import os

# Run scraper
if __name__ == "__main__":
    # Execute EuroPagesScraper
    EuroPagesProductsScraper().scrape_and_export("company_scraper", None, None)

    # Execute KvKNumberScraper
    # KvKNumberScraper().scrape()

    # Execute SMECompaniesScraper
    # SMECompaniesScraper().scrape()