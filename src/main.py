from utils import *
import sys
import os
import asyncio

sys.path.append("utils.py")

# Define async function
async def main():
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        dbutils.library.restartPython()
    # Execute EuroPagesScraper
    await EuroPagesProductsScraper().scrape_and_export("company_scraper", "italy", "agriculture_livestock", "agricultural_production")

# Run scraper
if __name__ == "__main__":
    # if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
    #     await main()
    # else:
    asyncio.run(main())