from utils import *
import subprocess
import os

# Run scraper
if __name__ == "__main__":
    # # Start the selenium server in the background
    # hub = subprocess.Popen(['java', '-jar', 'selenium-server-4.18.1.jar',
    #                        'standalone','--log-level', 'OFF' 
    #                         ])
    # # Wait for a while to make sure the server has time to start
    # time.sleep(5)

    # Print the test phrase
    # print("Selenium Grid started at http://localhost:4444")

    start_time = time.time()
    # Execute EuroPagesScraper
    EuroPagesProductsScraper().scrape_and_export()

    # Execute KvKNumberScraper
    # KvKNumberScraper().scrape()

    # Execute SMECompaniesScraper
    # SMECompaniesScraper().scrape()
    # Wait for a while to make sure the server has time to start
    end_time = time.time()
    elapsed_time = end_time - start_time
    # hub.terminate()
    print(f"Elapsed run time: {elapsed_time} seconds")