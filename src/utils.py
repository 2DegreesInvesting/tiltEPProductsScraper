# import required libraries
import csv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, as_completed
from selenium.webdriver.chrome.service import Service as ChromiumService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from bs4 import BeautifulSoup
from bs4 import Comment
from tqdm import tqdm

import chromedriver_autoinstaller
import pandas as pd
import numpy as np
import unidecode
import requests
import hashlib
import random
import string
import time
import glob
import re
import os

chromedriver_autoinstaller.install() 

class EuroPagesProductsScraper():
    """

    A class that scrapes all products from the EuroPages website and outputs a CSV file containing all the products and services.

    @methods:

        retrieve_html(url): retrieves the HTML content of a given URL
        @args:
            url: the URL to retrieve the HTML content from

        parse_html(html_content): parses the HTML content of a given URL and extracts the products and services
        @args:
            html_content: the HTML content to parse
        
        generate_product_id(product_name): generates a unique ID for each product name using a hash function
        @args:
            product_name: the product name to generate the ID for

        scrape_and_export(): scrapes all products and services from the EuroPages website and exports them to a CSV file


    """

    def __init__(self):
        self.base_url = "https://www.europages.co.uk/{}/{}.html"

    def cookie_handler(self, provided_driver):
        # accept the cookies and then wait a little bit
        try:
            WebDriverWait(provided_driver, 10).until(EC.element_to_be_clickable((By.ID, "cookiescript_accept"))).click()
        except Exception as e:
            pass
        time.sleep(1)

    def get_driver(self):
        if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
            chrome_driver_path="/tmp/chromedriver/chromedriver-linux64/chromedriver"
            download_path="/tmp/downloads"
            option_args = ['--no-sandbox', '--headless', '--disable-dev-shm-usage', "disable-infobars", '--blink-settings=imagesEnabled=false', '--start-maximized'
                            '--ignore-certificate-errors', '--ignore-ssl-errors']
            options = webdriver.ChromeOptions() 
            prefs = {'download.default_directory' : download_path, 'profile.default_content_setting_values.automatic_downloads': 1, 
                     "download.prompt_for_download": False,"download.directory_upgrade": True, "safebrowsing.enabled": True,
                     "translate_whitelists": {"vi":"en"}, "translate":{"enabled":"true"}}
            options.add_experimental_option('prefs', prefs)
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            for option in option_args:
                options.add_argument(option)
            driver = webdriver.Chrome(service=ChromiumService(chrome_driver_path), options=options)
        else:
            options.add_argument("--headless") # Set the Chrome webdriver to run in headless mode for 
            options.add_argument('--no-sandbox')
            options.add_argument("--disable-dev-shm-using")
            options.add_argument("disable-infobars")
            options.add_argument('--blink-settings=imagesEnabled=false')
            driver = webdriver.Chrome(options=options)
        return driver
    
    # Function to generate a unique ID for each product name using a hash function
    def generate_hash_id(self, name):
        return hashlib.md5(name.encode()).hexdigest()
    
    def extract_products_and_services(self, out: string, link: string):
        company_id = link.split("/")[-2].lower()+ "_" + link.split("/")[-1].split(".")[0]
        current_driver = self.get_driver()
        current_driver.get(link)
        self.cookie_handler(current_driver)

        # check if there is a particular element that needs to be clicked to reveal more products
        try:
            WebDriverWait(current_driver, 2).until(EC.element_to_be_clickable((By.XPATH, "//section[@class='ep-keywords ep-page-epage-home__order-other pb-4']/button")))
            current_driver.find_element(By.XPATH,"//section[@class='ep-keywords ep-page-epage-home__order-other pb-4']/button").click()
        except Exception as e:
            pass

        # wait 2 seconds to simulate human behavior
        time.sleep(2)
        # then scrape from the page
        soup = BeautifulSoup(current_driver.page_source, "html.parser")
        # get the activities
        activities = [activity.text.strip() for activity in soup.find("ul", class_="ep-keywords__list pl-0").find_all("li")]  
        # merge the list together separate by |
        activities = ' | '.join(activities).lower()

        with open(out, 'a') as file:
            writer = csv.writer(file)
            writer.writerow([activities, company_id])
            file.close()
        current_driver.quit()

    def extract_ep_categories_and_sectors(self, out: string, link: string):
        current_driver = self.get_driver()
        current_driver.get(link)
        self.cookie_handler(current_driver)

        try:
            WebDriverWait(current_driver, 2).until(EC.element_to_be_clickable((By.CLASS_NAME, "ep-show-more-less")))
            buttons = current_driver.find_elements(By.CLASS_NAME,"ep-show-more-less")
            for button in buttons:
                button.click()
        except Exception as e:
            pass

        # wait 2 seconds to simulate human behavior
        time.sleep(2)
        # then scrape from the page
        soup = BeautifulSoup(current_driver.page_source, "html.parser")

        # get the categories
        cards = soup.find_all("li", class_="ep-business-sector-item")
        for card in cards:
            category = " ".join(card.find("span", class_="ep-card-title__text").text.strip().split())
            sectors = card.find_all("a", class_="ep-link-list")
            for sector in sectors:
                with open(out, 'a') as file:
                    writer = csv.writer(file)
                    writer.writerow([category, " ".join(sector.text.strip().split())])
                    file.close()
        current_driver.quit()

    def extract_ep_subsectors(self, out: string, link: string):

        current_driver = self.get_driver()
        current_driver.get(link)

        self.cookie_handler(current_driver)

        soup = BeautifulSoup(current_driver.page_source, "html.parser")

        # get the subsectors
        subsectors = soup.find_all("a", class_="ep-link-list px-4 py-3 text-decoration-none d-flex")
        for subsector in subsectors:
            # print(subsector.text.strip())
            category = " ".join(soup.find_all("a", class_="v-breadcrumbs__item")[-1].text.strip().split())
            sector = " ".join(soup.find("div", class_="v-breadcrumbs__item").text.strip().split())
            with open(out, 'a') as file:
                writer = csv.writer(file)
                writer.writerow([self.generate_hash_id(subsector.text.strip()), re.sub('[^0-9a-zA-Z]+', "_", category.lower()), re.sub('[^0-9a-zA-Z]+', "_", sector.lower()), re.sub('[^0-9a-zA-Z]+', "_", subsector.text.strip().lower()), "https://www.europages.co.uk/{}".format(subsector["href"])])
                file.close()

        current_driver.quit()

    def extract_company_urls(self, out: string, link: string):
        current_driver = self.get_driver()
        current_driver.get(link)

        self.cookie_handler(current_driver)
        last_page_reached = False   
        # try to click the next page button
        # while not last_page_reached:
        while not last_page_reached:
            try:
                soup = BeautifulSoup(current_driver.page_source, "html.parser")
                # get the company urls
                company_url_final_part = soup.find_all("a", class_="ep-ecard-serp__epage-link")
                company_urls = ["https://www.europages.co.uk" + company["href"] for company in company_url_final_part]
                for company_url in company_urls:
                    with open(out, 'a') as file:
                        writer = csv.writer(file)
                        writer.writerow([link, company_url])
                        file.close()
                WebDriverWait(current_driver, 4).until(EC.element_to_be_clickable((By.XPATH, "//ul[@class='ep-server-side-pagination__list text-center pl-0 pt-5']/li[last()]/a[@class='ep-server-side-pagination-item rounded ep-server-side-pagination__prev-next elevation-2']")))
                next_page = current_driver.find_elements(By.XPATH, "//ul[@class='ep-server-side-pagination__list text-center pl-0 pt-5']/li[last()]/a[@class='ep-server-side-pagination-item rounded ep-server-side-pagination__prev-next elevation-2']")
                current_driver.get(next_page[-1].get_attribute("href"))
            except Exception as e:
                print("Last page reached")
                last_page_reached = True
    
    def extract_company_information(self, out: string, classification: pd.DataFrame, link: string):
        
        current_driver = self.get_driver()
        current_driver.get(link)

        self.cookie_handler(current_driver)
        all_info = {"company_name": "", "group": "" , "sector": "", "subsector": "", "main_activity": "", "address": "", "company_city": "", 
                    "postcode": "", "country": "", "products_and_services": "", "information": "", "min_headcount": "", "max_headcount": "", "type_of_building_for_registered_address": "", 
                    "verified_by_europages": "", "year_established": "", "websites": "", "download_datetime": "", "id": "", "filename": ""}
        
        ## ID
        company_id = link.split("/")[-2].lower()+ "_" + link.split("/")[-1].split(".")[0]
        all_info["id"] = company_id

        ## PRODUCT AND SERVICES
        # check if there is a particular element that needs to be clicked to reveal more products
        try:
            WebDriverWait(current_driver, 2).until(EC.element_to_be_clickable((By.XPATH, "//section[@class='ep-keywords ep-page-epage-home__order-other pb-4']/button")))
            current_driver.find_element(By.XPATH,"//section[@class='ep-keywords ep-page-epage-home__order-other pb-4']/button").click()
        except Exception as e:
            pass

        # wait 2 seconds to simulate human behavior
        time.sleep(2)
        # then scrape from the page
        soup = BeautifulSoup(current_driver.page_source, "html.parser")
        # get the activities
        products_and_services = [activity.text.strip() for activity in soup.find("ul", class_="ep-keywords__list pl-0").find_all("li")]  
        # merge the list together separate by |
        products_and_services = ' | '.join(products_and_services).lower()
        all_info["products_and_services"] = products_and_services

        ## KEY FIGURES
        try:
            headcounts = soup.find("li", class_="ep-epages-business-details-headcount").find("dd").text.strip().split()
            min_headcount = headcounts[0].strip()
            max_headcount = headcounts[2].strip()
            all_info["min_headcount"] = min_headcount
            all_info["max_headcount"] = max_headcount
        except Exception as e:
            pass

        ## COMPANY_NAME
        company_name = soup.find("h1", class_="ep-epages-header-title").text.strip().lower()
        all_info["company_name"] = company_name

        ## SUBSECTOR
        subsector = re.sub('[^0-9a-zA-Z]+', "_", soup.find_all("a", class_="v-breadcrumbs__item")[1].text.strip().lower())
        all_info["subsector"] = subsector

        ## SECTOR
        row_of_relevancy = classification[classification["subsector"] == subsector]
        sector = row_of_relevancy["sector"].values[0]
        all_info["sector"] = sector
        ## GROUP
        group = row_of_relevancy["group"].values[0]
        all_info["group"] = group

        ## ORGANISATION
        organisation = soup.find("div", class_="ep-epages-home-business-details__organization")

        try:
            year_established = organisation.find("li", class_="ep-epages-business-details-year-established").find("dd").text.strip().lower()
            all_info["year_established"] = year_established
        except Exception as e:
            pass

        try:
            main_activity = organisation.find("li", class_="ep-epages-business-details-main-activity").find("dd").text.strip().lower()
            all_info["main_activity"] = main_activity
        except Exception as e:
            pass

        ## WEBSITES
        try:
            # get href of the website
            websites = soup.find("a", class_="ep-epage-sidebar__website-button")["href"]
            all_info["websites"] = websites
        except Exception as e:
            pass
        
        ## VERIFIED BY EUROPAGES
        try:
            verified_by_europages = soup.find("img", class_="ep-verified-badge")
            all_info["verified_by_europages"] = True
        except Exception as e:
            pass

        ## INFORMATION
        information = soup.find("div", class_="ep-epage-home-description__text").find("p").text.strip().lower()
        all_info["information"] = information
        ## ADDRESS
        location_info = soup.find("dl", class_="ep-epages-sidebar__info").find("dd").find_all("p")
        address = location_info[0].text.strip().lower()
        all_info["address"] = address
        ## COUNTRY
        country = location_info[2].text.strip().split("-")[1].strip().lower()
        all_info["country"] = country
        ## CITY
        city = location_info[2].text.strip().split("-")[0].strip().split(" ", 1)[1].lower()
        all_info["company_city"] = city
        ## POSTCODE
        postcode = location_info[2].text.strip().split("-")[0].strip().split(" ", 1)[0].lower()
        all_info["postcode"] = postcode

        # write all the values of the dictionary to the csv file
        with open(out, 'a') as file:
            writer = csv.writer(file)
            writer.writerow([*all_info.values()])
            file.close()
        
        current_driver.quit()

    def scrape_and_export(self, type="product_updater", input_file=None, country=None):
        # List of jobs to get executed
        executors_list = []

        if type == "product_updater":
            out = f"../output_data/product_updater_out_{time.time()}.csv"
            with open(out, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["product_and_services", "id"])
                file.close()
            try:
                companies_t = pd.read_csv(input_file)["id"].tolist()
            except Exception as e:
                raise Exception("Please provide a valid input file")
            companies = [company.upper() for company in companies_t]
            links = np.array([self.base_url.format(*company.split("_")) for company in companies])
            args = [self.extract_products_and_services, out] # args is always of structure [function, *function_args]

        elif type == "company_url_scraper": 
            input = "../output_data/sohail_ep_categories.csv"
            categorization = pd.read_csv(input)
            # only get rows where the sector column is equal to "Agriculture - Machines & Equipment"
            sector_urls = categorization[categorization["sector"] == "agriculture_machines_equipment"]["subsector_url"].tolist()
            sector_urls = [sector_url.rsplit("/",1)[0]+ "/{}/".format(country) + sector_url.rsplit("/",1)[1] for sector_url in sector_urls][:1]

            out = f"../output_data/company_url_scraper_out.csv"
            with open(out, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["subsector_url", "company_url"])
                file.close()
            links = sector_urls
            args = [self.extract_company_urls, out] # args is always of structure [function, *function_args]

        elif type == "company_scraper":
            input = "../output_data/company_url_scraper_out.csv"
            classification = pd.read_csv("../output_data/sohail_ep_categories.csv")
            company_urls = pd.read_csv(input)["company_url"].tolist()[:50]

            out = f"../output_data/sohail_ep_companies.csv"
            with open(out, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["company_name","group", "sector", "subsector", "main_activity" ,"address", "company_city", "postcode", "country", "products_and_services",
                                 "information", "min_headcount", "max_headcount", "type_of_building_for_registered_address", "verified_by_europages", "year_established","websites"
                                ,"download_datetime" ,"id", "filename"])
                file.close()

            links = company_urls
            args = [self.extract_company_information, out, classification] # args is always of structure [function, *function_args]

        elif type == "business_sectors_scraper":
            out = f"../output_data/recent_business_sectors_scraper_out.csv"
            with open(out, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["group", "sector"])
                file.close()
            links = ["https://www.europages.co.uk/bs"]
            args = [self.extract_ep_categories_and_sectors, out] # args is always of structure [function, *function_args]

        elif type == "categories_scraper":
            input = "../output_data/recent_business_sectors_scraper_out.csv"
            categories = pd.read_csv(input)["group"].tolist()
            sectors = pd.read_csv(input)["sector"].tolist()
            out = f"../output_data/sohail_ep_categories.csv"
            with open(out, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["category_id", "group", "sector", "subsector", "link"])
                file.close()
            links = ['https://www.europages.co.uk/bs/{0}/{1}'.format(re.sub('[^0-9a-zA-Z]+', "-", categories[i].lower()), re.sub('[^0-9a-zA-Z]+', "-", sectors[i].lower())) for i in range(len(categories))]
            args = [self.extract_ep_subsectors, out] # args is always of structure [function, *function_args]

        print("--- Starting scraping procedure--")
        # start timer
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            # Add the 10 concurrent tasks to scrape product data from different parts of the 'links' list
            for i in range(len(links)):
                executors_list.append(
                    executor.submit(*args, links[i])
                )

        wait(executors_list)

        # calculate duration of scraping procedure
        duration = time.time() - start_time
        print("--- Finished scraping procedure. Duration: {}---".format(duration))

class KvKNumberScraper():
    def __init__(self):
        self.search_box_url = "https://www.kvk.nl/zoeken/handelsregister/?postcode={}&zoekvervallen=0&zoekuitgeschreven=0"
        self.all_postal_codes = []

    def cookie_handler(self, provided_driver):
        try:
            WebDriverWait(provided_driver, 2).until(EC.element_to_be_clickable((By.XPATH, "//div[@class='o-grid-column-start-1-end-13']/button[@type='button']"))).click()
        except Exception as e:
            pass
        time.sleep(1)

    def get_driver(self):
        options = webdriver.ChromeOptions() 
        options.add_argument("--headless") # Set the Chrome webdriver to run in headless mode for 
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Remote(
            command_executor="http://localhost:4444", options=options
        )
        print("Driver has been created")
        return driver
    
    def extract_postal_codes(self):
        # get all postal codes csv files beneath the input_data/dutch_postal-codes-master directory
        files = [os.path.normpath(i).replace(os.sep, '/') for i in glob.glob("../input_data/dutch-postal-codes-master/*.csv")] 
        # create a set of postal codes
        postal_codes = set()
        for file in files:
            # read the csv file
            df = pd.read_csv(file)
            # add the postal codes to the set
            postal_codes.update(df["postal_code"].tolist())
        # convert the set to a list
        postal_codes = list(postal_codes)
        # sort the list
        postal_codes.sort()
        self.all_postal_codes = postal_codes

    def extract_kvk_numbers(self, links_array: list, kvk_data: list, driver_name: str):
        current_driver = self.get_driver()
        for link in links_array:
            print(driver_name, "is scraping postal code", link)
            current_driver.get(link)
            self.cookie_handler(current_driver)
            pages = current_driver.find_elements(By.XPATH,"//nav[@class='nav-new text-center']/ul/li/button")

            if len(pages)==0:
                divs = BeautifulSoup(current_driver.page_source, "html.parser").find_all("ul", class_="kvk-meta")
                for div in divs:
                    kvk_data.append(div.find("li").text.split(" ").pop(1).strip())        
            else:
                for page in pages:
                    page.click()
                    time.sleep(1)
                    # get all divs of class "content"
                    divs = BeautifulSoup(current_driver.page_source, "html.parser").find_all("ul", class_="kvk-meta")
                    for div in divs:
                        kvk_data.append(div.find("li").text.split(" ").pop(1).strip())        
        current_driver.quit()

    def scrape(self):
        self.extract_postal_codes()
        # pick 1000 random postal codes
        random_postal_codes = random.sample(self.all_postal_codes, 10000)
        links = np.array([self.search_box_url.format(postal_code) for postal_code in random_postal_codes])
        # List of jobs to get executed
        executors_list = []
        # split the list by the number of cores
        splitted_links = np.array_split(links, os.cpu_count())
        kvk_numbers = []
        print("Scraping KVK numbers from the Dutch Chamber of Commerce")
        with ThreadPoolExecutor() as executor:
            # Add the 10 concurrent tasks to scrape product data from different parts of the 'links' list
            for i in range(len(splitted_links)):
                executors_list.append(
                    executor.submit(self.extract_kvk_numbers, splitted_links[i], kvk_numbers, f"Driver {i}")
                )

        # Wait for all tasks to complete
        for x in executors_list:
            print(x)
        
        kvk_numbers = list(set(kvk_numbers))
        # create a pandas dataframe from the list of kvk numbers and export to a csv file
        df = pd.DataFrame(kvk_numbers, columns=["kvk_number"])
        # make the kvk numbers as strings
        df["kvk_number"] = df["kvk_number"].astype(str)
        df.to_csv("../output_data/scraped_kvk_numbers.csv", index=False)
        

class SMECompaniesScraper():

    def __init__(self):
        self.kvk_search_url = 'https://www.samrate.com/nl/search?q="{}"'
        # read the kvk numbers from the csv file as a list
        self.kvk_numbers = pd.read_csv("../output_data/scraped_kvk_numbers.csv")["kvk_number"].tolist()

    def retrieve_html(self, url):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        return soup
    
    def preprocess(self, text):
        text = text.strip().lower()
        text = unidecode.unidecode(text)
        text = re.sub(r"[,;()'./&%]", "", text)
        text = re.sub(r"\s+", "-", text)
        text = re.sub(r"-+", "-", text)
        return text
    
    def scrape(self):

        print("Scraping per kvk number from SamRate")
        company_information = [] 
        for i in tqdm(range(len(self.kvk_numbers)), bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
            search_url = self.kvk_search_url.format(str(self.kvk_numbers[i]))
            search_output = self.retrieve_html(search_url)
            companies_html = [t for t in search_output.find_all("div", class_="flex w-full")]
            for company_html in companies_html:
                company_details_html = company_html.find_all("span")
                company_information.append({"kvk_number": str(self.kvk_numbers[i]), "company_name": company_details_html[0].text.strip(), "company_city": company_details_html[1].text.strip()})


        print("Scraping all company details from SamRate")
        company_details = []
        for i in tqdm(range(len(company_information)), bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
            company_name = self.preprocess(company_information[i]["company_name"])
            company_city = self.preprocess(company_information[i]["company_city"])
            kvk_number = company_information[i]["kvk_number"]
            search_url =  "https://www.samrate.com/nl/{}".format(company_name + "-"+ company_city +"-"+ kvk_number)
            soup = self.retrieve_html(search_url) 
            data = []

            for comment in soup.find_all(string=lambda text:isinstance(text, Comment)):
                if comment.strip() == 'Details':
                    next_node = comment.next_sibling

                    while next_node and next_node.next_sibling:
                        data.append(next_node)
                        next_node = next_node.next_sibling

                        if not next_node.name and next_node.strip() == 'Widgets': 
                            break
            if len(data) > 0:
                data = data[1].find_all("div", class_ = re.compile(".*flex"))

                company_info = []
                for element in data:
                    # create a dictionary element where the key is the first span element and the value is the second span element
                    company_info.append({element.find_all("span")[0].text.strip(): element.find_all("span")[1].text.strip()})
                
                # flatten the list of dictionaries
                company_info = {k: v for d in company_info for k, v in d.items()}
                company_details.append(company_info)
        
        # convert to pd.DataFrame and export to CSV
        df = pd.DataFrame(company_details)
        df.to_csv("../output_data/scraped_SME_companies.csv", index=False)
        


