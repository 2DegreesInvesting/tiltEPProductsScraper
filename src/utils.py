# import required libraries
import csv
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait
from selenium.webdriver.common.by import By
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
        self.base_url = "https://www.europages.co.uk/{0}/{1}.html"
        self.updated_products_file = "../output_data/updated_products.csv"
        with open(self.updated_products_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["products_and_services", "id"])
            file.close()

    def cookie_handler(self, provided_driver):
        # accept the cookies and then wait a little bit
        try:
            WebDriverWait(provided_driver, 10).until(EC.element_to_be_clickable((By.ID, "cookiescript_accept"))).click()
        except Exception as e:
            pass
        time.sleep(1)

    def get_driver(self):
        options = webdriver.ChromeOptions() 
        options.add_argument("--headless") # Set the Chrome webdriver to run in headless mode for 
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)
        return driver
    
    # Function to generate a unique ID for each product name using a hash function
    def generate_product_id(self, product_name):
        return hashlib.md5(product_name.encode()).hexdigest()
    
    def extract_products_and_services(self, link: str, company_id: str):
        current_driver = self.get_driver()
        current_driver.get(link)
        self.cookie_handler(current_driver)

        # check if there is a particular element that needs to be clicked to reveal more products
        try:
            WebDriverWait(current_driver, 3).until(EC.element_to_be_clickable((By.XPATH, "//section[@class='ep-keywords ep-page-epage-home__order-other pb-4']/button")))
            current_driver.find_element(By.XPATH,"//section[@class='ep-keywords ep-page-epage-home__order-other pb-4']/button").click()
            print("Webpage with more products and services found.")
        except Exception as e:
            return

        # wait 2 seconds to simulate human behavior
        time.sleep(2)
        # then scrape from the page
        soup = BeautifulSoup(current_driver.page_source, "html.parser")
        # get the activities
        activities = [activity.text.strip() for activity in soup.find("ul", class_="ep-keywords__list pl-0").find_all("li")]  
        # merge the list together separate by |
        activities = ' | '.join(activities).lower()
        with open(self.updated_products_file, 'a') as file:
            writer = csv.writer(file)
            writer.writerow([activities, company_id])
            file.close()
        current_driver.quit()

    def scrape_and_export(self):
        # first obtain a list of all the companies_ids
        companies_t = pd.read_csv("../input_data/products.csv")["id"].tolist()
        # splitted_ids = np.array_split(companies_t,os.cpu_count())
        companies = [company.upper() for company in companies_t]
        links = np.array([self.base_url.format(*company.split("_")) for company in companies])
        # split the list by the number of cores
        # splitted_links = np.array_split(links, os.cpu_count())

        # List of jobs to get executed
        executors_list = []

        print("--- Starting scraping procedure--")
        # start timer
        start_time = time.time()
        print("Scraping company products_and_services from EuroPages")
        with ThreadPoolExecutor() as executor:
            # Add the 10 concurrent tasks to scrape product data from different parts of the 'links' list
            for i in range(len(links)):
                executors_list.append(
                    executor.submit(self.extract_products_and_services, links[i], companies_t[i])
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
        


