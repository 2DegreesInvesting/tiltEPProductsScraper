# the required imports
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import numpy as np
import requests
import hashlib
import string
import time


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
        self.products_df = pd.DataFrame(columns=["Product Name", "Product ID"])
        self.alphabet = list(string.ascii_uppercase)
        self.base_url = "https://www.europages.co.uk/business-directory-europe/skwd/{}.html"
    
    def retrieve_html(self, url):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        return soup

    def parse_html(self, html_content):
        products_list = html_content.find("div", class_="ep-index-alpha-links-list").find_all('a', role='listitem')
        for product_name in products_list:
            self.products_df = self.products_df._append([{"Product Name": product_name.text, "Product ID": self.generate_product_id(product_name.text)}])
    
    # Function to generate a unique ID for each product name using a hash function
    def generate_product_id(self, product_name):
        return hashlib.md5(product_name.encode()).hexdigest()
    
    def scrape_and_export(self):
        print("--- Starting scraping procedure---")
        # start timer
        start_time = time.time()
        
        for letter in self.alphabet:
            category_size = len(self.retrieve_html(self.base_url.format(letter)).find("div", class_="ep-index-alpha-links-list").find_all('a', role='listitem'))
            print("Scraping and parsing products starting with letter {}".format(letter))
            for i in tqdm(range(1, category_size+1), bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
                url = self.base_url.format(letter + "-" + str(i))
                soup = self.retrieve_html(url)
                self.parse_html(soup)

            print("Finished scraping and parsing products starting with letter {}\n".format(letter))

        # calculate duration of scraping procedure
        duration = time.time() - start_time
        print("--- Finished scraping procedure. Duration: {}---".format(duration))

        # export the dataframe to a CSV file
        self.products_df.to_csv("../output_data/scraped_EP_products.csv", index=False)