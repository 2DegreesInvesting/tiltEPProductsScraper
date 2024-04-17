# import required libraries
from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from urllib.parse import quote
from bs4 import BeautifulSoup
from tqdm import tqdm

import pandas as pd
import numpy as np
import unidecode
import hashlib
import csv
import re
import os

import itertools
import asyncio
import aiohttp
import codecs

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class EuroPagesProductsScraper():
    """

    """

    def __init__(self):
        self.base_url = "https://www.europages.co.uk/{}/{}.html"
        self.base_path = "abfss://landingzone@storagetiltdevelop.dfs.core.windows.net/tiltEP/"
    
    async def bound_fetch(self, sem, url, session, function, pbar):
        # Getter function with semaphore.
        async with sem:
            result = await function(url, session)
            pbar.update(1)
            return result
    
    async def send_async_task(self, links, function, type):
        tasks = []
        # create instance of Semaphore
        sem = asyncio.Semaphore(1000)
        
        # Create client session that will ensure we dont open new connection
        # per each request.
        async with aiohttp.ClientSession() as session:
            # start timer
            pbar = tqdm(total=len(links), desc='Scraping {}'.format(type), bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}')    
            for link in links:
                # pass Semaphore and session to every GET request
                task = asyncio.ensure_future(self.bound_fetch(sem, link, session, function, pbar))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
        return responses
    
    # Function to generate a unique ID for each product name using a hash function
    def generate_hash_id(self, name):
        return hashlib.md5(name.encode()).hexdigest()
    
    async def extract_products_and_services(self, url, session):
        async with session.get(url,timeout=5000) as response: # not using proxy at the moment
            body = await response.text()
            soup = BeautifulSoup(body, 'html.parser')
            activities = []
            # check if you can there is extra information that can be scraped
            try:
                backend_script = soup.find("script", string=re.compile(r"^window\.__NUXT__"))
                # Extract the information between "keywords: [ ]"
                keywords = re.findall(r'keywords:\s*\[(.*?)\]', '\n'.join(backend_script), re.DOTALL)
                # Remove leading and trailing whitespaces from each keyword
                keywords = [keyword.strip() for keyword in keywords]
                # Extract the words between "name:" and "}"
                words = [unidecode.unidecode(word.lower()).strip() for word in re.findall(r'name:"(.*?)"', keywords[1])]
                activities.append(words)
            except Exception as e:
                pass
            try:
                activities.append([codecs.decode(unidecode.unidecode(activity.text.lower()), 'unicode_escape').strip() for activity in soup.find("ul", class_="ep-keywords__list pl-0").find_all("li")])
                flattened_activities = list(set(itertools.chain.from_iterable(activities)))
                # merge the list together separate by |
                activities = ' | '.join(flattened_activities)
                return activities
            except Exception as e:
                pass

    async def extract_ep_categories_and_sectors(self, url, session):
        async with session.get(url,timeout=5000) as response: 
            body = await response.text()
            soup = BeautifulSoup(body, 'html.parser')
            cats_and_sectors = []
            # get the categories
            cards = soup.find_all("li", class_="ep-business-sector-item")
            for card in cards:
                category = " ".join(card.find("span", class_="ep-card-title__text").text.strip().split())
                sectors = card.find_all("a", class_="ep-link-list")
                for sector in sectors:
                    cats_and_sectors.append([category, " ".join(sector.text.strip().split())])

            return cats_and_sectors
        
    async def extract_ep_subsectors(self, url, session):
        async with session.get(url,timeout=5000) as response: 
            body = await response.text()
            soup = BeautifulSoup(body, 'html.parser')
            cats_sectors_and_subsectors = []
            # get the subsectors
            subsectors = soup.find_all("a", class_="ep-link-list px-4 py-3 text-decoration-none d-flex")
            for subsector in subsectors:
                category = " ".join(soup.find_all("a", class_="v-breadcrumbs__item")[-1].text.strip().split())
                sector = " ".join(soup.find("div", class_="v-breadcrumbs__item").text.strip().split())

                cats_sectors_and_subsectors.append([self.generate_hash_id(subsector.text.strip()), 
                                                    re.sub('[^0-9a-zA-Z]+', "_", category.lower()), 
                                                    re.sub('[^0-9a-zA-Z]+', "_", sector.lower()), 
                                                    re.sub('[^0-9a-zA-Z]+', "_", subsector.text.strip().lower()), 
                                                    "https://www.europages.co.uk/companies/{}.html".format(quote(sector.lower())),
                                                    "https://www.europages.co.uk{}".format(subsector["href"])])
            return cats_sectors_and_subsectors
            
    async def extract_company_urls(self, url, session):
        async with session.get(url,timeout=5000) as response: 
            body = await response.text()
            soup = BeautifulSoup(body, 'html.parser')
            # get the company urls
            company_url_final_part = soup.find_all("a", class_="ep-ecard-serp__epage-link")
            company_urls = ["https://www.europages.co.uk" + company["href"] for company in company_url_final_part]
        return company_urls
    
    async def extract_subsector_page_urls(self, url, session):
        async with session.get(url,timeout=5000) as response: 
            body = await response.text()
            soup = BeautifulSoup(body, 'html.parser')
            try:
                subsector_page_size = int(soup.find_all("a", class_="ep-server-side-pagination-item")[-2].text.strip())
                subsector_page_urls = [url.rsplit("/", 1)[0] + "/pg-{}/".format(i) + url.rsplit("/", 1)[1] for i in range(1, subsector_page_size+1)]
            except:
                subsector_page_urls = [url]
            return await self.send_async_task(subsector_page_urls, self.extract_company_urls, "subsectors")
    
    async def extract_company_information(self, url, session):
        async with session.get(url,timeout=5000) as response: 
            body = await response.text()
            soup = BeautifulSoup(body, 'html.parser')
            all_info = {"company_name": "", "group": "" , "sector": "", "subsector": "", "main_activity": "", "address": "", "company_city": "", 
                        "postcode": "", "country": "", "products_and_services": "", "information": "", "min_headcount": "", "max_headcount": "", "type_of_building_for_registered_address": "", 
                        "verified_by_europages": "", "year_established": "", "websites": "", "download_datetime": "", "id": "", "filename": ""}
            
            ## ID
            company_id = url.split("/")[-2].lower()+ "_" + url.split("/")[-1].split(".")[0].lower()
            all_info["id"] = company_id

            ## PRODUCT AND SERVICES
            activities = []
            # check if you can there is extra information that can be scraped
            try:
                backend_script = soup.find("script", string=re.compile(r"^window\.__NUXT__"))
                # Extract the information between "keywords: [ ]"
                keywords = re.findall(r'keywords:\s*\[(.*?)\]', '\n'.join(backend_script), re.DOTALL)
                # Remove leading and trailing whitespaces from each keyword
                keywords = [keyword.strip() for keyword in keywords]
                matches = re.findall(r'id:"keyword-(\d+)"', keywords[1])
                # Extract the words between "name:" and "}"
                words = [unidecode.unidecode(word.lower()).strip() for word in re.findall(r'name:"(.*?)"', keywords[1])]
                activities.append(words)
            except Exception as e:
                pass
            try:
                activities.append([codecs.decode(unidecode.unidecode(activity.text.lower()), 'unicode_escape').strip() for activity in soup.find("ul", class_="ep-keywords__list pl-0").find_all("li")])
                flattened_activities = list(set(itertools.chain.from_iterable(activities)))
                # merge the list together separate by |
                activities = ' | '.join(flattened_activities)
            except Exception as e:
                pass
            all_info["products_and_services"] = activities

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
            company_name = soup.find("h1", class_="ep-epages-header-title").text.split("-")[0].strip().lower()
            all_info["company_name"] = company_name

            try:
                ## ORGANISATION
                organisation = soup.find("div", class_="ep-epages-home-business-details__organization")
            except Exception as e:
                pass

            try:
                all_info["year_established"] = organisation.find("li", class_="ep-epages-business-details-year-established").find("dd").text.strip().lower()
            except Exception as e:
                pass

            try:
                all_info["main_activity"] = organisation.find("li", class_="ep-epages-business-details-main-activity").find("dd").text.strip().lower()
            except Exception as e:
                pass

            try:
                all_info["websites"] = soup.find("a", class_="ep-epage-sidebar__website-button")["href"]
            except Exception as e:
                pass

            try:
                soup.find("img", class_="ep-verified-badge")
                all_info["verified_by_europages"] = True
            except Exception as e:
                pass

            try:
                all_info["information"] = soup.find("div", class_="ep-epage-home-description__text").find("p").text.strip().lower()
            except Exception as e:
                pass

            try:
                location_info = soup.find("dl", class_="ep-epages-sidebar__info").find("dd").find_all("p")
                all_info["address"] = location_info[0].text.strip().lower()
                all_info["country"] = location_info[2].text.strip().split("-")[1].strip().lower()
                all_info["company_city"] = location_info[2].text.strip().split("-")[0].strip().split(" ", 1)[1].lower()
                all_info["postcode"] = location_info[2].text.strip().split("-")[0].strip().split(" ", 1)[0].lower()
            except Exception as e:
                pass
            
            return all_info
    
    async def scrape_and_export(self, type="product_updater", country=None, group=None, sector=None):
        print("--- Starting scraping procedure--")
        
        if type == "product_updater":
            if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
                files = [x[0] for x in dbutils.fs.ls("abfss://landingzone@storagetiltdevelop.dfs.core.windows.net/tiltEP/") if f"/{country}" in x[0]]
            else:
                files = [x[0] for x in dbutils.fs.ls("abfss:/mnt/indicatorBefore/tiltEP/") if f"/{country}" in x[0]]

            complete_ids = []
            for i in range(len(files)):
                x = spark.read.csv(files[i],header=True, inferSchema=True, sep=";").toPandas()
                ids = list(x.drop_duplicates(subset="id")["id"])
                complete_ids.append(ids)

            complete_ids = list(set([item for sublist in complete_ids for item in sublist]))
            companies = [company.upper() for company in complete_ids]
            links = np.array([self.base_url.format(*company.split("_")) for company in companies])
            results = await self.send_async_task(links, self.extract_products_and_services, "products and services")

        elif type == "company_scraper":
            input = "../output_data/ep_categorization.csv"
            categorization = pd.read_csv(input)
            subsector_urls = categorization[categorization["sector"] == sector]["subsector_url"].tolist()
            sector_url = categorization[categorization["sector"] == sector]["sector_url"].values[0]
            out = f"{country}-{group}-{sector}.csv"

            # store all company_information
            all_company_info = []

            # first scrape on sector level
            country_filtered_sector_url = sector_url.rsplit("/",1)[0]+ "/{}/".format(country) + sector_url.rsplit("/",1)[1]
            company_urls = (await self.send_async_task([country_filtered_sector_url], self.extract_subsector_page_urls, "company urls"))[0]
            # flatten company_urls
            company_urls = [item for sublist in company_urls for item in sublist]
            company_info = await self.send_async_task(company_urls, self.extract_company_information, "company information")
            for company in company_info:
                company["group"] = group
                company["sector"] = sector
                company["subsector"] = pd.NA
                company["filename"] = out
            all_company_info.append(company_info)

            # then on subsector level
            country_filtered_subsector_urls = [subsector_url.rsplit("/",1)[0]+ "/{}/".format(country) + subsector_url.rsplit("/",1)[1] for subsector_url in subsector_urls]
            for i in range(len(subsector_urls)):
                subsector = categorization[categorization["subsector_url"] == subsector_urls[i]]["subsector"].values[0]
                company_urls = (await self.send_async_task([country_filtered_subsector_urls[i]], self.extract_subsector_page_urls, "company urls"))[0]
                # flatten company_urls
                company_urls = [item for sublist in company_urls for item in sublist]
                company_info = await self.send_async_task(company_urls, self.extract_company_information, "company information")
                # for every company_info line, set the group, sector and subsector
                for company in company_info:
                    company["group"] = group
                    company["sector"] = sector
                    company["subsector"] = subsector
                    company["filename"] = out
                all_company_info.append(company_info)

            # flatten company_info
            all_company_info = [item for sublist in all_company_info for item in sublist]

            # convert to pandas dataframe
            all_company_info_df = pd.DataFrame(all_company_info)
            
            # drop duplicates
            all_company_info_df.drop_duplicates(inplace=True)

            if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            # Convert the pandas dataframe to a spark sql dataframe
                all_company_info_spark = spark.createDataFrame(all_company_info_df)
                all_company_info_spark = all_company_info_spark.withColumn("download_datetime", all_company_info_spark["download_datetime"].cast("String"))
                
                all_company_info_spark.coalesce(1).write.option("sep",";").csv(self.base_path+"address-temp/",  mode="overwrite", header=True)

                ##This remove all CRC files
                file_path = dbutils.fs.ls(self.base_path+"address-temp/")
                csv_path = [x.path for x in file_path if x.name.endswith(".csv")][0]
                dbutils.fs.cp(csv_path ,self.base_path + out)
                dbutils.fs.rm(self.base_path+"address-temp/", recurse=True)
            else:
                # export to csv
                export = "../output_data/" + out
                all_company_info_df.to_csv(export, sep=";", index=False)

        elif type == "categories_scraper":
            out = f"../output_data/ep_categorization.csv"
            cats_and_sectors = (await self.send_async_task(["https://www.europages.co.uk/bs"], self.extract_ep_categories_and_sectors, "groups and sectors"))[0]
            cats_and_sectors_links = ['https://www.europages.co.uk/bs/{0}/{1}'.format(re.sub('[^0-9a-zA-Z]+', "-", cats_and_sectors[i][0].lower()), re.sub('[^0-9a-zA-Z]+', "-", cats_and_sectors[i][1].lower())) 
                                      for i in range(len(cats_and_sectors))]
            cats_sectors_and_subsectors = (await self.send_async_task(cats_and_sectors_links, self.extract_ep_subsectors, "groups, sectors and subsectors"))
            # flatten cats_sectors_and_subsectors
            results = [item for sublist in cats_sectors_and_subsectors for item in sublist]

            # write out to a csv file with the following headers: category, sector, subsector, subsector_url
            with open(out, mode='w', newline='') as file:
                writer = csv.writer(file, delimiter=",")
                writer.writerow(["id", "category", "sector", "subsector", "sector_url", "subsector_url"])
                for subsector in results:
                    writer.writerow(subsector)
                file.close()

        print("--- Finished scraping procedure. Duration: DURATION MISSING---")
