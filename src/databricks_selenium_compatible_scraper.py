# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from selenium.webdriver.chrome.service import Service as ChromiumService
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from bs4 import BeautifulSoup

import urllib, json, os

# COMMAND ----------

# MAGIC %md
# MAGIC ## ChromeDriver Initialization

# COMMAND ----------


# get the url to the latest stable version of the chromedriver
with urllib.request.urlopen("https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json") as url:
    data = json.load(url)
    # print(data['channels']['Stable']['version'])
    url = data['channels']['Stable']['downloads']['chromedriver'][0]['url']
    # print(url)
    
    # set the url as environment variable to use in scripting 
    os.environ['url']= url

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -N $url  -O /tmp/chromedriver_linux64.zip
# MAGIC unzip /tmp/chromedriver_linux64.zip -d /tmp/chromedriver/
# MAGIC
# MAGIC sudo rm -r /var/lib/apt/lists/* 
# MAGIC sudo apt clean && 
# MAGIC    sudo apt update --fix-missing -y
# MAGIC
# MAGIC sudo curl -sS -o - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add
# MAGIC sudo echo "deb https://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list
# MAGIC sudo apt-get -y update
# MAGIC sudo apt-get -y install google-chrome-stable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Selenium Webdriver

# COMMAND ----------

def get_driver():
    # set the path to the chromedriver path
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
    return driver

# COMMAND ----------

test_driver = get_driver()

# COMMAND ----------

# get some data from an url
link = "https://www.europages.co.uk/bs"
test_driver.get(link)
soup = BeautifulSoup(test_driver.page_source, "html.parser") 

# COMMAND ----------

print(soup)
