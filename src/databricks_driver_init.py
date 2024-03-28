# Databricks notebook source
import urllib, json, os
with urllib.request.urlopen("https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json") as url:
    data = json.load(url)
    print(data['channels']['Stable']['version'])
    url = data['channels']['Stable']['downloads']['chromedriver'][0]['url']
    print(url)
    
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
