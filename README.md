# Tilt EuroPages Products Scraper

This repository contains the script needed to scrape the products and services from the EuroPages website. The script, located in [utils.py](https://github.com/2DegreesInvesting/tiltEPProductsScraper/blob/3a49a9fa5b5c95ceea6b209485f72e1a727c8ca0/src/utils.py#L36), consists of the following steps:


<b>1. Scraping</b>: First, the raw HTML from the EuroPages website is fetched through HTTP request. The response sent by the requests is then collected as raw HTML.

<b>2. Parsing</b>: With the raw HTML stored, the next step is to convert it into a human readable formats. The raw HTML contains all the information contained within the website but we are only interested in a subset of this abundance of information. The parsing step realises this procedure by extracting the relevant pieces of information out of raw HTML.

## Preparation
To run the scripts, a set amount of steps in preparation must be done beforehand.
### Setting up virtual environment
It is highly recommend to operate in a virtual environment to be able to experiment and test within isolation and therefore not run the risk of pottentially impacting other environments or the local system.

In the development of these scripts, a Conda virtual environment was used.

To create a conda environment, Anaconda or Miniconda needs to be installed. This can be done through the following links:

[Anaconda](https://www.anaconda.com/download)

[Miniconda](https://docs.conda.io/projects/miniconda/en/latest/)

Once either of these have been installed, a Conda environment can be created through the following command.

````
conda create -n ${name of your enviroment} python=${specify python version}
````

This might take a couple of minutes but once that is completed, the environment can be activated by running the following command:

````
conda activate ${name of the enviroment you just created}
````

Voila! You succesfully created the Conda virtual environment in which you will operate!

### Install required packages
The scripts created for the scrape-and-parse process depend on specific packages for operation. These packages are listed in the  `requirements.txt` file. These can all be easily installed by running the following command in your environment terminal:
````
$ pip install -r requirements.txt
````

## Running script
As the scrape-and-parse script is constructed as a standalone class, a demo [main.py](https://github.com/2DegreesInvesting/tiltEPProductsScraper/blob/3a49a9fa5b5c95ceea6b209485f72e1a727c8ca0/src/main.py) file has been created that creates an instance of this class which is then executed by calling the class [`.scrape_and_export()`](https://github.com/2DegreesInvesting/tiltEPProductsScraper/blob/3a49a9fa5b5c95ceea6b209485f72e1a727c8ca0/src/utils.py#L55) method. To run this main.py file, the following command must be executed in the terminal (make sure that you have navigated to the right directory in which the [main.py](https://github.com/2DegreesInvesting/tiltEPProductsScraper/blob/3a49a9fa5b5c95ceea6b209485f72e1a727c8ca0/src/main.py) file is stored, otherwise the script is not able to be executed).
````
$ python main.py
````

The output of this command is a CSV file stored in the [<i>output_data</i>](https://github.com/2DegreesInvesting/tiltEPProductsScraper/blob/d6f5a18af443228acd5da32d483445f90730d073/output_data/scraped_EP_products.csv) folder containing all the products and services documented on EuroPages.
