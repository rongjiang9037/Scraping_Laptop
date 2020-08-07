from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras

import pandas as pd
import numpy as np

import json
import logging
import re
import time

from sql_queries import *
from config import get_config

logging.basicConfig(level=logging.INFO)

config = get_config()


def is_last_page(soup, curr_page):
    """
    Determine whether the scraped page is the last page.
    If it's the last page, B&H will redirect scraper to page 1.
    
    input:
    soup - beautiful soup object
    curr_page - the current scraping page
    
    return:
    is_last_page - True/False
    """
    # get a list of pages diaplsy on bottom of the page
    listing_pages = soup.find_all('a', attrs={'data-selenium':'listingPagingLink'})
    listing_pages = [int(x.text) for x in listing_pages]
    
    # if current page is the biggest displayed on the page
    if curr_page == max(listing_pages):
        return True
    else:
        return False
   
    

def iter_laptop_from_page(soup):
    """
    Retrieve laptop related information from the page.
    - Product Name
    - Product Brand
    - Product SKU
    - Product Price
    - Product URL
    - Product Availability
    - Product # of Reviews
    
    input:
    soup - beautiful soup object
    
    output:
    page_info_dict - dictionary of each product
    """
    # return a list of laptop info from "itemDetail" divs
    products = soup.find_all('div', attrs={'data-selenium':'miniProductPage'})
    
    print(f'Found {len(products)} in total.')
    # go over the product
    for product in products:
        product_dict = {}
        # get current date as time
        product_dict['time'] = np.datetime_as_string(np.datetime64('now'), unit='D')

        # get product brand & name
        ProductBrandName = product.find('span', attrs={'data-selenium':'miniProductPageProductName'}).text
        try:
            product_dict['brand'] = re.findall('(^[a-zA-Z]+ ?[a-zA-Z]*) [0-9]+', ProductBrandName)[0].lower()
            product_dict['name'] = re.findall('[1][0-9\.]+\".*$', ProductBrandName)[0].lower()
        except:
            ProductBrandNameList = ProductBrandName.split()
            product_dict['name'] = ' '.join(ProductBrandNameList[1:])
            product_dict['brand'] = ProductBrandNameList[0]

        # get product sku
        ProductSku = product.find('div', attrs={'data-selenium':'miniProductPageProductSkuInfo'}).text
        product_dict['sku'] = re.findall('B&H # ([0-9A-Z]+) MFR', ProductSku)[0]

        # get product price
        try: 
            ProductPrice = product.find('span', attrs={'data-selenium':'uppedDecimalPrice'}).text
            ProductPrice = int(re.sub('[$,]', '', ProductPrice)) / 100
            product_dict['price'] = ProductPrice
        except:
            product_dict['price'] = None
            print(f"{ProductBrandName} doesn't have a price.")

        # get product regular price
        try:
            ProductRegPrice = product.find('del', attrs={'data-selenium':'strikeThroughPrice'}).text
            ProductRegPrice = re.findall('\$([0-9\.,]+)', ProductRegPrice)[0].replace(',', '')
            product_dict['reg_price'] = float(ProductRegPrice)
        except:
            product_dict['reg_price'] = None
            print(f"{ProductBrandName} doesn't have a reg price.")

        # get money saved
        try:
            ProductSaved = product.find('span', attrs={'data-selenium':'defaultSavingLabel'}).text
            ProductSaved = re.findall('\$([0-9\.,]+)', ProductSaved)[0].replace(',', '')
            product_dict['money_saved'] = float(ProductSaved)
        except:
            product_dict['money_saved'] = None
            print(f"{ProductBrandName} doesn't have money saved info.")

        # get product URL
        product_dict['url'] = product.find('a', attrs={'data-selenium':'miniProductPageProductImgLink'})['href'].lower()

        # get product availability info
        try:
            productAvalability = product.find('span', attrs={'data-selenium':'stockStatus'}).text.lower()
            product_dict['availability'] = productAvalability
        except: 
            product_dict['availability'] = None
            print(f"{ProductBrandName} doesn't have availability info.")

        # get number of reviews
        try:
            reviews_str = product.find('span', attrs={'data-selenium':'miniProductPageProductReviews'}).text.replace(',', '')
            reviews_str = re.findall('([0-9\.]+) Review', reviews_str)[0]
            reviews_int = int(reviews_str)
        except:
            reviews_int = 0

        product_dict['review_num'] = reviews_int

        yield product_dict
        

def iter_laptop_from_site():
    """
    It's a generator function.
    It scrapes data from URL 'https://www.bhphotovideo.com/c/buy/laptops/ci/18818/N/4110474292',
    and return product information.
    
    input:
    page_size - the number of pages in total to search on B&H laptop section
    
    output:
    product_info - a dictionary contains all necessary info for one product  
    """
    # B&H laptop URL
    url = 'https://www.bhphotovideo.com/c/buy/laptops/ci/18818/N/4110474292/pn/'
    # scrape from the first page
    page = 1
    
    while True:
        # make request for each page, get source code and parse with BeautifulSoup
        print(f'Prepare to scrape for page {page}.')
        try:
            print(url+str(page))
            req = Request(url+str(page), headers = {'User-Agent':'Mozilla/5.0'})
            thepage = urlopen(req).read()
            page_soup = BeautifulSoup(thepage, 'html.parser')
        except Exception as e:
            print(f"ERROR ocurred when scraping data for page {page}.")
            print(f"ERROR message: {e}")
            break
              
        # parse data on this page
        product_info = iter_laptop_from_page(page_soup)
            
        yield from product_info
        
        # if last page is reached, exit
        if is_last_page(page_soup, page):
            print("Finished scraping all the data.")
            break
        
        # move to the next page
        page += 1

           
def process_data(cur, conn):
    """
    - Scrape laptop data
    
    - create staging_laptop table
    - Store scraped data into staging_laptop table
    
    - extract data from staging_laptop into laptop table
    - extract data from staging_laptop into brand table
    
    input: 
    cur - cursor variable
    conn - database connection
    """
    try:
        print("Started to scrape")
        # get all laptop info and save it as a list
        all_laptop_iter = iter_laptop_from_site()

        # create staging table
        cur.execute(staging_table_create)
        print("Created staging_laptop table.")

        # insert into staging table
        psycopg2.extras.execute_batch(cur, staging_table_insert, all_laptop_iter)
        print("Successfully inserted scrapted data into the staging table.")

        # extract data from staging table and insert into dimlaptop table
        cur.execute(laptop_table_insert)
        print("Successfully inserted scrapted data into laptop table")
        
        # extract brand data from staging table and insert into dimlaptop table
        cur.execute(brand_table_insert_from_staging)

        # extract data from ticker_csv and insert into brand table
        df_ticker = pd.read_csv('../data/brand_ticker_info.csv')
        df_ticker = df_ticker.rename(columns={'brand':'name'})
        psycopg2.extras.execute_batch(cur, brand_table_insert, df_ticker.to_dict(orient='records'))
        print("Successfully inserted data into brand table.")

        # extract time info from staging_laptop table
        cur.execute("SELECT DISTINCT time FROM staging_laptop2")
        curr_time = cur.fetchone()[0]

        # convert np.datetime format to pd.timestamp
        time_dt = pd.to_datetime(curr_time)
        time_str = time_dt.strftime("%Y-%m-%d")
        cur.execute(time_table_insert, (curr_time, time_dt.day, 
                                        time_dt.week, time_dt.month, time_dt.year, time_dt.dayofweek))
        print("Successfully inserted data into time table.")

        # insert laptop info into laptop table
        cur.execute(laptopinfo_table_insert, {'time':time_str})
        print("Successfully inserted data into laptopinfo table.")

        # delete staging table
        cur.execute(staging_table_delete)
        print("Deleted staging_laptop table.")
        
        # commit 
        conn.commit()
    except Exception as e:
        print(f'Error happened: {e}')
        conn.rollback()
    
    
    
    
    
def main():
    """
    - Connect to postgresql database
    
    - get cursor variable
    
    - extract, transform and load data from B&H site
    
    - close database connection
    
    input: None
    return: None
    """
    # connect to database
    logging.info("Started to scrape data for {}".format(np.datetime_as_string(np.datetime64('now'), unit='D')))
    conn = psycopg2.connect(host='localhost', 
                            dbname='bnhlaptop', 
                            password='test', 
                            port=5432, 
                            user='postgres')
    cur = conn.cursor()
    logging.info("Connected to database bnhlaptop.")
    
    # process data
    process_data(cur, conn)

    # close database connection
    conn.close()
    
if __name__ == '__main__':
    main()