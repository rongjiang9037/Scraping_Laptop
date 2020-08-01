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

def get_page_info(soup):
    """
    This function page information from each page.
    
    input:
    soup - beautiful soup object
    
    return:
    page - page info
    """
    # get all script snippets of type 'text/javascript'
    js_info = soup.find_all("script", attrs={"type":"text/javascript"})
    
    which_page = None
    
    # loop over each of them, looking for page info
    for s in js_info:
        try:
            which_page = re.search('\"page\" : (\d+),', s.text).group(1)
            break
        except:
            pass
    return int(which_page) if which_page else -1


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
    if curr_page != 1 and get_page_info(soup) <= 1:
        return True
    else:
        return False
    
def extract_availibility_info(info_str):
    """
    This function extract availibility info.
    
    input:
    info_str - string of availability
    
    return: 'in stock' / ''
    """
    info_str = info_str.lower()
    if 'in stock' in info_str:
        return 'in stock'
    elif 'special order' in info_str:
        return 'special order'
    elif 'back-ordered' in info_str:
        return 'back-ordered'
    elif 'new item' in info_str:
        return 'new item'
    elif 'more on the way' in info_str:
        return 'more on the way'
    else:
        return 'not available'
    

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
    products = soup.find_all('div', attrs={'data-selenium':'itemDetail'})
    
    for product in products:
        try:
            # empty dictionary
            product_dict = {}
            
            # get current date as time
            product_dict['time'] = np.datetime_as_string(np.datetime64('now'), unit='D')

            # get product name
            ProductName = product.find("span", attrs={'itemprop':'name'}).text.strip()
            product_dict['name'] = ProductName.lower()

            # get product brand
            ProductBrand = product.find("span", attrs={'itemprop':'brand'}).text.strip()
            product_dict['brand'] = ProductBrand.lower()

            # get product sku
            ProductInfoDict = json.loads(product['data-itemdata'])
            product_dict['sku'] = ProductInfoDict['sku']

            # get product price
            try: 
                product_dict['price'] = float(ProductInfoDict['price'])
            except:
                product_dict['price'] = None

            # get product URL
            product_dict['url'] = product.find("a", attrs={"class":"itemImg"})['href'].lower()


            # get product availability info
            productAvalability = product.find("div", attrs={"data-selenium":"salesComments"}).text.strip()
            product_dict['availability'] = extract_availibility_info(productAvalability)

            # get number of reviews
            try:
                reviews_str = product.find("a", attrs={'data-selenium':'itemReviews'}).text.strip()
                reviews_str = re.findall(re.compile(r'\((\d+)\)'), reviews_str)[0]
                reviews_int = int(reviews_str)
            except:
                reviews_int = 0
            product_dict['review_num'] = reviews_int
        
        except:
            logging.info('An Error Ocurred for product {}.'.format(ProductName))
            continue
            
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
    url = config.get('SCRAPE', 'URL')
    # scrape from the first page
    page = 1
    
    while True:
        # make request for each page, get source code and parse with BeautifulSoup
        try:
            req = Request(url+str(page), headers = {'User-Agent':'Mozilla/5.0'})
            thepage = urlopen(req).read()
            page_soup = BeautifulSoup(thepage, 'html.parser')
        except Exception as e:
            logging.info("ERROR ocurred when scraping data for page {}.".format(page))
            logging.info("ERROR message: {}".format(e))
            break
            
        # if last page is reached, exit
        if is_last_page(page_soup, page):
            logging.info("Finished scraping all the data.")
            break
        
        #parse data on this page
        product_info = iter_laptop_from_page(page_soup)
        
        # break loop if product_info is None
        if not product_info:
            break
            
        yield from product_info
        
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
        # get all laptop info and save it as a list
        all_laptop_iter = iter_laptop_from_site()

        # create staging table
        cur.execute(staging_table_create)
        print("Created staging_laptop table.")

        # insert into staging table
        psycopg2.extras.execute_batch(cur, staging_table_insert, all_laptop_iter)
        conn.commit()
        print("Successfully inserted scrapted data into the staging table.")

        # extract data from staging table and insert into dimlaptop table
        cur.execute(laptop_table_insert)
        print("Successfully inserted scrapted data into laptop table")
        
        # extract brand data from staging table and insert into dimlaptop table
        cur.execute(brand_table_insert_from_staging)
        
        # extract data from ticker_csv and insert into brand table
        df_ticker = pd.read_csv('/Users/margaret/OneDrive/Documents/projects/Scraping_Laptop/data/brand_ticker_info.csv')
        df_ticker = df_ticker.rename(columns={'brand':'name'})
        psycopg2.extras.execute_batch(cur, brand_table_insert, df_ticker.to_dict(orient='records'))
        conn.commit()
        print("Successfully inserted data into brand table.")

        # extract time info from staging_laptop table
        cur.execute("SELECT DISTINCT time FROM staging_laptop;")
        curr_time = cur.fetchone()[0]

        # convert np.datetime format to pd.timestamp
        time_dt = pd.to_datetime(curr_time)
#         time_str = np.datetime_as_string(curr_time, unit='D')
        cur.execute(time_table_insert, (curr_time, time_dt.day, 
                                        time_dt.week, time_dt.month, time_dt.year, time_dt.dayofweek))
        print("Successfully inserted data into time table.")

        # insert laptop info into laptop table
        cur.execute(laptopinfo_table_insert, {'time':curr_time})
        print("Successfully inserted data into laptopinfo table.")

        # delete staging table
        cur.execute(staging_table_delete)
        print("Deleted staging_laptop table.")
        conn.commit()
    except Exception as e:
        print(e)
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