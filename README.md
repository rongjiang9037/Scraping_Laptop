# Project Overview
This project scrapes [B&H Laptops'](https://www.bhphotovideo.com/c/buy/laptops/ci/18818/N/4110474292) daily price, availability and number of reviews info and store them into a STAR schema, Postgresql data tables for further analysis. 


## Design
### Relational Database
* Fact Table
    * factlaptopinfo
* Dimension Table
    * dimlaptop
    * dimbrand
    * dimtime

### ETL
The python library urllib.request and BeautifulSoup are used to scrape web data.

### Workflow
![Image of Workflow](https://www.dropbox.com/s/poagkbp0o54aejg/laptop_workflow.png?raw=1)

## Files
* analysis
    * trend_analysis.ipynb
        * Analyze trend for number of laptop, average price at B&H.
    * snapshot_analysis.ipynb
        * Analyze market share of B&H products with the latest available data.
* src
    * sql_queries.py
        * All SQL quries used for ETL are stored here
    * create_table.py
        * Create or recreate tables and related objects
    * etl.py
        * Used to ETL full dataset in Postgres database
* jupyter_notebook_test
    * etl.ipynb
        * Jupyter Notebook used to develop ETL.
    * create_table.ipynb
        * Jupyter Notebook used to test data creating process.
* data
    * brand_ticker_info.csv
        * This is a csv file contains laptop brand, ticker name and exchanged listed on if it's a public company.
        

## ER Diagram
![Image of ER Diagram](https://www.dropbox.com/s/pdoc6on5aarut2v/Blank%20Diagram2.jpeg?raw=1)
