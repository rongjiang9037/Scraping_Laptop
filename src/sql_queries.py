# drop database
bnhlaptop_db_drop = """DROP DATABASE IF EXISTS bnhlaptop"""

# create database
bnhlaptop_db_create = """CREATE DATABASE bnhlaptop 
                            WITH ENCODING 'utf8'
                            TEMPLATE template0"""

# drop tables
laptop_table_drop = """DROP TABLE IF EXISTS dimlaptop"""

brand_table_drop = """DROP TABLE IF EXISTS dimbrand"""

time_table_drop = """DROP TABLE IF EXISTS dimtime"""

laptopinfo_table_drop = """DROP TABLE IF EXISTS factlaptopinfo"""

staging_table_delete = """DROP TABLE IF EXISTS staging_laptop2;"""

# create tables
laptop_table_create = """CREATE TABLE IF NOT EXISTS dimlaptop
                                (   laptop_key SERIAL PRIMARY KEY,
                                    name varchar NOT NULL,
                                    sku varchar NOT NULL,
                                    url varchar NOT NULL,
                                UNIQUE (sku));
                            """

brand_table_create = """CREATE TABLE IF NOT EXISTS dimbrand
                                (   brand_key SERIAL PRIMARY KEY,
                                    name varchar NOT NULL,
                                    ticker varchar,
                                    exchange_nm varchar,
                                UNIQUE(name));
                            """

time_table_create = """CREATE TABLE IF NOT EXISTS dimtime
                                (   time date PRIMARY KEY,
                                    day smallint NOT NULL,
                                    week smallint NOT NULL,
                                    month smallint NOT NULL,
                                    year smallint NOT NULL,
                                    weekday smallint NOT NULL
                                );
                            """
laptopinfo_table_create = """CREATE TABLE IF NOT EXISTS factlaptopinfo
                                (   ID SERIAL PRIMARY KEY,
                                    time date REFERENCES dimtime(time),
                                    laptop_key integer REFERENCES dimlaptop(laptop_key),
                                    brand_key integer REFERENCES dimbrand(brand_key),
                                    price numeric,
                                    reg_price numeric,
                                    money_saved numeric,
                                    availability varchar,
                                    review_num integer
                                )
                    """

staging_table_create = """CREATE TABLE IF NOT EXISTS staging_laptop2
                                (   ID SERIAL PRIMARY KEY,
                                    time date NOT NULL,
                                    name varchar NOT NULL,
                                    brand varchar NOT NULL,
                                    sku varchar NOT NULL,
                                    price numeric,
                                    reg_price numeric,
                                    money_saved numeric,
                                    url varchar NOT NULL,
                                    availability varchar,
                                    review_num integer,
                                UNIQUE(sku)
                                )
"""

# insert tables
laptop_table_insert = """INSERT INTO dimlaptop (name, sku, url)
                            SELECT name, sku, url
                            FROM staging_laptop2
                            ON CONFLICT (sku)
                            DO UPDATE SET name = excluded.name,
                                          url = excluded.url;
                      """

brand_table_insert = """INSERT INTO dimbrand (name, ticker, exchange_nm)
                            VALUES (%(name)s,
                                    %(ticker)s,
                                    %(exchange_nm)s)
                            ON CONFLICT (name)
                            DO UPDATE SET ticker = excluded.ticker,
                                          exchange_nm = excluded.exchange_nm;
"""
brand_table_insert_from_staging = """INSERT INTO dimbrand (name, ticker, exchange_nm)
                                        SELECT DISTINCT brand, null, null
                                            FROM staging_laptop2
                                    ON CONFLICT 
                                    DO NOTHING
"""

time_table_insert = """INSERT INTO dimtime (time, day, week, month, year, weekday)
                            VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (time)
                       DO NOTHING
                    """

laptopinfo_table_insert = """INSERT INTO factlaptopinfo (time, laptop_key, brand_key, price, reg_price, money_saved, availability, review_num)
                                SELECT %(time)s, 
                                       laptop.laptop_key, 
                                       brand.brand_key,
                                       staging.price,
                                       staging.reg_price,
                                       staging.money_saved,
                                       staging.availability,
                                       staging.review_num
                                FROM staging_laptop2 staging
                                JOIN dimlaptop laptop ON (staging.sku = laptop.sku)
                                JOIN dimbrand brand ON (staging.brand = brand.name);
"""


staging_table_insert = """INSERT INTO staging_laptop2 (name, time, brand, sku, price, reg_price, money_saved, url, availability, review_num)
                                VALUES ( %(name)s,
                                         %(time)s,
                                         %(brand)s,
                                         %(sku)s,
                                         %(price)s,
                                         %(reg_price)s,
                                         %(money_saved)s,
                                         %(url)s,
                                         %(availability)s,
                                         %(review_num)s
                                        )
                                ON CONFLICT (sku)
                                DO UPDATE SET name = excluded.name,
                                              time = excluded.time,
                                              brand = excluded.brand,
                                              sku = excluded.sku,
                                              price = excluded.price,
                                              reg_price = excluded.reg_price,
                                              money_saved = excluded.money_saved,
                                              url = excluded.url,
                                              availability = excluded.availability,
                                              review_num = excluded.review_num;
"""

# query list
drop_table_queries = [laptopinfo_table_drop, laptop_table_drop, brand_table_drop, time_table_drop]
create_table_queries = [laptop_table_create, brand_table_create, time_table_create, laptopinfo_table_create]

