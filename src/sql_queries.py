# drop tables
laptop_table_drop = """DROP TABLE IF EXISTS laptop;"""


# create tables
laptop_table_create = """CREATE TABLE IF NOT EXISTS laptop
                                (   product_id SERIAL PRIMARY KEY,
                                    name varchar not null,
                                    brand varchar,
                                    sku varchar not null,
                                    price numeric,
                                    url varchar not null,
                                    availability varchar,
                                    review_num int);
                            """


# insert tables
laptop_table_insert = """INSERT INTO laptop VALUES (
                                %(name)s,
                                %(brand)s,
                                %(sku)s,
                                %(price)s,
                                %(url)s,
                                %(availability)s,
                                %(review_num)s)
                            ON CONFLICT (sku)
                            DO UPDATE SET name = excluded.name,
                                          brand = exluded.brand,
                                          price = excluded.price,
                                          url = excluded.url,
                                          availability = excluded.availability,
                                          review_num = excluded.review_num;
                      """


# query list
drop_table_queries = [laptop_table_drop]
create_table_queries = [laptop_table_create]