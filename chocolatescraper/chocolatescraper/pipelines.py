from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import psycopg2

class PriceToUSDPipeline:

    gbpToUsdRate = 1.3

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('price'):

            #converting the price to a float
            floatPrice = float(adapter['price'])

            #converting the price from gbp to usd using our hard coded exchange rate
            adapter['price'] = floatPrice * self.gbpToUsdRate

            return item
        else:
            raise DropItem(f"Missing price in {item}")


class DuplicatesPipeline:

    def __init__(self):
        self.names_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter['name'] in self.names_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            self.names_seen.add(adapter['name'])
            return item


class SavingToPostgresPipeline(object):
    def __init__(self):
        def __init__(self):
            ## Connection Details
            hostname = 'localhost',
            username = 'postgres',
            database = 'postgres',
            password = '4360',
            port='5432'


            ## Create/Connect to database
            self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database,port=port)

            ## Create cursor, used to execute commands
            self.cur = self.connection.cursor()

            ## Create quotes table if none exists
            self.cur.execute("""
               CREATE TABLE IF NOT EXISTS chocolate_product(
                   id serial PRIMARY KEY, 
                   name varchar(200),
                   price varchar(200),
                   url text,
               )
               """)


    def process_item(self, item, spider):
        self.store_db(item)
        #we need to return the item below as scrapy expects us to!
        return item

    def store_db(self, item):
        try:
            self.curr.execute(""" insert into chocolate_products (name, price, url) values (%s, %s, %s)""", (
                item["name"],
                item["price"],
                item["url"]
            ))

        except BaseException as e:
            print(e)
            self.connection.commit()

