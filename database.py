import psycopg2
# Loading Environment Variables from .env file
from dotenv import load_dotenv
import os
import pandas as pd

#Load environmental variables for DB connection
load_dotenv()


#Create queries needed for laod to DB
CREATE_CUSTOMERS_TABLE = """CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    date_of_birth DATE,
    email VARCHAR(255) NOT NULL,
    phone_number VARCHAR(75),
    address VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(75),
    postcode VARCHAR(75),
    last_change TIMESTAMP,
    segment VARCHAR(255)
);"""
CREATE_PRODUCTS_TABLE = """CREATE TABLE IF NOT EXISTS products (
    sku INTEGER PRIMARY KEY,
    name VARCHAR(255),
    price NUMERIC CHECK(price > 0),
    category TEXT,
    popularity REAL CHECK(popularity > 0)
);"""
CREATE_TRANSACTIONS_TABLE = """CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  uuid PRIMARY KEY,
    transaction_time TIMESTAMP,
    customer_id INTEGER,
    delivery_address VARCHAR(255),
    delivery_postcode VARCHAR(15),
    delivery_city VARCHAR(100),
    delivery_country VARCHAR(50),
    transaction_cost NUMERIC,
    FOREIGN KEY(customer_id) REFERENCES customers(id)
);
"""
CREATE_TRANSACTIONLINES_TABLE = """CREATE TABLE IF NOT EXISTS transactionlines (
    transaction_id  uuid NOT NULL,
    transline_no INTEGER NOT NULL,
    transline_sku INTEGER,
    transline_quantity INTEGER,
    transline_price NUMERIC,
    transline_total NUMERIC,
    FOREIGN KEY(transaction_id) REFERENCES transactions(transaction_id),
    FOREIGN KEY(transline_sku) REFERENCES products(sku),
    PRIMARY KEY (transaction_id, transline_no)
);
"""
CREATE_ERASURES_TABLE = """CREATE TABLE IF NOT EXISTS erasures (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    email VARCHAR(255),
    CONSTRAINT id_or_email_present CHECK(customer_id IS NOT NULL OR email IS NOT NULL)
);
""" 
CREATE_ERRORLOG_TABLE = """CREATE TABLE IF NOT EXISTS errorlog (
    error_id SERIAL PRIMARY KEY,
    table_name VARCHAR,
    record_id VARCHAR,
    payload VARCHAR,
    error VARCHAR
);
""" 

SELECT_ALL_CUSTOMERS = "SELECT * FROM customers;"
SELECT_ALL_PRODUCTS = "SELECT * FROM products;"
SELECT_ALL_TRANSACTIONS = """SELECT t.transaction_id, t.transaction_time, t.customer_id, t.delivery_address, 
                                t.delivery_postcode, t.delivery_city, t.delivery_country, tl.transline_sku, 
                                tl.transline_quantity, tl.transline_price, tl.transline_total,
                                t.transaction_cost 
                            FROM transactions t 
                            INNER JOIN transactionlines tl 
                                on tl.transaction_id = t.transaction_id;"""
SELECT_ALL_ERASURES = "SELECT customer_id, email FROM erasures;"
INSERT_CUSTOMERS = """INSERT INTO customers 
                        (id,first_name,last_name,date_of_birth,email,phone_number,address,city,country,postcode,last_change,segment) 
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
UPDATE_CUSTOMERS_FOR_ERASURE = """UPDATE customers
                                SET first_name = digest(first_name, 'sha256'),
                                    last_name = digest(last_name, 'sha256'),
                                    date_of_birth = '0001-01-01',
                                    email = digest(email, 'sha256'),
                                    phone_number = digest(phone_number, 'sha256'),
                                    address = digest(address, 'sha256'),
                                    postcode = digest(postcode, 'sha256')
                                WHERE id = %s
                                OR email = %s;
                                """
INSERT_PRODUCTS = """INSERT INTO products 
                    (sku, name, price, category, popularity)
                    VALUES(%s,%s,%s,%s,%s)
                    ON CONFLICT(sku)
                    DO UPDATE SET name = %s, price = %s, category = %s, popularity = %s;"""
INSERT_TRANSACTIONS= """INSERT INTO transactions
                        (transaction_id, transaction_time, customer_id, delivery_address, 
                        delivery_postcode, delivery_city, delivery_country, transaction_cost)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s);"""
INSERT_TRANSACTIONLINES= """INSERT INTO transactionlines
                        (transaction_id, transline_no, transline_sku, 
                        transline_quantity, transline_price, transline_total)
                        VALUES(%s,%s,%s,%s,%s,%s);"""
INSERT_ERASURES = """INSERT INTO erasures(customer_id, email) VALUES(%s,%s);"""
INSERT_ERRORLOG = """INSERT INTO errorlog(table_name, record_id,payload, error) VALUES(%s,%s,%s,%s);"""
SEARCH_CUSTOMERS_FOR_ERASURE =  """
                                SELECT id as customer_id, email FROM customers
                                WHERE id = %s
                                OR email = %s;
                                """


#Attempt to connect to PostgresDb 
try: 
    HOST = os.environ.get("POSTGRES_HOST")
    DATABASE = os.environ.get("POSTGRES_DATABASE")
    UID = os.environ.get("POSTGRES_UID")
    PWD = os.environ.get("POSTGRES_PASSWORD")
except Exception as e:
    print(e)
    raise Exception('Failed to get environment variables')


print(os.environ.get("POSTGRES_PASSWORD"))

connection = psycopg2.connect(f"host={HOST} dbname={DATABASE} user={UID} password={PWD}")

def create_tables():
    """Create tables needed"""
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_CUSTOMERS_TABLE)
            cursor.execute(CREATE_PRODUCTS_TABLE)
            cursor.execute(CREATE_TRANSACTIONS_TABLE)
            cursor.execute(CREATE_ERASURES_TABLE)
            cursor.execute(CREATE_TRANSACTIONLINES_TABLE)
            cursor.execute(CREATE_ERRORLOG_TABLE)

def insert_customers(customer):
    """Function for customer insert"""
    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(INSERT_CUSTOMERS,
                            (customer["id"],customer["first_name"],customer["last_name"], customer["date_of_birth"]
                                , customer["email"], customer["phone_number"], customer["address"]
                                , customer["city"], customer["country"], customer["postcode"], customer["last_change"], customer["segment"]))
            except Exception as e:
                if connection:
                    connection.rollback()
                    with connection.cursor() as cursor:
                        try:
                            cursor.execute(INSERT_ERRORLOG, ('Customer', str(customer["id"]), str(customer), str(e)))
                        except Exception as e:
                            print(e)


def insert_products(product):
    """Function for product upsert"""
    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(INSERT_PRODUCTS,
                           (product["sku"], product["name"], product["price"], product["category"], product["popularity"], product["name"], product["price"], product["category"], product["popularity"])) 
            except Exception as e:
                if connection:
                    connection.rollback()
                    with connection.cursor() as cursor:
                        try:
                            cursor.execute(INSERT_ERRORLOG, ('Product', str(product["sku"]), str(product), str(e)))
                        except Exception as e:
                            print(e)

def insert_transactions(transaction):
    """Function for transaction insert, normalised with with transactionline table insert if constraints are met."""
    orderline_sum = 0.0
    transaction_orderline = []

    #Pull out the nested orderline rows
    for line_no, orderline in enumerate(transaction["purchases"]["products"],start=1):
        transaction_orderline.append({
            "transaction_id" : transaction["transaction_id"],
            "trans_line_no" : line_no,
            "trans_line_sku" :  orderline["sku"],
            "trans_line_quantity" : orderline["quanitity"],
            "trans_line_price" : orderline["price"],
            "trans_line_total" : orderline["total"]
        })
        #Get sum of orderlines
        orderline_sum += float(orderline["total"])
    
    #Compare orderline sum to transaction total, if match, insert transaction
    if round(orderline_sum,2) == float(transaction["purchases"]["total_cost"]):
        try:
            with connection:
                with connection.cursor() as cursor:
                        cursor.execute(INSERT_TRANSACTIONS,
                                (transaction["transaction_id"], transaction["transaction_time"], transaction["customer_id"], 
                                    transaction["delivery_address"]["address"], transaction["delivery_address"]["postcode"], transaction["delivery_address"]["city"], 
                                    transaction["delivery_address"]["country"] , transaction["purchases"]["total_cost"])) 
                        
        except Exception as e:
                if connection:
                    connection.rollback()
                    with connection.cursor() as cursor:
                        try:
                            cursor.execute(INSERT_ERRORLOG, ('Transaction', str(transaction["transaction_id"]), str(transaction), str(e)))
                        except Exception as e:
                            print(e)

        else:
            # Insert matching orderlines, these will fail on the Db FK constraint if no match in product exists
            with connection:
                for transline in transaction_orderline:
                    with connection.cursor() as cursor:
                        try:
                            cursor.execute(INSERT_TRANSACTIONLINES,
                                    (transline["transaction_id"], transline["trans_line_no"], 
                                        transline["trans_line_sku"], transline["trans_line_quantity"], transline["trans_line_price"], 
                                        transline["trans_line_total"])) 
                        except Exception as e:
                            if connection:
                                connection.rollback()
                                with connection.cursor() as cursor:
                                    try:
                                        cursor.execute(INSERT_ERRORLOG, ('TransactionLine', str(transline["transaction_id"]), str(transline), str(e)))
                                    except Exception as e:
                                        print(e)

    else:
        with connection.cursor() as cursor:
            try:
                cursor.execute(INSERT_ERRORLOG, ('Transaction', transaction["transaction_id"]), 
                                                 str(transaction), 
                                                 str(f"""{transaction["transaction_id"]}: Sum or Orderlines {orderline_sum} is not equal to purchase cost total 
                                                     {transaction["purchases"]["total_cost"]}"""))
            except Exception as e:
                print(e)
        print(f'Excluded: {transaction["transaction_id"]}')
        print(f'{transaction["transaction_id"]}: Sum or Orderlines {orderline_sum} is not equal to purchase cost total {transaction["purchases"]["total_cost"]}')

def insert_erasures(erasure):
    """Insert into erasures table and a call to obfuscate the customer data via function update_customer_erasures()"""
    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(INSERT_ERASURES,
                           (erasure["customer-id"], erasure["email"]))
            except Exception as e:
                if connection:
                    connection.rollback()
                    with connection.cursor() as cursor:
                        try:
                            cursor.execute(INSERT_ERRORLOG, ('Erasures', str(erasure["customer-id"]), str(erasure), str(e)))
                        except Exception as e:
                            print(e)
    update_customer_erasures(erasure)

def update_customer_erasures(erasure):   
    """Obfuscate the customer data who have requested their details to be erased"""
    with connection:     
        with connection.cursor() as cursor:
            try:
                cursor.execute(SEARCH_CUSTOMERS_FOR_ERASURE, (erasure["customer-id"], erasure["email"]))
                erasure_request = cursor.fetchone()
                if erasure_request:
                    cursor.execute(UPDATE_CUSTOMERS_FOR_ERASURE, (erasure_request[0], erasure_request[1]))
                    connection.commit()
                else:
                    if connection:
                        connection.rollback()
                        with connection.cursor() as cursor:
                            try:
                                cursor.execute(INSERT_ERRORLOG, ('ErasureRequest', str(erasure_request[0]), str(erasure_request), str(e)))
                            except Exception as e:
                                print(e)
                    print("Customer not in dB")
                
                # commit the transaction
                
            except Exception as e:
                if connection:
                    connection.rollback()
        
def create_final_dataset(query):
    """Create pandas dataframe for use in the creation of final csv"""
    with connection:     
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                query_response = cursor.fetchall()
                df = pd.DataFrame(query_response, columns=[desc[0] for desc in cursor.description])
                return df
            except Exception as e:
                raise Exception('Failed to fetch data from database')




