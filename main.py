import os
import pandas as pd
from pathlib import Path
import json
import gzip
import database
import logging
import time


#Setting up required directories for file movement and root traversal
rootdir = "./test-data/test-data/"
final_file_dir = "./final/"


#Setting up logger functionality
logging.basicConfig()
logger = logging.getLogger('ETL Application')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def main():
    # Setting up required variables for log statistics and file traversal
    filelist = []
    extract_start = time.time()

    # File traversal
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            filepath = os.path.join(subdir,file)
            filelist.append(filepath)

            logger.info(f'Processing {filepath}')

            customers = []
            products = []
            transactions = []
            erasures = []

            #Loading contents of each file into relevant list datatype
            try:
                with gzip.open(filepath, "r") as f:
                    for line in f:
                        if file == 'customers.json.gz':
                            customers.append(json.loads(line))
                        elif file == 'products.json.gz':
                            products.append(json.loads(line))
                        elif file == 'transactions.json.gz':
                            transactions.append(json.loads(line))
                        elif file == 'erasure-requests.json.gz':
                            erasures.append(json.loads(line))
                        else:
                            raise Exception(f"Unexpected file : {filepath}")
            except:
                print(f"File not zipped as expected")
                continue

            #Logging extrcat end time
            extract_end = time.time() - extract_start
            logger.info(f'Extract took {extract_end} seconds')

            # Individual file count statistics
            row_count = 0
            if customers:
                logger.info(f'Attempting to load {row_count} to {row_count + len(customers)}')
            elif products:
                logger.info(f'Attempting to load {row_count} to {row_count + len(products)}')   
            elif transactions:
                logger.info(f'Attempting to load {row_count} to {row_count + len(transactions)}') 
            elif erasures:
                logger.info(f'Attempting to load {row_count} to {row_count + len(erasures)}') 
            else:
                raise Exception(f"No data available to load: {filepath}")

            # Looping through list and calling DB insert functions, should of done this via a df instead of RBAR
            for customer in customers:
                database.insert_customers(customer)
            for product in products:
                database.insert_products(product)
            for transaction in transactions:
                database.insert_transactions(transaction) 
            for erasure in erasures:
                database.insert_erasures(erasure)    

            #move_files(filepath)

    #Create final dataset
    customer_df = database.create_final_dataset(database.SELECT_ALL_CUSTOMERS)
    product_df = database.create_final_dataset(database.SELECT_ALL_PRODUCTS)
    transactions_df = database.create_final_dataset(database.SELECT_ALL_TRANSACTIONS)
    exclusions_df = database.create_final_dataset(database.SELECT_ALL_ERASURES)

    move_files(customer_df, final_file_dir, 'customers.csv')
    move_files(product_df, final_file_dir, 'products.csv')
    move_files(transactions_df, final_file_dir, 'transactions.csv')
    move_files(exclusions_df, final_file_dir, 'exclusions.csv')

def move_files(df , filedirpath, filename):
    """Create csv from dataframe"""
    filepath = os.path.join(filedirpath,filename)
    os.makedirs(os.path.dirname(filedirpath), exist_ok=True)
    df.to_csv(filepath, index=False)

if __name__ == "__main__":
    database.create_tables()
    main()
