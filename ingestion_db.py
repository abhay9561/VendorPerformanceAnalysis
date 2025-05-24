import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Ensure log folder exists
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(chunk, table_name, engine):
    chunk.to_sql(table_name, con=engine, if_exists='append', index=False)

def load_raw_data():
    start = time.time()

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            table_name = file[:-4]
            logging.info(f"Ingesting {file} into DB in chunks")

            chunk_count = 0
            for chunk in pd.read_csv('data/' + file, chunksize=100_000):
                ingest_db(chunk, table_name, engine)
                chunk_count += 1
                logging.info(f" - Chunk {chunk_count} ingested (shape={chunk.shape})")

    end = time.time()
    total_time = (end - start) / 60
    logging.info('-------- Ingestion Complete --------')
    logging.info(f"\nTotal Time Taken: {total_time:.2f} minutes")

if __name__ == '__main__':
    load_raw_data()
