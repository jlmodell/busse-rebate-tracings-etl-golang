from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd
import fire
import os
from pathlib import Path
from datetime import datetime

load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI")

client = MongoClient(MONGO_URI)
db = client.get_database("busserebatetraces")

collection = db.get_collection("data_warehouse")

def deleteMany(filter: dict):
    global collection
    results = collection.delete_many(filter)
    print(f"Deleted {results.deleted_count} documents")

def main(month: str, year: str, filepath: str):
    file = Path(filepath)
    filter = {
        "__month__": month,
        "__year__": year,
        "__file__": os.path.basename(file)
    }
    data = collection.count_documents(filter)
    if data > 0:
        print(f"Data for {month} {year} and {filter['__file__']} already exists")
        deleteMany(filter)

    data = pd.read_csv(file)
    cols = data.columns

    data.fillna("", inplace=True)
    data = data[data[cols[0]] != ""]

    data["__month__"] = f"{month}"
    data["__year__"] = f"{year}"
    data["__file__"] = os.path.basename(file)
    date = datetime(int(year), int(month), 1)
    data["__date__"] = date
        
    results = collection.insert_many(data.to_dict(orient="records"))
    print(f"Inserted {len(results.inserted_ids)} documents")


if __name__ == "__main__":
    fire.Fire(main)


    # "X:\rebate_trace_files\february 2024\completed\HENRY_ADDITIONAL_2024_02.csv"
    # "X:\rebate_trace_files\february 2024\completed\Atlantic_2024_02.csv"
    # "X:\rebate_trace_files\february 2024\completed\Twin-Med_2024_02Rebate_BUS01_020124-022924 (Submitted 3-8-24).csv"
    # "X:\rebate_trace_files\february 2024\completed\DealMed_2024_02.csv"
    # "X:\rebate_trace_files\february 2024\completed\TRI-ANIM_2024_02.csv"

    # "X:\rebate_trace_files\march 2024\completed\concordance_mms_we_20240308.csv"
    # "X:\rebate_trace_files\march 2024\completed\concordance_we_20240308.csv"
    # "X:\rebate_trace_files\march 2024\completed\MGM_CB_03092024_104331.csv"