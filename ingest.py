#!python3

import pymongo
import time
import json

if __name__ == "__main__":

    myclient = pymongo.MongoClient('mongodb+srv://ogi:ogi@cluster0.2zf4p.mongodb.net')
    mydb = myclient["digitalskola"]
    mycol = mydb["log"]
    with open('./data/logFraud.json','rb') as file:
        file = json.load(file)

    while True:
        for data in file :
            mycol.insert_one(data)
            print(f'[mongo ingest] {data}')
            time.sleep(10)