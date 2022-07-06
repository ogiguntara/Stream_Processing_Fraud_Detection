#!python3

import json
import time
import pymongo

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

if __name__ == "__main__":

    myclient = pymongo.MongoClient('mongodb+srv://ogi:ogi@cluster0.2zf4p.mongodb.net')
    mydb = myclient["digitalskola"]
    mycol = mydb["log"]
    
    producer = KafkaProducer(bootstrap_servers=['localhost'], 
                             value_serializer=json_serializer)
    
    while True:
        data=mycol.find().sort('_id',-1).limit(1)
        for x in data:
            producer.send("digitalskola-p6", str(x))
            print(str(x))
        time.sleep(10)

    
    
    
            
