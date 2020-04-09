# #!/usr/bin/env python
# # -*- coding:utf-8 -*-
 
import config
from config import *
import time
import requests
import threading
from concurrent import futures
import random
import json


 
workers = 100
mutex = threading.Lock()
session = requests.Session()
contain = {'average_cost':0,'min_cost':0,'max_cost':0,'hit_count':0}
 
 
def handle(cost):
    with mutex:
        min_cost = contain['min_cost']
        max_cost = contain['max_cost']
        hit_count = contain['hit_count']
        average_cost = contain['average_cost']
        if min_cost == 0:
            contain['min_cost'] = cost
        if min_cost > cost:
            contain['min_cost'] = cost
        if max_cost < cost:
            contain['max_cost'] = cost
        average_cost = (average_cost*hit_count + cost) / (hit_count + 1)
        hit_count +=1
        contain['average_cost'] = average_cost
        contain['hit_count'] = hit_count
        print(contain)
    
 
def post():
    while True:
        try:
            stime = time.time()
            id = random.randint(1, 1000)
            response = requests.post("http://" + ROUTER + "/put/t1/"+str(id), data=json.dumps({
                "name": "ansj",
                "age": 35,
                "content": "hello tig"
            }))
            assert response.status_code == 200
            etime = time.time()
            if id %1000==0:
                handle(float(etime-stime))
            
        except Exception as e:
            print(e)


    
def test_run():
    with futures.ThreadPoolExecutor(workers) as executor:
        for i in range(workers):
            executor.submit(post)

    time.sleep(100000)