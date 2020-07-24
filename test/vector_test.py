import pytest
import requests
import json
import random
import config
import time
import numpy as np


def test_del_collection():
    url = "http://" + config.MASTER + "/collection/delete/t1"
    response = requests.delete(url)
    print("collection_delete---\n" + response.text)

    assert response.status_code == 200 or response.status_code == 555


def test_create_collection():
    url = "http://" + config.MASTER + "/collection/create"
    headers = {"content-type": "application/json"}
    data = {
        "name": "t1",
        "partition_num": 1,
        "partition_replica_num": 1,
        "fields": [
            {"string": {"name": "name"}},
            {"int": {"name": "age"}},
            {"text": {"name": "content"}},
            {
                "vector": {
                    "name": "photo",
                    "array": False,
                    "none": True,
                    "train_size": 1000,
                    "dimension": 128,
                    "description": "PCA32,IVF100,PQ8",
                    "metric_type": "L2"
                }
            }
        ]
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("space_create---\n" + response.text)
    assert response.status_code == 200


def test_put():

    headers = {"content-type": "application/json"}

    for i in range(1, 20000):
        url = "http://" + config.ROUTER + "/put/t1/"+str(i)
        print("insert value ", i)
        data = {
            "name": "name_"+str(i),
            "age": i % 100,
            "content": "hello tig "+str(i),
            "photo": np.random.rand(128).tolist()
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print("space_create---\n" + response.text)
        assert response.status_code == 200


def test_search():
    time.sleep(5)
    response = requests.get(
        "http://"+config.ROUTER+"/search/t1?query=hello%20tig&size=10&def_fields=content")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["hits"][0]["doc"]["_source"]["name"][:5] == "name_"
