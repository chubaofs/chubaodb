import pytest
import requests
import json
import random
import config
import time
import math


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
            {"string": {"name": "name", "array": True, "value": True}},
            {"float": {"name": "price",  "value": True}},
            {"int": {"name": "age",  "value": True}},
            {"text": {"name": "content",  "value": True}}
        ]
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("space_create---\n" + response.text)
    assert response.status_code == 200


def test_put():
    for i in range(200):
        url = "http://" + config.ROUTER + "/put/t1/"+str(i)
        headers = {"content-type": "application/json"}
        data = {
            "name": ["ansj"+str(i), "sun"],
            "age": i % 17,
            "content": "hello tig "+str(i),
            "price": math.sqrt(i)
        }

        print(data)
        print(url + "---" + json.dumps(data))
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print("doc put ---\n" + response.text)
        assert response.status_code == 200


def test_agg():
    time.sleep(5)
    response = requests.get(
        "http://"+config.ROUTER+"/agg/t1?query=hello&size=10&def_fields=content&sort=key:desc")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["total"] == 200
    assert v["result"][0]["key"] == ""
    assert v["result"][0]["values"][0]["count"] == 200


def test_agg_group_term():
    time.sleep(5)
    response = requests.get(
        "http://"+config.ROUTER+"/agg/t1?group=term(age)&sort=value:asc")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["total"] == 200
    assert v["result"][0]["values"][0]["count"] == 11

    response = requests.get(
        "http://"+config.ROUTER+"/agg/t1?group=term(age)&sort=value:desc")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["total"] == 200
    assert v["result"][0]["values"][0]["count"] == 12


def test_agg_stats_term():
    time.sleep(5)
    response = requests.get(
        "http://"+config.ROUTER+"/agg/t1?group=term(age)&sort=value:desc&fun=stats(age)")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["total"] == 200
    assert v["result"][0]["values"][0]["count"] == 12
