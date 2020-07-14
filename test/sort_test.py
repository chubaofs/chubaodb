import pytest
import requests
import json
import random
import config
import time


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
            {"text": {"name": "content",  "value": True}},
            {"date": {"name": "birthday",  "value": True, "format": "%Y-%m-%d"}}
        ]
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    time.sleep(5)  # TODO: FIX ME wait raft ok


def test_put():
    url = "http://" + config.ROUTER + "/put/t1/1"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj", "sun"],
        "age": 35,
        "content": "hello tig",
        "price": 12.5,
        "birthday":  "2016-06-07",
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("doc put ---\n" + response.text)
    assert response.status_code == 200

    url = "http://" + config.ROUTER + "/put/t1/2"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj1", "sun"],
        "age": 36,
        "content": "hello tig1",
        "price": 20.12,
        "birthday":  "2016-07-07",
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("doc put ---\n" + response.text)
    assert response.status_code == 200

    url = "http://" + config.ROUTER + "/put/t1/3"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj2", "sun"],
        "age": 37,
        "content": "hello tig11",
        "price": 50.12,
        "birthday":  "2016-08-07",
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("doc put ---\n" + response.text)
    assert response.status_code == 200

    url = "http://" + config.ROUTER + "/put/t1/4"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj4", "sun"],
        "age": 33,
        "content": "hello tig11",
        "price": 100.0,
        "birthday":  "2016-06-08",
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("doc put ---\n" + response.text)
    assert response.status_code == 200

    url = "http://" + config.ROUTER + "/put/t1/5"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansjs4", "sun"],
        "age": 80,
        "content": "hello tig11x",
        "price": 100.0,
        "birthday":  "2016-06-17",
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("doc put ---\n" + response.text)
    assert response.status_code == 200

    url = "http://" + config.ROUTER + "/put/t1/5"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj", "sun"],
        "age": 72,
        "content": "hello tig11x",
        "price": 100.0,
        "birthday":  "0206-06-07",
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("doc put ---\n" + response.text)
    assert response.status_code == 200


def test_search():
    time.sleep(5)
    response = requests.get(
        "http://"+config.ROUTER+"/search/t1?query=hello%20tig&size=10&def_fields=content&sort=age:desc")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["hits"][0]["doc"]["_source"]["age"] == 72

    response = requests.get(
        "http://"+config.ROUTER+"/search/t1?query=hello%20tig&size=10&def_fields=content&sort=age:asc")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["hits"][0]["doc"]["_source"]["age"] == 33

    response = requests.get(
        "http://"+config.ROUTER+"/search/t1?query=hello%20tig&size=10&def_fields=content&sort=price:desc|age:asc")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["hits"][0]["doc"]["_source"]["age"] == 33
    assert v["hits"][0]["doc"]["_source"]["price"] == 100
    assert v["hits"][1]["doc"]["_source"]["age"] == 72
    assert v["hits"][1]["doc"]["_source"]["price"] == 100

    response = requests.get(
        "http://"+config.ROUTER+"/search/t1?query=hello%20tig&size=10&def_fields=content&sort=price:desc|age:desc")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["hits"][0]["doc"]["_source"]["age"] == 72
    assert v["hits"][0]["doc"]["_source"]["price"] == 100
    assert v["hits"][1]["doc"]["_source"]["age"] == 33
    assert v["hits"][1]["doc"]["_source"]["price"] == 100

    response = requests.get(
        "http://"+config.ROUTER+"/search/t1?sort=birthday:asc")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["hits"][0]["doc"]["_source"]["birthday"] == "0206-06-07"
    assert v["hits"][1]["doc"]["_source"]["birthday"] == "2016-06-07"
    assert v["hits"][2]["doc"]["_source"]["birthday"] == "2016-06-08"
    assert v["hits"][3]["doc"]["_source"]["birthday"] == "2016-07-07"
