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

    assert response.status_code == 200 or response.status_code == 503


def test_create_collection():
    url = "http://" + config.MASTER + "/collection/create"
    headers = {"content-type": "application/json"}
    data = {
        "name": "t1",
        "partition_num": 1,
        "partition_replica_num": 1,
        "fields": [
            {"string": {"name": "name", "array": True}},
            {"int": {"name": "age"}},
            {"text": {"name": "content"}}
        ]
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("space_create---\n" + response.text)
    assert response.status_code == 200


def test_put():
    url = "http://" + config.ROUTER + "/put/t1/1"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj", "sun"],
        "age": 35,
        "content": "hello tig"
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("space_create---\n" + response.text)
    assert response.status_code == 200


def test_get():
    response = requests.get("http://"+config.ROUTER+"/get/t1/1")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["doc"]["_source"]["name"] == ["ansj", "sun"]


def test_search():
    time.sleep(5)
    response = requests.get(
        "http://"+config.ROUTER+"/search/t1?query=hello%20tig&size=10&def_fields=content")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["hits"][0]["doc"]["_source"]["name"] == ["ansj", "sun"]
