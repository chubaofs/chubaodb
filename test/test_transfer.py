# -*- coding:utf-8 -*-

import logging
import pytest
import requests
import json
import random
import config
from config import *


def test_del_collection():
    url = "http://" + MASTER + "/collection/delete/t1"
    response = requests.delete(url)
    print("collection_delete---\n" + response.text)

    assert response.status_code == 200 or response.status_code == 503


def test_create_collection():
    url = "http://" + MASTER + "/collection/create"
    headers = {"content-type": "application/json"}
    data = {
        "name": "t1",
        "partition_num": 1,
        "replica_num": 1,
        "source": True,
        "fields": [{
            "name": "name",
            "field_type": "string",
            "index": False,
            "store": True,
            "array": False
        },
            {
            "name": "age",
            "field_type": "integer",
            "index": False,
            "store": True,
            "array": False
        },
            {
            "name": "content",
            "field_type": "text",
            "index": False,
            "store": True,
            "array": False
        }
        ]
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("space_create---\n" + response.text)
    assert response.status_code == 200


def test_transfer():
    response = requests.get("http://" + MASTER + "/collection/get/t1")
    assert response.status_code == 200
    value = json.loads(response.text)
    assert len(value["partitions"]) == 1
    partition_id = value["partitions"][0]

    collection_id = value["id"]

    response = requests.get("http://" + MASTER +
                            "/partition/get/"+str(collection_id)+"/"+str(partition_id))
    partition = json.loads(response.text)

    response = requests.get("http://" + MASTER + "/pserver/list")
    pss = json.loads(response.text)

    random.shuffle(pss)
    to_server = pss[0]

    if len(pss) > 1:
        for ps in pss:
            if ps["addr"] != partition["leader"]:
                to_server = ps

    response = requests.post("http://"+MASTER+"/collection/partition/transfer", headers={"content-type": "application/json"}, data=json.dumps({
        "collection_id": collection_id,
        "partition_id": partition_id,
        "to_server": to_server["addr"]
    }))

    assert response.status_code == 200


def test_put():
    id = random.randint(1, 100000000)
    response = requests.post("http://" + ROUTER + "/put/t1/"+str(id), data=json.dumps({
        "name": "ansj",
        "age": 35,
        "content": "hello tig"
    }))
    assert response.status_code == 200

    response = requests.get("http://" + ROUTER + "/get/t1/"+str(id))
    assert response.status_code == 200


def test_multiple():
    for i in range(10):
        test_transfer()
        test_put()
