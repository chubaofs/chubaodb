import pytest
import requests
import json
import random
import config
import time
from sgqlc.endpoint.http import HTTPEndpoint




def test_del_collection():
    query ="""
        mutation($name:String!){
            collectionDelete(name:$name)
        }
    """
    endpoint = HTTPEndpoint(config.MASTER)
    data = endpoint(query, {"name":"t1"})
    print("del_collection---\n" + json.dumps(data))
    if len(data.get("errors",[])) > 0:
        assert "code:CollectionNotFound" in data["errors"][0]["message"]




def test_create_collection():
    query ="""
        mutation{
            collectionCreate(
                name:"t1", 
                partitionNum:1, 
                partitionReplicaNum:1
                fields:{
                string:[{name:"name", array:true }]
                int:[{name:"age"}]
                text:[{name:"content"}]
                }  
            )
        }
    """
    endpoint = HTTPEndpoint(config.MASTER)
    data = endpoint(query, {})
    print("create_collection---\n" + json.dumps(data))
    assert len(data.get("errors",[])) == 0



def test_put():
    url = config.ROUTER + "/put/t1/1"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj", "sun"],
        "age": 35,
        "content": "hello tig"
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("doc put ---\n" + response.text)
    assert response.status_code == 200

    response = requests.get(config.ROUTER+"/get/t1/1")
    print("get---" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["doc"]["_version"] == 1
    assert v["doc"]["_source"]["name"] == ["ansj", "sun"]


def test_update():
    test_put()
    url =  config.ROUTER + "/update/t1/1"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj", "ansj"],
        "age": 35,
        "content": "hello tig"
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("update---\n" + response.text)
    assert response.status_code == 200

    response = requests.get(config.ROUTER+"/get/t1/1")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["doc"]["_source"]["name"] == ["ansj", "ansj"]
    assert v["doc"]["_source"]["age"] == 35
    assert v["doc"]["_version"] == 2
    # diff update
    url = config.ROUTER + "/update/t1/1"
    headers = {"content-type": "application/json"}
    data = {
        "age": 33
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("put---" + response.text)
    assert response.status_code == 200
    # get doc
    response = requests.get(config.ROUTER+"/get/t1/1")
    print("get--" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["doc"]["_version"] == 3
    assert v["doc"]["_source"]["age"] == 33


def test_delete():
    url = config.ROUTER + "/delete/t1/1"
    print(url)
    response = requests.delete(url)
    print("delete---" + response.text)
    assert response.status_code == 200 or response.status_code == 555

    response = requests.get(config.ROUTER+"/get/t1/1")
    print("get---" + response.text)
    assert response.status_code == 555


def test_create():
    test_delete()
    # first create
    url = config.ROUTER + "/create/t1/1"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj", "sun"],
        "age": 35,
        "content": "hello tig"
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("create---\n" + response.text)
    assert response.status_code == 200

    # second create
    url = config.ROUTER + "/create/t1/1"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj", "sun"],
        "age": 35,
        "content": "hello tig"
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("create---\n" + response.text)
    assert response.status_code == 550
    # get doc
    response = requests.get(config.ROUTER+"/get/t1/1")
    print("get--" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["doc"]["_version"] == 1


def test_upsert():
    test_delete()

    ####################################
    url = config.ROUTER + "/upsert/t1/1"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj", "sun"],
        "age": 35,
        "content": "hello tig"
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("upsert---" + response.text)
    assert response.status_code == 200
    # find by id
    response = requests.get(config.ROUTER+"/get/t1/1")
    print("get---" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["doc"]["_source"]["name"] == ["ansj", "sun"]
    assert v["doc"]["_version"] == 1
    # same upsert
    url = config.ROUTER + "/upsert/t1/1"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj", "ansj"],
        "age": 35,
        "content": "hello tig"
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("upsert---" + response.text)
    assert response.status_code == 200
    # get doc
    response = requests.get(config.ROUTER+"/get/t1/1")
    print("get ---" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["doc"]["_version"] == 2
    assert v["doc"]["_source"]["name"] == ["ansj", "ansj"]

    # diff upsert
    url = config.ROUTER + "/upsert/t1/1"
    headers = {"content-type": "application/json"}
    data = {
        "name": ["ansj", "sun"],
        "age": 36
    }
    print(url + "---" + json.dumps(data))
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("put---" + response.text)
    assert response.status_code == 200
    # get doc
    response = requests.get(config.ROUTER+"/get/t1/1")
    print("get--" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["doc"]["_version"] == 3
    assert v["doc"]["_source"]["name"] == ["ansj", "sun"]
    assert v["doc"]["_source"]["age"] == 36
    assert v["doc"]["_source"]["content"] == "hello tig"


def test_search():
    time.sleep(5)
    response = requests.get(config.ROUTER+"/search/t1?query=hello%20tig&size=10&def_fields=content")
    print("space_create---\n" + response.text)
    assert response.status_code == 200
    v = json.loads(response.text)
    assert v["code"] == 200
    assert v["hits"][0]["doc"]["_source"]["name"] == ["ansj", "sun"]
