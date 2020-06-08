import pytest
import requests
import json
import random
import config



def test_create_collection():
    url = "http://" + ROUTER + "/command"
    headers = {"content-type": "application/json"}
    data = {
        "target": [PS1, PS2],
        "method": "file_info",
        "path": PATH
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    assert response.status_code == 200

    pss = json.loads(response.text)

    s1 = set()
    s2 = set()
    for o in pss[0]["result"]:
        s1.add(json.dumps(o))
    
    for o in pss[1]["result"]:
        s2.add(json.dumps(o))

    print("------------------")

    for s in s1-s2:
        s = json.loads(s)
        print("s1-- ", s["path"])
