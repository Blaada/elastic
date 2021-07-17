import requests,json
ELASTIC_URL = "http://209.188.7.148:9200/"
header = {'Content-Type': 'application/json'}



def check_if_index_exists(index):
    url = ELASTIC_URL + f"clnd_{index}"
    response = requests.get(url)
    return response.status_code

def create_index(index):
    payload = { "settings": { "index": { "number_of_shards": 1, "number_of_replicas": 0 } } }
    url = ELASTIC_URL + f"clnd_{index}"
    requests.put(url, headers=header, json=payload)

def create_payload(data, index):
    payload = []
    for item in data:
        payload.append(json.dumps({"create" : { "_index" : index, "_id" : item["_id"] } }))
        payload.append(json.dumps({ "group" : item["_source"]["node"]["group_comment_info"]["group"]["id"] }))
    return payload

def scroll_index(index, size):
    payload = { "size": size,"query": {
            "exists": {
                "field": "node.group_comment_info.group.id"
            }
        } }
    url = ELASTIC_URL + f"{index}/_search?scroll=1d"
    response = requests.post(url, headers=header, json=payload)
    data = json.loads(response.text)
    return data["hits"]["hits"],data["_scroll_id"]

def get_next_scroll(scroll_id):
    payload = { "scroll" : "1d", "scroll_id": scroll_id }
    url = ELASTIC_URL + "_search/scroll"
    response = requests.post(url, headers=header, json=payload)
    data = json.loads(response.text)
    return data["hits"]["hits"]

def bulk_create(data):
    url = ELASTIC_URL + "_bulk"
    response = requests.post(url, headers=header, data=data)
    print(response.text)

def get_data(index):
    data, scroll_id = scroll_index(index, 10)
    while len(data)>0:
        payload = "\n".join(create_payload(data, f"clnd_{index}")) + "\n"
        bulk_create(payload)
        data = get_next_scroll(scroll_id)

def clean(index):
    check = check_if_index_exists(index)
    if check == 404:
        create_index(index)
    get_data(index)


clean("boa")
        

