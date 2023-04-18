from flask import Blueprint, jsonify, request, current_app
from pymongo import MongoClient
from functools import wraps
import json
import hmac
import datetime
from dateutil.parser import parse
from bson.objectid import ObjectId
from . import search_blue

MONGO_DB = current_app.config['MONGO_DB']
MONGO_META_COL = current_app.config['MONGO_META_COL']
client = MongoClient(current_app.config['MONGO_URL'])


class MongoSearch:

    def __init__(self, client, db_name=None, collection_name=None, fields=None):
        self.client = client
        self.db_name = db_name or MONGO_DB
        self.collection_name = collection_name or MONGO_META_COL
        self.db = self.client[self.db_name]
        self.col = self.db[self.collection_name]
        if fields is None:
            self.fields = [
                {"prop": "Datetime", "class": "date-range", "searchable": True, "type": "str", "label": "时间戳"},
                {"prop": "ShotNumber", "class": "number-range", "searchable": True, "type": "int", "label": "发次"},
                {"prop": "Proton_Aimed_Energy", "class": "number-range", "searchable": True, "type": "float", "label": "能量"},
                {"prop": "Owner", "class": "text", "searchable": True, "type": "str", "label": "所有者"},
                {"prop": "Comment", "class": "text", "searchable": True, "type": "str", "label": "实验备注"},
                {"prop": "PVSnapshot", "class": "dict", "searchable": False, "type": "dict", "label": "PV值"},
                {"prop": "Images", "class": "dict", "searchable": False, "type": "dict", "label": "图片文件"}
            ]
        else:
            self.fields = fields


mongo_search = MongoSearch(client, MONGO_DB, MONGO_META_COL)

# 如果只有一个 collection 则不用


@search_blue.route("/collection/list", methods=["GET"])
def list_collections():
    return jsonify({
        "code": 20000,
        "data": {
            "collections": [
                {
                    "value": "CLAPA_ExperimentalData",
                    "children": [
                        {"value": "metadata_collection",
                            "label": "metadata_collection"}
                    ],
                    "label": "CLAPA_ExperimentalData"
                }
            ]
        }
    }
    )
    mongo_search = MongoSearch(client)
    collections = mongo_search.db.list_collection_names()
    return jsonify({"code": 20000, "data": collections})

# 获取类型
# def get_fileds_type():
#     sample_doc = mongo_search.col.find_one()
#     data_types = {}
#     for key, value in sample_doc.items():
#         data_types[key] = type(value).__name__
#     print(data_types)


@search_blue.route("/collection/info", methods=["POST"])
def collection_status():

    query_params = request.get_json()
    if mongo_search.collection_name not in mongo_search.db.list_collection_names():
        return jsonify({"message": "Collection doesn't exist."}), 404

    if mongo_search.col.count_documents({}) == 0:
        return jsonify({"message": "Collection is empty."}), 404

    return jsonify({"code": 20000, "data": {
        "database_name": mongo_search.db_name,
        "collection_name": mongo_search.collection_name,
        "fields": mongo_search.fields
    }})


@search_blue.route("/collection", methods=["POST"])
def search_mongodb():
    query_params = request.get_json()

    query = query_params.get('query', {})
    projection = query_params.get("projection", {'_id': 0})
    limit = int(query_params.get("limit", 5))
    offset = int(query_params.get("offset", 0)) 
    for field in mongo_search.fields:
        if field["type"] == "date-range":
            prop = field["prop"]
            if prop in query:
                query[prop] = {
                    "$gte": parse(query[prop]["$gte"]),
                    "$lte": parse(query[prop]["$lte"])
                }

    pipeline = []
    if query:
        pipeline.append({'$match': query})

    if projection:
        pipeline.append({'$project': projection})

    pipeline.extend([
        {'$sort': {field['prop']: 1}}
        for field in mongo_search.fields if field['searchable']
    ])

    pipeline.append({'$skip': offset})
    pipeline.append({'$limit': limit})

    collection = mongo_search.db[mongo_search.collection_name]
    results = list(collection.aggregate(pipeline))

    total_results = collection.count_documents(query)

    return jsonify({"code": 20000, "data": {
        "results": results,
        "metadata": {
            "total_results": total_results,
            "limit": limit,
            "offset": offset
        }
    }})


@search_blue.route("/collection/detail/", methods=["POST"])
def detail_mongodb():
    id = request.json.get('id')
    collection = mongo_search.db[mongo_search.collection_name]
    query = {"_id": ObjectId(id)}
    result = collection.find_one(query, {"_id": 0})

    return jsonify(result)
