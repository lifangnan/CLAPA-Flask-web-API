from flask import Blueprint, jsonify, request, current_app
from pymongo import MongoClient
from flask_jwt import jwt_required, current_identity
from functools import wraps     # 导入 wraps 函数
import json
import hmac
import datetime
from dateutil.parser import parse
from . import search_blue

# print(current_app.config['MONGO_URL'])

# with current_app.app_context():
client = MongoClient(current_app.config['MONGO_URL'])

def database_access(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        requested_database = request.json.get('database')
        if not requested_database in current_identity.database:
            return jsonify({'message': 'You are not authorized to access this database.'}), 401
        # 获取指定数据库的db对象
        db = client[requested_database]
        return f(db, *args, **kwargs)
    return decorated


@search_blue.route("/collection/list", methods=["GET"])
# @jwt_required()
# @database_access
#由于 @database_access 装饰器的返回值是一个函数，因此在使用时不需要加括号，只需将其放在已有装饰器列表的最后即可。在示例代码中，原始视图函数先被 @jwt_required() 装饰器修饰，然后再被 @database_access 装饰器修饰，这样就能够实现在 JWT 认证之后对数据库访问权限的检查。

def collection_list():
    # 获取查询参数和集合名称
    db_names = ['data']
    available_collections = []
    for db_name in db_names:
        collections = client[db_name].list_collection_names()
        children = [{'value': col, 'label': col} for col in collections]
        available_collections.append({'value': db_name, 'children': children,'label':db_name})
    return jsonify({"code": 20000, "data":{'collections': available_collections}})
    return jsonify({'availableCollections': available_collections})
    return jsonify({'databases':current_identity.database})
    return jsonify({'collection':db.list_collection_names()})
    requested_database = request.args.get('database')
    print(current_identity.database)
    if not current_identity.database == requested_database:
        return jsonify({'message': 'You are not authorized to access this database.'}), 401
    return jsonify({'message': 'You have access to the database.'})


# 出于安全考虑，使用 POST 而非 GET。
# @search_blue.route("/collection/info", methods=["POST"])
# # @jwt_required()
# # @database_access
# def collectio_status():
#     # 获取查询参数和集合名称
#     collection_name = request.json.get('collection_name')
#     if collection_name not in db.list_collection_names():
#         return jsonify({"message": "Collection doesn't exist."}), 404

#     # 获取一个文档以获取键列表和数据类型
#     sample_doc = db[collection_name].find_one()

#     if sample_doc is None:
#         return jsonify({"message": "Collection is empty."}), 404


#     data_types = {}
#     for key, value in sample_doc.items():
#         data_types[key] = type(value).__name__

#     # 获取已创建索引的字段

#     index_info = db[collection_name].index_information()
#     indexed_fields = []
#     for index_name, index_info in index_info.items():
#         if index_name != '_id_': # exclude default _id_ index
#             indexed_fields.extend([field[0] for field in index_info['key']])


#     # 将结果转换为JSON格式并返回
#     return jsonify({
#         "collection_name": collection_name,

#         "data_types": data_types,
#         "indexed_keys": indexed_fields
#     })
    

@search_blue.route("/collection/info", methods=["POST"])
# @jwt_required()
# @database_access
def collection_status():
    # 从POST请求中获取数据库名称和集合名称
    query_params = request.get_json()
    
    # 从查询参数中获取集合名称和查询条件
    collection_name = query_params['col'][-1]
    db_name = query_params['col'][0]

    # 获取指定数据库
    db = client[db_name]

    if collection_name not in db.list_collection_names():
        return jsonify({"message": "Collection doesn't exist."}), 404

    # 获取一个文档以获取键列表和数据类型
    sample_doc = db[collection_name].find_one()

    if sample_doc is None:
        return jsonify({"message": "Collection is empty."}), 404


    data_types = {}
    for key, value in sample_doc.items():
        data_types[key] = type(value).__name__

    # 获取已创建索引的字段
    index_info = db[collection_name].index_information()
    indexed_fields = []
    for index_name, index_info in index_info.items():
        if index_name != '_id_': # exclude default _id_ index
            indexed_fields.extend([field[0] for field in index_info['key']])


    # 将结果转换为JSON格式并返回
    return jsonify({"code": 20000, "data":{
        "database_name": db_name,
        "collection_name": collection_name,
        "data_types": data_types,
        "indexed_keys": indexed_fields
    }})
    return jsonify({
        "database_name": db_name,
        "collection_name": collection_name,
        "data_types": data_types,
        "indexed_keys": indexed_fields
    })


@search_blue.route("/collection", methods=["POST"])
# @jwt_required()
# @database_access
def search_mongodb():
    # 从查询参数中获取关键词
    # 从查询参数中获取关键词、limit和offset
    print(request.json)
    query_params = request.get_json()
    
    # 从查询参数中获取集合名称和查询条件
    collection_name = query_params['col'][-1]
    db_name = query_params['col'][0]
    db = client[db_name]
    query = query_params['query']
    projection = query_params.get("projection", {'_id':0})
    limit = int(query_params.get("limit", 20)) # 默认每页20条记录
    offset = int(query_params.get("offset", 0) if query_params.get("offset", 0) else 0) # 默认从第0条记录开始
  
    # 解析查询参数，并进行安全性验证
  

    # query, projection, limit, offset = parse_query_params(query_params)
    # collection_name = request.json.get('collection_name')
    # collection = db[collection_name]
    # query = request.json.get("query")
    # projection = request.json.get("projection")
    # limit = int(request.json.get("limit", 20)) # 默认每页10条记录
    # offset = int(request.json.get("offset", 0) if request.json.get("offset", 0)  else 0) # 默认从第0条记录开始
    # print((query, projection))
    # 在MongoDB中查找匹配的记录并只返回需要的字段，按照指定的分页条件进行分页
    # results = list(collection.find(query, projection).limit(limit).skip(offset))
    
    config = [
        {"prop": "timestamp", "type": "date-range"},
        {"prop": "recordId", "type": "select"},
        {"prop": "energy", "type": "number-range"},
        {"prop": "experimentNote", "type": "text"},
        {"prop": "pvValues", "type": "text"},
        {"prop": "imageFiles", "type": "text"}
    ]
    for field in config:
        if field["type"] == "date-range":
            prop = field["prop"]
            if prop in query:
                query[prop] = {
                    "$gte": parse(query[prop]["$gte"]),
                    "$lte": parse(query[prop]["$lte"])
                }

    
    
    pipeline = []

    # 替换，两部分替换都可以不必做
    query = {
    'start_time': {
        '$gte': (query['timestamp']['$gte']),
        '$lte': (query['timestamp']['$lte'])
    },
    'I': {
        '$gte': query['energy']['$gte'],
        '$lte': query['energy']['$lte']
    }
    }

    pipeline.append({
        '$addFields': {
            'start_time': { '$toDate': '$start_time' } # 将字符串格式的日期转换为日期类型
        }
    })
    if query:
        pipeline.append({'$match': query})
    
  
    
    if projection:
        pipeline.append({'$project': projection})
    
    pipeline.append({'$skip': offset})
    pipeline.append({'$limit': limit})
    

    print(pipeline)
    collection = db[collection_name] # 获取对应的集合
    results = list(collection.aggregate(pipeline))
    total_results = len(results)  # 先使用 results 计算出总记录数
    
    # 获取总匹配数
    total_results = collection.count_documents(query)

    # 替换，两部分替换都可以不必做
    for result in results:
        result['energy'] = result.pop('I')
        result['timestamp'] = result.pop('start_time', None)
    # 将结果转换为 JSON 格式并一起返回
    return jsonify({"code": 20000, "data":{
        "results": results,
        "metadata": {
            "total_results": total_results,
            "limit": limit,
            "offset": offset
        }
    }})

    # query['I'] = float(query['I'])


    print(query)
    pipeline = [
        {
            '$addFields': {
                'start_time': { '$toDate': '$start_time' } # 将字符串格式的日期转换为日期类型
            }
        },
        {
            '$match': query # 使用query筛选文档
        },
          {
        '$skip': offset # 跳过前 offset 个文档
        },
        {
            '$limit': limit # 返回最多 limit 个文档
        },
            {
                '$project': projection # 返回指定字段
            }
    ]

    results = list(collection.aggregate(pipeline))
    print(results)
    total_results = len(results)
    # 获取总匹配数
    total_results = collection.count_documents(query)

    # 将结果转换为JSON格式并一起返回
    return jsonify({
        "hits": results,
        "nbHits": total_results,
        "limit": limit,
        "offset": offset
    })


@search_blue.route("/collection/detail/", methods=["POST"])
@jwt_required()
@database_access
def detail_mongodb(db):
    collection_name = request.json.get('collection_name')
    id = request.json.get('id')
    collection = db[collection_name]
    # 在MongoDB中根据ID查找记录
    query = {"id":id}
    result = collection.find_one(query,{"_id":0})

    # 将结果转换为JSON格式并返回
    return jsonify(result)
