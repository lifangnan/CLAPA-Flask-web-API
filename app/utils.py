from flask import Blueprint, jsonify, request, current_app
from pymongo import MongoClient
from flask_jwt import JWT, jwt_required, current_identity
from functools import wraps     # 导入 wraps 函数
import json
import hmac
import datetime


client = MongoClient(current_app.config['MONGO_URL'])

str_to_bytes = lambda s: s.encode("utf-8") if isinstance(s, str) else s
safe_str_cmp = lambda a, b: hmac.compare_digest(str_to_bytes(a), str_to_bytes(b))


# 访问控制
class User:
    def __init__(self, id, username, password, database=None):
        self.id = id
        self.username = username
        self.password = password
        self.database = database

    def __str__(self):
        return f"User(id='{self.id}', username='{self.username}', database='{self.database}')"

# 认证函数
def authenticate(username, password):
    user = client[current_app.config['MONGO_USERDB']][current_app.config['MONGO_USERCOL']].find_one({'username': username})
    if user and safe_str_cmp(user.get('password').encode('utf-8'), password.encode('utf-8')):
        return User(id=str(user['id']), username=user['username'], password=user['password'], database=user.get('database'))

# 身份标识函数
def identity(payload):
    user_id = payload['identity']
    user = client[current_app.config['MONGO_USERDB']][current_app.config['MONGO_USERCOL']].find_one({'id': int(user_id)})
    if user:
        return User(id=str(user['id']), username=user['username'], password=user['password'], database=user.get('database'))

# JWT 配置
