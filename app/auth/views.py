# auth_bp.py
from flask import Blueprint, jsonify, request, current_app
from pymongo import MongoClient
from flask_jwt import JWT, jwt_required, current_identity
from functools import wraps     # 导入 wraps 函数
import json
import hmac
import datetime
from . import auth_blue


# 可以在 Flask 应用程序的主模块中定义 Flask 的 config 对象，然后在蓝图代码中引用该对象。
# 然后，在蓝图代码中，您可以使用 Flask 的 current_app 上下文变量来访问应用程序的 config 对象。例如：
# client = MongoClient(current_app.config['MONGO_URL'])

@auth_blue.route('/register', methods=['POST'])
def register():
    # 处理注册逻辑
    pass