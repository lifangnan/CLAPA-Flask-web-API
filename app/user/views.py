
from flask import Flask, request, jsonify, make_response, send_file, render_template, Response, send_from_directory
from flask_cors import CORS
from datetime import datetime
from . import user_blue


# 与登陆相关
@user_blue.route("/login", methods = ['POST'])
def login():
    get_json = request.get_json()
    print(get_json)
    res = make_response(jsonify({"code": 20000,"data": {"token":"admin-token"}})) # 设置响应体
    res.status = '200' # 设置状态码
    # res.headers['token'] = "admin-token" # 设置响应头
    # res.headers['Access-Control-Allow-Origin'] = '*'
    # res.headers['Access-Control-Allow-Methods'] = 'GET,POST'
    # res.headers['Access-Control-Allow-Credentials'] = True
    # res.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
    # res.headers['X-Content-Type-Options'] = 'nosniff'
    res.headers['X-Powered-By'] = 'Express'
    res.headers['Content-Type'] = 'application/json; charset=utf-8'
    res.headers['Connection'] = 'keep-alive'
    res.headers['Keep-Alive'] = 'timeout=5'
    return res

@user_blue.route("/info", methods = ['GET', 'POST'])
def userInfo():
    users = {
                'admin-token': {
                    'roles': ['admin'],
                    'introduction': 'I am a super administrator',
                    'avatar': 'https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif',
                    'name': 'Super Admin'
                },
                'editor-token': {
                    'roles': ['editor'],
                    'introduction': 'I am an editor',
                    'avatar': 'https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif',
                    'name': 'Normal Editor'
                }
            }
    token = request.args.get("token")
    # print(token)
    res = make_response({"code": 20000,"data": users[token]})
    res.headers['Content-Type'] = 'application/json; charset=utf-8'
    res.headers['X-Powered-By'] = 'Express'
    
    return res
