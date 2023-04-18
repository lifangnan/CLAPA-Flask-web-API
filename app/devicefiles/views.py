import os
import shutil
import zipfile
from flask import Blueprint, Response, jsonify, make_response, request, current_app, send_file
from pymongo import MongoClient
from functools import wraps
import json
import hmac
import datetime
from dateutil.parser import parse
from bson.objectid import ObjectId
from . import devicefiles_blue

from ExperimentalDataSave import ExperimentalDataBase

DataBase = ExperimentalDataBase(_mongoclient_address = "mongodb://222.29.111.164:27017/", \
                                        _minioclient_address = "222.29.111.164:9000", \
                                        _access_key = "laser", _secret_key = "laserplasma")


# 从mongoDB获取特定相机的所有图片的文件名和url
@devicefiles_blue.route("/getFilenamesAndUrls", methods = ['GET'])
def getFilenamesAndUrls():
    cameraName = request.args.get('cameraName')
    startTime = request.args.get('startTime')
    endTime = request.args.get('endTime')
    if startTime:
        startTime = datetime.strptime(startTime,'%Y-%m-%dT%H:%M:%S.%fZ')
    if endTime:
        endTime = datetime.strptime(endTime,'%Y-%m-%dT%H:%M:%S.%fZ')
    filenames = []
    srcList = []

    imgs_dict = DataBase.getCameraImgs(cameraName)
    for key, img in imgs_dict.items():
        img_datatime = img['Datetime'].strptime(startTime,'%Y-%m-%dT%H:%M:%S.%fZ')
        if img_datatime >= startTime and img_datatime <= endTime:
            filenames.append(key)
            srcList.append("?cameraName=%s&objectname=%s"%(cameraName, key))

        res = make_response(jsonify({"code": 20000, "data": {'filenames': filenames, 'srcList': srcList}}))
        res.status = '200'
        return res
    
# 根据mongoDB的id获取图片
@devicefiles_blue.route("/getOneImg", methods = ['GET'])
def getOneImg():
    cameraName = request.args.get('cameraName')
    objectName = request.args.get('objectname')

    temp_filename = './temp_files/imgs/%s_%s.jpeg'%(cameraName, objectName)

    if os.path.exists(temp_filename):
        return send_file(temp_filename)
    
    with open(temp_filename, 'wb') as f:
        img_bytes = DataBase.getOneObjectFromMinio(objectName)
        f.write(img_bytes)
    return send_file(temp_filename)
    

# 打包下载
@devicefiles_blue.route("/downloadAllImages", methods=['GET'])
def downloadAllImages():
    cameraName = request.args.get('cameraName')
    id_list = []
    filename_list = []

    imgs_dict = DataBase.getCameraImgs(cameraName)

    file_dir = "./temp_files/zip_files/experiment_data"
    if os.path.exists(file_dir) == False:
        os.mkdir(file_dir)
    if os.path.exists(file_dir + '/' + cameraName) == False:
        os.mkdir(file_dir + '/' + cameraName)

    for objectName in imgs_dict.keys():
        temp_filename = file_dir + '/' + cameraName + '/' + objectName
        with open(temp_filename, 'wb') as f:
            img_bytes = DataBase.getOneObjectFromMinio(objectName)
            f.write(img_bytes)

    # 压缩从服务器端临时保存的文件
    with zipfile.ZipFile('./temp_files/zip_files/temporary_file.zip', mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        for ev_name in os.listdir(file_dir):
            if len( os.listdir(os.path.join(file_dir, ev_name)) ) == 0:
                zf.write(os.path.join(file_dir, ev_name), arcname = ev_name)
            for file_name in os.listdir(os.path.join(file_dir, ev_name)):
                zf.write(os.path.join(file_dir, ev_name, file_name), arcname = os.path.join(ev_name, file_name))

    shutil.rmtree('./temp_files/zip_files/experiment_data') # 递归删除非空文件夹
    # return send_file(open('./temp_files/zip_files/temporary_file.zip', 'rb'), mimetype = 'application/zip', as_attachment=True, attachment_filename='experiment_images.zip')

    def send_file():
        with open('./temp_files/zip_files/temporary_file.zip', 'rb') as targetfile:
            while 1:
                data = targetfile.read(10 * 1024 * 1024)   # 每次读取20M
                if not data:
                    break
                yield data

    response = Response(send_file(), content_type='application/octet-stream')
    response.headers["Content-disposition"] = 'attachment; filename=experiment_images.zip'
    return response


