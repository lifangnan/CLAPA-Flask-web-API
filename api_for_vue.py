from cachetools import Cache
from flask import Flask, request, jsonify, make_response, send_file, render_template, Response, send_from_directory
from flask_cors import CORS
import pymongo
from bson.objectid import ObjectId
import pickle
from PIL import Image

import magic
import os
import numpy as np
import pandas as pd
import re
from CaChannel import ca, CaChannel, CaChannelException

from urllib import parse

from read_pv_from_archiver import get_secs_val, get_pv_csv
# import datetime
from datetime import datetime
import time

import shutil # 用于递归删除非空文件夹  
import zipfile

db_client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
# db_client = pymongo.MongoClient()
db = db_client.clapa_test
db_img = db_client.clapa9


channels_dict = dict()
channels_dict['IT:PSQ1:GetCurrent'] = CaChannel("IT:PSQ1:GetCurrent")
channels_dict['IT:PSQ2:GetCurrent'] = CaChannel("IT:PSQ2:GetCurrent")
channels_dict['IT:PSQ3:GetCurrent'] = CaChannel("IT:PSQ3:GetCurrent")
channels_dict['IT:PSQ4:GetCurrent'] = CaChannel("IT:PSQ4:GetCurrent")
channels_dict['IT:PSQ5:GetCurrent'] = CaChannel("IT:PSQ5:GetCurrent")



app = Flask(__name__, static_folder="./dist", template_folder="./dist", static_url_path="")
CORS(app, supports_credentials=True)

@app.route("/")
def main_page():
    return render_template('index.html')

# 与登陆相关
@app.route("/user/login", methods = ['POST'])
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

@app.route("/user/info", methods = ['GET', 'POST'])
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

# api1: 折线图初始化阶段。提供：PV名，读回：1个PV值数据
@app.route("/getdata48Hours", methods = ['GET'])
def getdata48Hours():
    pvName = request.args.get('pvName')
    # print(pvName)

    if pvName in channels_dict.keys():
        temp_channel = channels_dict[pvName]
    else:
        temp_channel = CaChannel("pvName")
        channels_dict[pvName] = temp_channel
    
    current_value = None
    try:
        temp_channel.searchw()
        current_value = temp_channel.getw()
    except CaChannelException as e:
        print(e)

    # for test
    rt_list = []
    now = datetime.now()
    rt_list.append({'name': now.strftime('%Y/%m/%d %H:%M:%S'), 'value':[ now.strftime('%Y/%m/%d %H:%M:%S'), current_value]})

    res = make_response(jsonify({"code": 20000, "data": rt_list}))
    res.status = '200'

    return res


# api2: 折线图更新阶段（1秒更新一次）。提供：PV名，读回：一个时间戳-PV值对。目前直接用caget实现
@app.route("/getOneCurrentData", methods = ['GET'])
def getOneCurrentData():
    pvName = request.args.get('pvName')

    if pvName in channels_dict.keys():
        temp_channel = channels_dict[pvName]
    else:
        temp_channel = CaChannel("pvName")
        channels_dict[pvName] = temp_channel
    
    current_value = None
    try:
        # temp_channel.searchw()
        current_value = temp_channel.getw()
    except CaChannelException as e:
        print(pvName, e)

    now = datetime.now()
    res = make_response(jsonify({"code": 20000, "data": {'name': now.strftime('%Y/%m/%d %H:%M:%S'), 'value':[ now.strftime('%Y/%m/%d %H:%M:%S'), current_value]}}))
    res.status = '200'
    return res


# api3: 获取特定一段时间的一个PV的Archiver存储值
@app.route("/getArchiverData", methods = ['GET'])
def getArchiverData():
    pvName = request.args.get('pvName')
    startTime = request.args.get('startTime')
    endTime = request.args.get('endTime')
    if pvName and startTime and endTime:
        startTime = datetime.strptime(startTime,'%Y-%m-%dT%H:%M:%S.%fZ')
        endTime = datetime.strptime(endTime,'%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        res = make_response()
        res.status = '400'
        return res

    archiver_daraframe = get_pv_csv(pvName, startTime, endTime)
    archiver_daraframe = archiver_daraframe[(archiver_daraframe.index >= startTime)&(archiver_daraframe.index <= endTime)]
    vals = archiver_daraframe['val']
    val_range = vals.max() - vals.min()
    diff_ratio = 0.02
    pick_head_end = [False for i in range(len(vals))]
    pick_head_end[0] = True
    pick_head_end[-1] = True
    archiver_daraframe = archiver_daraframe[(vals.diff() > val_range*diff_ratio)|(vals.diff(-1) > val_range*diff_ratio)|(pick_head_end)]

    # if len(archiver_daraframe) > 200:
    #     archiver_daraframe = archiver_daraframe.sample(200).sort_index()

    rt_list = []


    vals = np.array(archiver_daraframe.val)
    times = np.array(archiver_daraframe['secs']+archiver_daraframe['nanos']*0.000000001)

    for i in range(len(vals)):
        temp_time = datetime.fromtimestamp(times[i])
        temp_value = vals[i]
        rt_list.append({'name': temp_time.strftime('%Y/%m/%d %H:%M:%S.%f'), 'value':[ temp_time.strftime('%Y/%m/%d %H:%M:%S.%f'), temp_value]})

    
    # Archiver_data = get_secs_val(pvName, startTime, endTime)
    # rt_list = []

    # sample_freq = 1
    # if len(Archiver_data) > 100:
    #     sample_freq = len(Archiver_data)/1000

    # for i in range(0, len(Archiver_data), int(sample_freq)):
    #     item = Archiver_data[i]
    #     temp_time = datetime.fromtimestamp(item['secs']+item['nanos']*0.000000001)
    #     temp_value = item['val']
    #     if temp_time >= startTime and temp_time <= endTime:
    #         rt_list.append({'name': temp_time.strftime('%Y/%m/%d %H:%M:%S.%f'), 'value':[ temp_time.strftime('%Y/%m/%d %H:%M:%S.%f'), temp_value]})

    res = make_response(jsonify({"code": 20000, "data": rt_list}))
    res.status = '200'

    return res


# api4: 下载特定一段时间的一个PV的Archiver存储值，csv文件
@app.route("/getArchiverDataCSV", methods = ['GET'])
def getArchiverDataCSV():
    pvName_list = request.args.get('pvName_list').split(',')
    startTime = request.args.get('startTime')
    endTime = request.args.get('endTime')

    startTime = datetime.strptime(startTime,'%Y-%m-%dT%H:%M:%S.%fZ')
    endTime = datetime.strptime(endTime,'%Y-%m-%dT%H:%M:%S.%fZ')

    archiver_daraframe = pd.DataFrame()

    for pvName in pvName_list:
        new_daraframe = get_pv_csv(pvName, startTime, endTime)
        new_daraframe = new_daraframe[(new_daraframe.index >= startTime) & (new_daraframe.index <= endTime)]
        new_daraframe.rename(columns={'val':pvName}, inplace=True)
        if archiver_daraframe.empty:
            archiver_daraframe = new_daraframe[pvName]
        else:
            archiver_daraframe = pd.merge(archiver_daraframe, new_daraframe[pvName], how="outer", left_index=True, right_index=True)

    print(archiver_daraframe.shape)
    archiver_daraframe['datetime'] = pd.to_datetime(archiver_daraframe.index, format='%Y-%m-%d %H:%M:%S.%f')
    archiver_daraframe['datetime'] = archiver_daraframe['datetime'].apply(lambda x: str(x))
    file_path = "./temp_files/Archiver_Data.csv"
    archiver_daraframe.to_csv(file_path)

    def send_file():
        with open('./temp_files/Archiver_Data.csv', 'rb') as targetfile:
            while 1:
                data = targetfile.read(10 * 1024 * 1024)   # 每次读取10M
                if not data:
                    break
                yield data

    response = Response(send_file(), content_type='application/octet-stream')
    response.headers["Content-disposition"] = 'attachment; filename=Archiver_Data_%s_%s.csv'%(startTime.strftime('%Y%m%d'), endTime.strftime('%Y%m%d'))
    return response

    # return send_file("./temp_files/Archiver_Data.csv", as_attachment=True, attachment_filename="Archiver_Data.csv")


# 从mongoDB获取特定相机的所有图片的文件名和url
@app.route("/getFilenamesAndUrls", methods = ['GET'])
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

    if cameraName:
        all_files = db_img[cameraName].find({}, {'_id':1, 'filename':1, 'TimeStamp':1})
        for item in all_files:
            if startTime and endTime:
                if 'TimeStamp' in item.keys():
                    TimeStamp = datetime.strptime(item['TimeStamp'], '%Y-%m-%d %H:%M:%S.%f')
                    if TimeStamp >= startTime and TimeStamp <= endTime:
                        filenames.append(item['filename'])
                        srcList.append("?cameraName=%s&id=%s"%(cameraName, item['_id']))
            else:
                filenames.append(item['filename'])
                srcList.append("?cameraName=%s&id=%s"%(cameraName, item['_id']))

        res = make_response(jsonify({"code": 20000, "data": {'filenames': filenames, 'srcList': srcList}}))
        res.status = '200'
        return res



# 根据mongoDB的id获取图片
@app.route("/getOneImg", methods = ['GET'])
def getOneImg():
    cameraName = request.args.get('cameraName')
    mongoDB_id = request.args.get('id')

    temp_filename = './temp_files/imgs/%s_%s.jpeg'%(cameraName, mongoDB_id)

    if os.path.exists(temp_filename):
        return send_file(temp_filename)

    collection = db_img[cameraName]
    img_dict = collection.find_one(ObjectId(mongoDB_id))
    pixelx = img_dict['pixelx']
    pixely = img_dict['pixely']
    img = pickle.loads(img_dict['data']).reshape(pixelx, pixely)
    outputImg = Image.fromarray( img, 'L')
    # 储存代价可能比较大
    outputImg.save(temp_filename, 'JPEG', quality = 75, subsampling = 0)

    return send_file(temp_filename)


# 打包下载
@app.route("/downloadAllImages", methods=['GET'])
def downloadAllImages():
    cameraName = request.args.get('cameraName')
    id_list = []
    filename_list = []

    # if cameraName:
    all_files = db_img[cameraName].find({}, {'_id':1})
    for item in all_files:
        id_list.append(item['_id'])


    file_dir = "./temp_files/zip_files/experiment_data"
    if os.path.exists(file_dir) == False:
        os.mkdir(file_dir)
    if os.path.exists(file_dir + '/' + cameraName) == False:
        os.mkdir(file_dir + '/' + cameraName)
    
    collection = db_img[cameraName]
    for img_id in id_list:
        img_dict = collection.find_one(ObjectId(img_id))
        filename = img_dict['filename']
        if os.path.exists(file_dir + '/andor1/'+ filename[:-4] + '.jpeg'):
            continue
        pixelx = img_dict['pixelx']
        pixely = img_dict['pixely']
        img = pickle.loads(img_dict['data']).reshape(pixelx, pixely)
        outputImg = Image.fromarray( img, 'L')
        # 储存代价可能比较大
        outputImg.save(file_dir + '/andor1/'+ filename[:-4] + '.jpeg', 'JPEG', quality = 100, subsampling = 0)

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









# api4-1: 从mongoDB获取发次index。提供：日期（多个日期列表），读回：日期下所有发次index

# 测试api：获取所有andor相机下的图片的url
@app.route("/getImgsUrl", methods = ['GET'])
def getImgsUrl():
    all_files = db_img['andor1'].find({}, {'_id':1, 'filename':1})
    filenames = []
    id_lst = []
    for item in all_files:
        id_lst.append(item['_id'])
        filenames.append(item['filename'])
    return jsonify({})

# api4-2：获取图片文件。提供：url，读回：一张图片
@app.route("/getImg", methods = ['GET'])
def getImg():
    img_id = request.args.get('img_id')
    collection = db_img["andor1"]
    img_dict = collection.find_one(ObjectId(img_id))
    pixelx = img_dict['pixelx']
    pixely = img_dict['pixely']
    img = pickle.loads(img_dict['data']).reshape(pixelx, pixely)
    outputImg = Image.fromarray( img, 'L')
    # 储存代价可能比较大
    outputImg.save('./static/temp_img.jpeg', 'JPEG', quality = 75, subsampling = 0)

    return send_file('./static/temp_img.jpeg')


@app.route("/get_binary_images/<source>/<id>/<if_hd>")
def get_one_image(source, id, if_hd):
    # all_files = db_img[source].find({}, {'_id':1, 'filename':1})
    # filenames = []
    # for item in all_files:
    #     filenames.append({'filename': item['filename'], '_id': item['_id']})
    collection = db_img[source]
    img_dict = collection.find_one(ObjectId(id))
    pixelx = img_dict['pixelx']
    pixely = img_dict['pixely']
    img = pickle.loads(img_dict['data']).reshape(pixelx, pixely)
    outputImg = Image.fromarray( img, 'L')
    if(if_hd != 'true'): # 是否是高清图
        outputImg = outputImg.resize( (int(200*pixelx/pixely), 200) )

    # 储存代价可能比较大
    home_app_dir = './'
    outputImg.save(home_app_dir+'/static/temp_img.jpeg', 'JPEG', quality = 75, subsampling = 0)

    # 对于图片文件，可以不判断mime类型
    # with open(home_app_dir + '/static/temp_img.tiff', 'rb') as f:
    #     MIME_type = magic.from_buffer(f.read(), mime=True)
    return send_file(home_app_dir + '/static/temp_img.jpeg')


if __name__ == '__main__':
    # 直接用python运行flask服务
    # app.run(port='5055')
    
    app.run(debug=True, port='5055')