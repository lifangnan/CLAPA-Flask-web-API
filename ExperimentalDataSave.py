from io import BytesIO
import random
import time
from pymongo import MongoClient
from minio import Minio
import CaChannel
import datetime
import os
import numpy as np
from PIL import Image
import threading


class CAkeeper:
    def __init__(self):
        self.PV_names_list = ['13ANDOR1:cam1:ArrayCounter_RBV', 'energy', 'IT:PS:Sld01:CurrentRead', 'IT:PS:Sld02:CurrentRead', 
                             'IT:PS:Sld03:CurrentRead', 'IT:PS:QuaH01:CurrentRead',
                             'IT:PS:QuaH02:CurrentRead', 'IT:PS:CorH01:CurrentRead', 'IT:PS:BenH01:CurrentRead', 'IT:PS:QuaH03:CurrentRead',
                             'IT:PS:QuaH04:CurrentRead', 'IT:PS:BenH02:CurrentRead', 'IT:PS:CorH02:CurrentRead', 'IT:PS:QuaH05:CurrentRead',
                             'IT:PS:QuaH06:CurrentRead', 'IT:PS:Oct0x:CurrentRead', 'IT:PS:QuaH07:CurrentRead', 'IT:PS:QuaH08:CurrentRead', 
                             'IT:PS:Oct0y:CurrentRead', 'IT:PS:QuaH09:CurrentRead', 'IT:PS:QuaH10:CurrentRead', 'IT:PS:QuaH11:CurrentRead', 
                             'IT:PS:QuaV01:CurrentRead', 'IT:PS:CorV01:CurrentRead', 'IT:PS:CctV01:CurrentRead', 'IT:PS:CctV02:CurrentRead', 
                             'IT:PS:QuaV02:CurrentRead', 'IT:PS:CorV02:CurrentRead', 'IT:PS:CorV03:CurrentRead', 'IT:PS:QuaV03:CurrentRead', 
                             'IT:PS:CctV03:CurrentRead', 'IT:PS:CorV04:CurrentRead', 'IT:PS:CctV04:CurrentRead', 'IT:PS:QuaV04:CurrentRead', 
                             'IT:PS:QuaV05:CurrentRead', 'IT:PS:CctV05:CurrentRead', 'IT:PS:CorV05:CurrentRead', 'IT:PS:CctV06:CurrentRead', 
                             'IT:PS:QuaV06:CurrentRead', 'IT:PS:QuaV07:CurrentRead', 'IT:PS:QuaV08:CurrentRead']
        
        self.CA_dict = dict()
        
        for PV_name in self.PV_names_list:
            temp_channel = CaChannel.CaChannel(PV_name)
            temp_channel.search()
            self.CA_dict[PV_name] = temp_channel
            if temp_channel.state() != 2:
                print("Cannot connect to %s or timeout"%PV_name)
    
    def reconnectAllPVs(self):
        for PV_name in self.PV_names_list:
            temp_channel = CaChannel.CaChannel(PV_name)
            temp_channel.search()
            self.CA_dict[PV_name] = temp_channel
            if temp_channel.state() != 2:
                print("Cannot connect to %s or timeout"%PV_name)
    
    def addNewPVs(self, new_PV_name_list):
        self.PV_names_list.extend(new_PV_name_list)
        for PV_name in new_PV_name_list:
            temp_channel = CaChannel.CaChannel(PV_name)
            temp_channel.search()
            self.CA_dict[PV_name] = temp_channel
            if temp_channel.state() != 2:
                print("Cannot connect to %s or timeout"%PV_name)

    def replaceAllPVs(self, new_PV_name_list):
        self.PV_names_list = new_PV_name_list
        self.reconnectAllPVs()
    
    def startHeartBeat(self):
        self.alive = True
        while True:
            if not self.alive:
                break
            time.sleep(3)
            print("Beat")
            for PV_name, CA in self.CA_dict.items():
                if CA.state() != 2:
                    temp_channel = CaChannel.CaChannel(PV_name)
                    # 需要优化
                    temp_channel.search()
                    self.CA_dict[PV_name] = temp_channel
                    if temp_channel.state() != 2:
                        print("Cannot connect to %s or timeout"%PV_name) 

    
    def run(self):
        self.heartBeatThread = threading.Thread(target = self.startHeartBeat)
        self.heartBeatThread.start()
    
    def stop(self):
        self.alive = False


class Camera:
    def __init__(self, _device_name, _PV_trigger, _PV_rawdata, _PV_pixelx="", _PV_pixely="", _PV_ExposureTime=""):
        self.device_name = _device_name
        self.PV_trigger = _PV_trigger
        self.PV_pixelx = _PV_pixelx
        self.PV_pixely = _PV_pixely
        self.PV_rawdata = _PV_rawdata
        self.PV_ExposureTime = _PV_ExposureTime

        self.CA_trigger = None
        self.CA_pixelx = None
        self.CA_pixely = None
        self.CA_rawdata = None
        self.CA_ExposureTime = None

        try:
            if self.PV_trigger != "":
                self.CA_trigger = CaChannel.CaChannel(self.PV_trigger)
                self.CA_trigger.searchw()
        except:
            print("Cannot connect to some trigger PV")
        
        try:
            if self.PV_pixelx != "":
                self.CA_pixelx = CaChannel.CaChannel(self.PV_pixelx)
                self.CA_pixelx.searchw()
        except:
            print("Cannot connect to some pixelx PV")

        try:
            if self.PV_pixely != "":
                self.CA_pixely = CaChannel.CaChannel(self.PV_pixely)
                self.CA_pixely.searchw()
        except:
            print("Cannot connect to some pixely PV")

        try:
            if self.PV_rawdata != "":
                self.CA_rawdata = CaChannel.CaChannel(self.PV_rawdata)
                self.CA_rawdata.searchw()
        except:
            print("Cannot connect to some rawdata PV")
        
        try:
            if self.PV_ExposureTime != "":
                self.CA_ExposureTime = CaChannel.CaChannel(self.PV_ExposureTime)
                self.CA_ExposureTime.searchw()
        except:
            print("Cannot connect to some ExposureTime PV")
        

    def getDeviceName(self):
        return self.device_name

    def getPixelX(self):
        return self.CA_pixelx.getValue()

    def getPixelY(self):
        return self.CA_pixely.getValue()

    def getData(self):
        return self.CA_rawdata.getw(use_numpy=True)

    def getExposureTime(self):
        return self.CA_ExposureTime.getValue()


class ExperimentalDataBase:
    def __init__(self, _mongoclient_address = "mongodb://localhost:27017/",
                 _minioclient_address = "localhost:9000",
                 _access_key = "",
                 _secret_key = ""):
        # related to experiment
        self.shot_number = 1

        # EPICS 
        self.PV_name_list = ['13ANDOR1:cam1:ArrayCounter_RBV', 'energy', 'IT:PS:Sld01:CurrentRead', 'IT:PS:Sld02:CurrentRead', 'IT:PS:Sld03:CurrentRead', 'IT:PS:QuaH01:CurrentRead',
                             'IT:PS:QuaH02:CurrentRead', 'IT:PS:CorH01:CurrentRead', 'IT:PS:BenH01:CurrentRead', 'IT:PS:QuaH03:CurrentRead',
                             'IT:PS:QuaH04:CurrentRead', 'IT:PS:BenH02:CurrentRead', 'IT:PS:CorH02:CurrentRead', 'IT:PS:QuaH05:CurrentRead',
                             'IT:PS:QuaH06:CurrentRead', 'IT:PS:Oct0x:CurrentRead', 'IT:PS:QuaH07:CurrentRead', 'IT:PS:QuaH08:CurrentRead', 
                             'IT:PS:Oct0y:CurrentRead', 'IT:PS:QuaH09:CurrentRead', 'IT:PS:QuaH10:CurrentRead', 'IT:PS:QuaH11:CurrentRead', 
                             'IT:PS:QuaV01:CurrentRead', 'IT:PS:CorV01:CurrentRead', 'IT:PS:CctV01:CurrentRead', 'IT:PS:CctV02:CurrentRead', 
                             'IT:PS:QuaV02:CurrentRead', 'IT:PS:CorV02:CurrentRead', 'IT:PS:CorV03:CurrentRead', 'IT:PS:QuaV03:CurrentRead', 
                             'IT:PS:CctV03:CurrentRead', 'IT:PS:CorV04:CurrentRead', 'IT:PS:CctV04:CurrentRead', 'IT:PS:QuaV04:CurrentRead', 
                             'IT:PS:QuaV05:CurrentRead', 'IT:PS:CctV05:CurrentRead', 'IT:PS:CorV05:CurrentRead', 'IT:PS:CctV06:CurrentRead', 
                             'IT:PS:QuaV06:CurrentRead', 'IT:PS:QuaV07:CurrentRead', 'IT:PS:QuaV08:CurrentRead']
        
        self.PV_Snapshot_list = ['IT:PS:Sld01:CurrentRead', 'IT:PS:Sld02:CurrentRead', 'IT:PS:Sld03:CurrentRead', 'IT:PS:QuaH01:CurrentRead',
                             'IT:PS:QuaH02:CurrentRead', 'IT:PS:CorH01:CurrentRead', 'IT:PS:BenH01:CurrentRead', 'IT:PS:QuaH03:CurrentRead',
                             'IT:PS:QuaH04:CurrentRead', 'IT:PS:BenH02:CurrentRead', 'IT:PS:CorH02:CurrentRead', 'IT:PS:QuaH05:CurrentRead',
                             'IT:PS:QuaH06:CurrentRead', 'IT:PS:Oct0x:CurrentRead', 'IT:PS:QuaH07:CurrentRead', 'IT:PS:QuaH08:CurrentRead', 
                             'IT:PS:Oct0y:CurrentRead', 'IT:PS:QuaH09:CurrentRead', 'IT:PS:QuaH10:CurrentRead', 'IT:PS:QuaH11:CurrentRead', 
                             'IT:PS:QuaV01:CurrentRead', 'IT:PS:CorV01:CurrentRead', 'IT:PS:CctV01:CurrentRead', 'IT:PS:CctV02:CurrentRead', 
                             'IT:PS:QuaV02:CurrentRead', 'IT:PS:CorV02:CurrentRead', 'IT:PS:CorV03:CurrentRead', 'IT:PS:QuaV03:CurrentRead', 
                             'IT:PS:CctV03:CurrentRead', 'IT:PS:CorV04:CurrentRead', 'IT:PS:CctV04:CurrentRead', 'IT:PS:QuaV04:CurrentRead', 
                             'IT:PS:QuaV05:CurrentRead', 'IT:PS:CctV05:CurrentRead', 'IT:PS:CorV05:CurrentRead', 'IT:PS:CctV06:CurrentRead', 
                             'IT:PS:QuaV06:CurrentRead', 'IT:PS:QuaV07:CurrentRead', 'IT:PS:QuaV08:CurrentRead']

        self.CA_dict = dict()
        for PV_name in self.PV_name_list:
            temp_channel = CaChannel.CaChannel(PV_name)
            temp_channel.search()
            self.CA_dict[PV_name] = temp_channel
            if temp_channel.state() != 2:
                print("Cannot connect to %s or timeout"%PV_name)
        
        self.camera_name_list = ['Andor1']
        self.camera_dict = dict()
        self.camera_dict['Andor1'] = Camera('Andor1', _PV_trigger="13ANDOR1:cam1:NumImagesCounter_RBV", _PV_rawdata="13ANDOR1:image1:ArrayData",
                                             _PV_pixelx="13ANDOR1:cam1:SizeX", _PV_pixely="13ANDOR1:cam1:SizeY", _PV_ExposureTime = "13ANDOR1:cam1:AcquireTime")

        # mongoDB
        self.mongoclient_address = _mongoclient_address
        self.mongo_dbname = "CLAPA_ExperimentalData"
        self.metadata_collection_name = "metadata_collection"
        self.devicedata_collection_name = "devicedata_collection"

        try:
            self.mongoclient = MongoClient(self.mongoclient_address)
            self.db = self.mongoclient[self.mongo_dbname]
            self.metadata_collection = self.db[self.metadata_collection_name]
            self.devicedata_collection = self.db[self.devicedata_collection_name]
            print('Connected to MongoDB')
        except Exception as e:
            print(e)
            print('Failed to connect to MongoDB')

        # minIO
        self.minioclient_address = _minioclient_address
        self.minio_bucket_name = "experimentalimagebucket"
        self.access_key = _access_key
        self.secret_key = _secret_key

        try:
            self.minio_client = Minio(
                self.minioclient_address,
                access_key = self.access_key,
                secret_key = self.secret_key,
                secure=False
            )
            print('Connected to MinIO')
        except Exception as e:
            print(e)
            print('Failed to connect to MinIO')
    
    def setMongoAddress(self, mongoAddress):
        self.mongoclient_address = mongoAddress
    def getMongoAddress(self):
        return self.mongoclient_address
    
    def setMinioAddress(self, minioAddress):
        self.minioclient_address = minioAddress
    def getMinioAddress(self):
        return self.minioclient_address
    
    def setMinioKeys(self, _access_key, _secret_key):
        self.access_key = _access_key
        self.secret_key = _secret_key
    
    def reconnectDataBases(self):
        try:
            self.mongoclient = MongoClient(self.mongoclient_address)
            self.db = self.mongoclient[self.mongo_dbname]
            self.metadata_collection = self.db[self.metadata_collection_name]
            self.devicedata_collection = self.db[self.devicedata_collection_name]
            print('Connected to MongoDB')
        except:
            print('Failed to connect to MongoDB')

        try:
            self.minio_client = Minio.client(
                self.minioclient_address,
                access_key = self.access_key,
                secret_key = self.secret_key,
                secure=False
            )
            print('Connected to MinIO')
        except:
            print('Failed to connect to MinIO')
    
    def setShotNumber(self, new_shot_number = 1):
        self.shot_number = new_shot_number

    def getPVNameList(self):
        return self.PV_name_list

    def getCameraNameList(self):
        return  self.camera_name_list

    # Methods for MinIO
    def putOneObjectToMinio(self, object_name, binaryFileStream):
        binaryFileStream.seek(0)
        filesize = len(binaryFileStream.read())
        binaryFileStream.seek(0)
        self.minio_client.put_object(self.minio_bucket_name, object_name, binaryFileStream, length=filesize)
    
    def getOneObjectFromMinio(self, object_name):
        response = self.minio_client.get_object(self.minio_bucket_name, object_name)
        bytes_file = response.read()
        return bytes_file

    # Methods for MongoDB

    # Methods for save data generated from One shot experiment.
    def insertOneShotData_Example(self, proton_aimed_energy = 5, comment = ""):
        # 发次 self.shot_number，应随实验自增1
        # 能量：从PV获取
        # proton_aimed_energy = 5
        # 时间戳：从同步系统获取
        str_datetime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # 数据所有者：来源于实验记录
        owner = "CLAPA"
        # 实验备注：来源于实验记录
        # comment = "关闭Q3"
        # PV快照：来源于EPICS控制系统
        PVsnapshot = dict()
        for PV_name in self.PV_name_list:
            PVsnapshot[PV_name] = random.randint(0, 10000)
        # 图片文件：来源于EPICS控制系统。一张图片包含字段：pixelx、pixely、minio_rawdata
        metadata_imgs = dict()
        for camera_name, camera_object in self.camera_dict.items():
            pixelx = 1024
            pixely = 1024
            # Metadata collection
            image_file_name = "%s_%s.jpeg" %(camera_name, str_datetime)
            # raw_data = camera_object.getData().reshape([pixelx, pixely])
            # raw_image = Image.fromarray(raw_data)
            # fileBytesIO = BytesIO() 
            # raw_image.save(fileBytesIO, format='PNG')
            # fileBytesIO.seek(0)
            img_names = os.listdir('./images')
            with open('images/%s' %random.choice(img_names), 'rb') as image_file:
                self.putOneObjectToMinio(image_file_name, image_file)
            metadata_imgs[image_file_name] = {"Datetime": str_datetime, "BucketName": self.minio_bucket_name, "Key": image_file_name, "PixelX": pixelx, "PixelY": pixely, "ImageType": "png"}

            # Devicedata collection
            camera_document = self.devicedata_collection.find_one({"DevieName": camera_name})
            if camera_document == None:
                camera_document = dict()
                camera_document["DevieName"] = camera_name
                camera_document["ImageType"] = "png"
                camera_document["PV_Trigger"] = camera_object.PV_trigger
                camera_document["PV_Rawdata"] = camera_object.PV_rawdata
                camera_document["PV_PixelX"] = camera_object.PV_pixelx
                camera_document["PV_PixelY"] = camera_object.PV_pixely
                camera_document["PV_ExposureTime"] = camera_object.PV_ExposureTime
                camera_document["Images"] = dict()
                camera_document["Images"][image_file_name] = {"Datetime": str_datetime, "BucketName": self.minio_bucket_name, "Key": image_file_name, "PixelX": pixelx, "PixelY": pixely, "ImageType": "png"}
                self.devicedata_collection.insert_one(camera_document)
            else:
                camera_document["Images"][image_file_name] = {"Datetime": str_datetime, "BucketName": self.minio_bucket_name, "Key": image_file_name, "PixelX": pixelx, "PixelY": pixely, "ImageType": "png"}
                self.devicedata_collection.update_one({"DevieName": camera_name}, {"$set": {"Images": camera_document["Images"]}})

        one_shot_document = dict()
        one_shot_document["ShotNumber"] = self.shot_number
        one_shot_document["Proton_Aimed_Energy"] = proton_aimed_energy
        one_shot_document["Datetime"] = str_datetime
        one_shot_document["Owner"] = owner
        one_shot_document["Comment"] = comment
        one_shot_document["PVSnapshot"] = PVsnapshot
        one_shot_document["Images"] = metadata_imgs
        self.metadata_collection.insert_one(one_shot_document)
        self.shot_number += 1
    

    def insertOneShotData(self):
        # 发次 self.shot_number，应随实验自增1
        # 能量：从PV获取
        proton_aimed_energy = self.CA_dict['energy'].getValue()
        # 时间戳：从同步系统获取
        str_datetime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # 数据所有者：来源于实验记录
        owner = "CLAPA"
        # 实验备注：来源于实验记录
        comment = ""
        # PV快照：来源于EPICS控制系统
        PVsnapshot = dict()
        for PV_name in self.PV_Snapshot_list:
            PVsnapshot[PV_name] = self.CA_dict[PV_name].getw()
        # 图片文件：来源于EPICS控制系统。一张图片包含字段：pixelx、pixely、minio_rawdata
        metadata_imgs = dict()
        for camera_name, camera_object in self.camera_dict.items():
            pixelx = camera_object.getPixelX()
            pixely = camera_object.getPixelY()
            # Metadata collection
            image_file_name = "%s_%s.jpeg" %(camera_name, str_datetime)
            raw_data = camera_object.getData().reshape([pixelx, pixely])
            raw_image = Image.fromarray(raw_data)
            fileBytesIO = BytesIO() 
            raw_image.save(fileBytesIO, format='PNG')
            fileBytesIO.seek(0)
            self.putOneObjectToMinio(image_file_name, fileBytesIO)
            metadata_imgs[image_file_name] = {"Datetime": str_datetime, "BucketName": self.minio_bucket_name, "Key": image_file_name, "PixelX": pixelx, "PixelY": pixely, "ImageType": "png"}

            # Devicedata collection
            camera_document = self.devicedata_collection.find_one({"DevieName": camera_name})
            if camera_document == None:
                camera_document = dict()
                camera_document["DevieName"] = camera_name
                camera_document["ImageType"] = "png"
                camera_document["PV_Trigger"] = camera_object.PV_trigger
                camera_document["PV_Rawdata"] = camera_object.PV_rawdata
                camera_document["PV_PixelX"] = camera_object.PV_pixelx
                camera_document["PV_PixelY"] = camera_object.PV_pixely
                camera_document["PV_ExposureTime"] = camera_object.PV_ExposureTime
                camera_document["Images"] = dict()
                camera_document["Images"][image_file_name] = {"Datetime": str_datetime, "BucketName": self.minio_bucket_name, "Key": image_file_name, "PixelX": pixelx, "PixelY": pixely, "ImageType": "png"}
                self.devicedata_collection.insert_one(camera_document)
            else:
                camera_document["Images"][image_file_name] = {"Datetime": str_datetime, "BucketName": self.minio_bucket_name, "Key": image_file_name, "PixelX": pixelx, "PixelY": pixely, "ImageType": "png"}
                self.devicedata_collection.update_one({"DevieName": camera_name}, {"$set": {"Images": camera_document["Images"]}})

        one_shot_document = dict()
        one_shot_document["ShotNumber"] = self.shot_number
        one_shot_document["Proton_Aimed_Energy"] = proton_aimed_energy
        one_shot_document["Datetime"] = str_datetime
        one_shot_document["Owner"] = owner
        one_shot_document["Comment"] = comment
        one_shot_document["PVSnapshot"] = PVsnapshot
        one_shot_document["Images"] = metadata_imgs
        self.metadata_collection.insert_one(one_shot_document)
        self.shot_number += 1


    def getCameraImgs(self, camera_name):
        camera_document = self.devicedata_collection.find_one({"DevieName": camera_name})
        imgs_dict = camera_document['Images']
        return imgs_dict

                



if __name__ == "main":
    OperateData = ExperimentalDataBase(_mongoclient_address = "mongodb://222.29.111.164:27017/", \
                                        _minioclient_address = "222.29.111.164:9000", \
                                        _access_key = "laser", _secret_key = "laserplasma")
    
    OperateData.insertOneShotData_Example(proton_aimed_energy=1.0, comment="第一块闪烁体")
    OperateData.insertOneShotData_Example(proton_aimed_energy=1.5, comment="第一块闪烁体")
    OperateData.insertOneShotData_Example(proton_aimed_energy=2.0, comment="第一块闪烁体")
    OperateData.insertOneShotData_Example(proton_aimed_energy=2.5, comment="第一块闪烁体")
    OperateData.insertOneShotData_Example(proton_aimed_energy=2.5, comment="第一块闪烁体，关Q3")
    OperateData.insertOneShotData_Example(proton_aimed_energy=2.5, comment="第一块闪烁体，关Q1")
    OperateData.insertOneShotData_Example(proton_aimed_energy=2.5, comment="第一块闪烁体，关Q2")
    OperateData.insertOneShotData_Example(proton_aimed_energy=3, comment="第二块闪烁体，关偏转铁")
    OperateData.insertOneShotData_Example(proton_aimed_energy=3, comment="第二块闪烁体，开偏转铁")
    OperateData.insertOneShotData_Example(proton_aimed_energy=3, comment="第二块闪烁体")
    
