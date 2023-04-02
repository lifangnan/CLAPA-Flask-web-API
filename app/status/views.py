
from flask import Flask, request, jsonify, make_response, send_file, render_template, Response, send_from_directory
from flask_cors import CORS
from datetime import datetime
from . import status_blue



@status_blue.route("/getOneCurrentData", methods = ['GET'])
def getOneCurrentData():
    pvName = request.args.get('pvName')

    # if pvName in channels_dict.keys():
    #     temp_channel = channels_dict[pvName]
    # else:
    #     temp_channel = CaChannel("pvName")
    #     channels_dict[pvName] = temp_channel
    
    # current_value = None
    # try:
    #     # temp_channel.searchw()
    #     current_value = temp_channel.getw()
    # except CaChannelException as e:
    #     print(pvName, e)

    now = datetime.now()

    current_value = 0

    res = make_response(jsonify({"code": 20000, "data": {'name': now.strftime('%Y/%m/%d %H:%M:%S'), 'value':[ now.strftime('%Y/%m/%d %H:%M:%S'), current_value]}}))
    res.status = '200'
    return res