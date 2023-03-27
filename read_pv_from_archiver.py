import urllib.request
from urllib import parse, error
import socket
from datetime import datetime
import time
import json
import pandas as pd 

# time_now = datetime.datetime.now()
# print(time_now.isoformat())
# time_now.timestamp()
# time.mktime(time_now)

def get_url_str(PV, start_time, end_time):
    start_time = start_time.isoformat()+'Z'
    end_time = end_time.isoformat()+'Z'
    form = parse.urlencode({"pv":PV, "from":start_time, "to":end_time})
    url = "http://222.29.111.164:17668/retrieval/data/getData.json?" + form
    # url = "http://222.29.111.164:17668/retrieval/data/getData.csv?" + form
    return url

def get_pv(PV, start_time, end_time):
    url = get_url_str(PV, start_time, end_time)
    try:
        json_return = urllib.request.urlopen(url)
        return json.load(json_return)
    except error.URLError as e:
        print(e.reason)
        if isinstance(e.reason, socket.timeout):
            print("获取超时")
        return None

def get_secs_val(PV, start_time, end_time):
    return get_pv(PV, start_time, end_time)[0]['data']


def get_pv_csv(PV, start_time, end_time):
    start_time = start_time.isoformat()+'Z'
    end_time = end_time.isoformat()+'Z'
    form = parse.urlencode({"pv":PV, "from":start_time, "to":end_time})
    url = "http://222.29.111.164:17668/retrieval/data/getData.csv?" + form
    try:
        file_path = "./temp_files/%s.csv"%("".join(filter(str.isalnum, PV)))
        urllib.request.urlretrieve(url, file_path)
        df = pd.read_csv(file_path, header=None, names=['secs' ,'val', 'severity', 'status', 'nanos'])
        # df['time'] = (df['secs']+df['nanos']*0.000000001).apply(datetime.fromtimestamp)
        df.set_index((df['secs']+df['nanos']*0.000000001).apply(datetime.fromtimestamp), inplace=True)
        return df
    except error.URLError as e:
        print(e.reason)
        if isinstance(e.reason, socket.timeout):
            print("获取超时")
        return None


# PV = "Mshutteropen"
# PV = "IT:PSQ1:GetCurrent"
# start_time = datetime(2022, 3, 1)
# end_time = datetime.now()

# data = urllib.request.urlretrieve("http://222.29.111.164:17668/retrieval/data/getData.csv?pv=IT%3APSQ1%3AGetCurrent&from=2022-03-01T00%3A00%3A00Z&to=2022-06-24T13%3A36%3A42.278214Z", "./temp_files/%s.csv"%("".join(filter(str.isalnum, PV))))



# df = pd.read_csv(data[0])


# # 获取各个发次对应的事件戳，保存在字典里
# counter2sec = dict()
# counter = data_parse( get_pv("13ANDOR1:cam1:ArrayCounter_RBV", start_time, end_time) )
# for item in counter:
#     counter2sec[ str(item['val']) ] = item['secs']
# # counter2 = get_pv("13ANDOR1:image1:ArrayCounter_RBV", start_time, end_time)

# for index in counter2sec.keys():
#     event_time = counter2sec[index]
#     # event_time = '1636791613'
#     print(index, event_time)
#     # start_t = datetime.datetime.fromtimestamp(event_time) - datetime.timedelta(seconds=2)
#     # end_t = datetime.datetime.fromtimestamp(event_time) + datetime.timedelta(seconds=2)
#     start_t = datetime.datetime.now() - datetime.timedelta(days = 100)
#     end_t = datetime.datetime.now()

#     pv = data_parse( get_pv("Mshutterstate", start_t, end_t) )
    
#     for record in pv:
#         if record['secs'] <= event_time:
#             recent_record = record
#         else:
#             break
#     print(get_url_str("Mshutterstate", start_t, end_t))
#     print(recent_record)
