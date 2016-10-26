# coding:utf-8
import base64
import hashlib, hmac
from urllib import urlencode
import datetime,time
import MySQLdb as mdb
import requests
import json
import threading
import sys, os
from multiprocessing import Process
from datetime import datetime as dt


def para_load():
    rawData = open('allPara.txt','r').readlines()
    profile = []
    for item in rawData:
        temp = item.split(' ')
        profile.append(temp)
    return profile


def city_load():
    rawData = open('chinaweather_city.txt', 'r').readlines()
    cityCode = []
    for item in rawData:
        temp = eval(item)['cityid']
        cityCode.append(temp)
    return cityCode


def rescode_load(filename):
    rawData = open(filename, 'r').readlines()
    resCode = {}
    for item in rawData:
        temp = item.split()
        resCode[temp[0]] = temp[1]
    return resCode


def gen_request_url(areaid, type, time):
    private_key = 'lenovo_webapi_data'
    public_key = 'http://open.weather.com.cn/data/?areaid=' + areaid + '&type=' + type + '&date='+ time +'&appid=33f8aee9fe7c0a2e'
    # print public_key
    hash_sha1 = hmac.new(private_key, public_key, hashlib.sha1).digest()
    encoded_key = base64.b64encode(hash_sha1)
    urlencoded_key = urlencode({'key':encoded_key})
    request_url = 'http://open.weather.com.cn/data/?areaid=' + areaid + '&type=' + type + '&date='+ time +'&appid=33f8ae&'+urlencoded_key
    # print request_url
    return request_url


def http_get(request_url):
    r = requests.get(request_url)
    return r.content


# ~~~~~~~~~~~~~forecast4d response的json解析~~~~~~~~~~~~~~~~~~
# 第一级别：response['c']表示城市相关的反馈，response['f']表示预报的内容，
# 第二级别：response['c']['...']直接查参数文件，response['f']['f0']表示预报发布时间，response['f']['f1']是列表形式,分别代表4天的预报情况
# 第三级别：response['f']['f1'][0]['...']表示第1天的，具体查询参数文件
def forecast4d_response_trans(response, allPara, weatherCondition, windDire, windLevel, day):
    result = {}
    # 获取所有可能的字段名和字段代码
    forecast4dFiled = [allPara[i][3] for i in range(len(allPara)) if (allPara[i][0] == 'forecast4d' and allPara[i][3] != 'null')]
    forecast4dCode = [allPara[i][1] for i in range(len(allPara)) if (allPara[i][0] == 'forecast4d' and allPara[i][3] != 'null')]
    # 将返回结果中的数据写入字典
    for i in range(4):
        if response['c'][forecast4dCode[i]]!='' and response['c'][forecast4dCode[i]] != '?':
            result[forecast4dFiled[i]] = response['c'][forecast4dCode[i]]
    for i in range(5, len(forecast4dFiled)-2) :
        if response['f']['f1'][day][forecast4dCode[i]]!='' and response['f']['f1'][day][forecast4dCode[i]] != '?':
            result[forecast4dFiled[i]] = response['f']['f1'][day][forecast4dCode[i]]
    # 需要特殊处理的字段
    weather_UpdateTime = response['f']['f0']
    result['weather_UpdateTime'] = str(DT.strptime(weather_UpdateTime, "%Y%m%d%H%M"))
    # datetime保存更新的天的信息
    result['dateTime'] = str(datetime.datetime.strptime(weather_UpdateTime, "%Y%m%d%H%M") + datetime.timedelta(days=day)).split(' ')[0]
    result['sunrise'] = response['f']['f1'][day]['fi'].split('|')[0]
    result['sunset'] = response['f']['f1'][day]['fi'].split('|')[1]
    # 需要根据配置文件转译返回结果的字段
    if result.has_key('weather'):
        result['weather'] = weatherCondition[result['weather']]
    if result.has_key('weather_night'):
        result['weather_night'] = weatherCondition[result['weather_night']]
    if result.has_key('wind_direction'):
        result['wind_direction'] = windDire[result['wind_direction']]
    if result.has_key('wind_direction_night'):
        result['wind_direction_night'] = windDire[result['wind_direction_night']]
    if result.has_key('wind_night'):
        result['wind_night'] = windLevel[result['wind_night']]
    if result.has_key('wind'):
        result['wind'] = windLevel[result['wind']]
    # result['']
    return result


# ~~~~~~~~~~~~~~~~index response的json解析~~~~~~~~~~~~~~~~~~~~
# 第一级别：response['i']是一个列表，3个元素分别表示三种指数; response['i0']表示指数发布时间
# 第二级别：response['i'][0]表示穿衣指数，response['i'][1]表示感冒指数，response['i'][2]表示洗车指数。
# 第三级别：response['i'][0]['i4']是穿衣指数的总体描述；response['i'][0]['i5']表示穿衣指数的建议，不需要解析。
def index_response_trans(response):
    result = {}
    indexFiled = ['index_ChuanYi','index_GanMao','index_XiChe']
    if response.has_key('i'):
        if len(response['i']) == 3:
            indexValue = [response['i'][i]['i5'] for i in range(3) if response['i'][i].has_key('i5')]
    for i in range(3):
        if indexValue[i]!='' and indexValue[i]!='?':
            result[indexFiled[i]] = indexValue[i]
    return result


# ~~~~~~~~~~~~~~observe response的json解析~~~~~~~~~~~~~~~~~~~~
# 第一级别：response['l']表示内容
# 第二级别：response['l']['...']直接查询参数文件，部分需要查配置文件
def observe_response_trans(response, allPara, weatherCondition, windDire):
    result = {}
    hourWeatherStr = ''
    finalResult = {}
    # 获取所有可能的字段名和字段在response中的代码
    observeFiled = [allPara[i][3] for i in range(len(allPara)) if (allPara[i][0] == 'observe' and allPara[i][3] != 'null')]
    observeCode = [allPara[i][1] for i in range(len(allPara)) if (allPara[i][0] == 'observe' and allPara[i][3] != 'null')]
    # 将返回结果中的数据写入字典
    for i in range(len(observeFiled)):
        if response.has_key('l'):
            if response['l'].has_key(observeCode[i]):
                if response['l'][observeCode[i]]!= '' and response['l'][observeCode[i]] != '?':
                    result[observeFiled[i]] = response['l'][observeCode[i]]
    # 需要查询配置文件的字段
    if result.has_key('windDire'):
        result['windDire'] = windDire[result['windDire']]
    if result.has_key('weatherCondition'):
        result['weatherCondition'] = weatherCondition[result['weatherCondition']]
    for key, value in result.iteritems():
        hourWeatherStr = hourWeatherStr + key +':' +  value + ","
    if result.has_key('humidity'):
        finalResult['humidity'] = result['humidity']
    if result.has_key('precipitation'):
        finalResult['precipitation'] = result['precipitation']
    if hourWeatherStr != '':
        finalResult['hourWeather'] = hourWeatherStr[:-1]
    return finalResult


# ~~~~~~~~~~~~~~~~air response的json解析~~~~~~~~~~~~~~~~~~~~
# 第一级别：response['p']表示内容
# 第二级别：response['p']['...']直接查询结果，不需要查配置文件
def air_response_trans(response, allPara):
    result = {}
    airFiled = [allPara[i][3] for i in range(len(allPara)) if (allPara[i][0] == 'air' and allPara[i][3] != 'null')]
    airCode = [allPara[i][1] for i in range(len(allPara)) if (allPara[i][0] == 'air' and allPara[i][3] != 'null')]
    # 将返回结果中的数据写入字典
    for i in range(len(airFiled)):
        if type(response['p']) == dict:
            if response['p'].has_key(airCode[i]):
                result[airFiled[i]] = int(response['p'][airCode[i]])
    return result


# 将数据写入数据库
def insert_DB(content):
    try:
        conn = mdb.connect(host='10.100.213.221', user='dialog', passwd='speaker2016', db='DialogService',port=3306, charset='utf8')
        cur= conn.cursor()
        keys = ''
        values = ''
        duplicate_key_values =''
        for key, value in content.iteritems():
            keys = keys + str(key) + ","
            values = values + "'"+ str(value) + "',"
            duplicate_key_values = duplicate_key_values + str(key) + "='" + str(value) + "',"
        sql = "INSERT INTO weather_info(" + keys[:-1].replace('\'','') + ") VALUES (" + values[:-1] + ")" + " ON DUPLICATE KEY UPDATE " + duplicate_key_values[:-1]
        # print sql
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
    except mdb.Error,e:
         print "Mysql Error %d: %s" % (e.args[0], e.args[1])


def insertDB_loop(count,cityCode, time, allPara, weatherCondition, windDire, windLevel):
    # # lock.acquire()
    # try:
    #
    # finally:
    #     # 改完了一定要释放锁:
    #     # lock.release()
    for cityID in cityCode:
        global countAll
        countAll += 1
        print "Task {0} is running...the NO.{1} city {2} is downloading".format(threading.current_thread().name, countAll, cityID)
        forecast4d_request_url = gen_request_url(cityID, 'forecast4d', time)
        forecast4d_response = json.loads(http_get(forecast4d_request_url))
        for i in range(4):
            forecast4d_contentDB = forecast4d_response_trans(forecast4d_response, allPara, weatherCondition, windDire,
                                                             windLevel, i)
            if i == 0:
                index_request_url = gen_request_url(cityID, 'index', time)
                index_response = json.loads(http_get(index_request_url))
                index_contentDB = index_response_trans(index_response)
                observe_request_url = gen_request_url(cityID, 'observe', time)
                observe_response = json.loads(http_get(observe_request_url))
                observe_contentDB = observe_response_trans(observe_response, allPara, weatherCondition, windDire)
                air_request_url = gen_request_url(cityID, 'air', time)
                air_response = json.loads(http_get(air_request_url))
                air_contentDB = air_response_trans(air_response, allPara)
                contentDB = dict(forecast4d_contentDB, **index_contentDB)
                contentDB = dict(contentDB, **observe_contentDB)
                contentDB = dict(contentDB, **air_contentDB)
            else:
                contentDB = forecast4d_contentDB
            contentDB['countyID'] = cityID
            insert_DB(contentDB)
            # lock.acquire()
            # try:
            #     # 放心地改吧:
            #
            # # finally:
            #     # 改完了一定要释放锁:
            #     # lock.release()



if __name__ == '__main__':
    lock = threading.Lock()
    countAll = 0
    count = 0
    default_encoding = 'utf-8'
    if sys.getdefaultencoding() != default_encoding:
        reload(sys)
        sys.setdefaultencoding(default_encoding)
    time = time.strftime("%Y%m%d%H%M", time.localtime())
    allPara = para_load()
    allCityCode = city_load()
    weatherCondition = rescode_load("weatherCondition.txt")
    windDire = rescode_load("windDire.txt")
    windLevel = rescode_load("windLevel.txt")
    cityCode = []
    for i in range(5):
        cityCode.append(allCityCode[(i*len(allCityCode)/5):((i+1)*len(allCityCode)/5)])
    p1 = threading.Thread(target=insertDB_loop, args=(count, cityCode[0], time, allPara, weatherCondition, windDire, windLevel))
    p2 = threading.Thread(target=insertDB_loop, args=(count, cityCode[1], time, allPara, weatherCondition, windDire, windLevel))
    p3 = threading.Thread(target=insertDB_loop, args=(count,cityCode[2], time, allPara, weatherCondition, windDire, windLevel))
    # p4 = threading.Thread(target=insertDB_loop, args=(count,cityCode[3], time, allPara, weatherCondition, windDire, windLevel))
    # p5 = threading.Thread(target=insertDB_loop, args=(count,cityCode[4], time, allPara, weatherCondition, windDire, windLevel))
    p1.start()
    p2.start()
    p3.start()
    # p4.start()
    # p5.start()
    p1.join()
    p2.join()
    p3.join()
    # p4.join()
    # p5.join()
    # startTime = datetime.datetime.now()
    # runTime = datetime.datetime.now() - startTime
    # print "runTime: {0} ".format(runTime)