# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 10:19:21 2019

@author: 46573
"""

import numpy as np
import pandas as pd
from pymongo import MongoClient
import re
import requests
import json
import threading as td
from queue import Queue
import datetime

def split_date(s):
    one_str = s
    day = int(one_str.split()[0].split('-')[2])
    hours = int(one_str.split()[1].split(':')[0])
    minutes = int(one_str.split()[1].split(':')[1])    
    seconds = int(one_str.split()[1].split(':')[2].split('.')[0]) 
    final_time = day*3600*24 + hours*3600 + minutes*60 + seconds
    return final_time
def split_12dian(s):
    one_str = list(s)[0]
    hour = int(one_str.split()[1].split('.')[0].split(':')[0])
    if hour < 12:
        return True
    else:
        return False
def sort_by_value(d): 
    items=d.items() 
    backitems=[[v[1],v[0]] for v in items] 
    backitems.sort(reverse = True) 
    return [ backitems[i][1] for i in range(0,len(backitems))] 
def p_xiache(dic):
    num = 0
    for i in dic.keys():
        num += dic[i]
    return num
def find_enterTime(s):
    temp1 = s.split()[0]
    temp2 = list(s.split()[1].split(':'))
    temp2[1] = int(temp2[1])+5
    if temp2[1] > 60:
        temp2[0] = str(int(temp2[0])+1)
        temp2[1] = str(temp2[1] - 60)
    else:
        temp2[1] = str(temp2[1])
    if len(temp2[1]) == 1 :
        temp2[1] = '0' + temp2[1]
    if len(temp2[0]) == 1:
        temp2[0] = '0' + temp2[0]
    temp = temp1 + ' ' + ':'.join(temp2)
    return temp
def find_enterTime_zao(s):
    temp1 = s.split()[0]
    temp2 = list(s.split()[1].split(':'))
    temp2[1] = int(temp2[1])-5
    if temp2[1] < 0 :
        temp2[0] = str(int(temp2[0]) - 1)
        temp2[1] = str(temp2[1] + 60)
    else:
        temp2[1] = str(temp2[1])
    if len(temp2[1]) == 1:
        temp2[1] = '0' + temp2[1]  
    if len(temp2[0]) == 1:
        temp2[0] = '0' + temp2[0]
    temp = temp1 + ' ' + ':'.join(temp2)
    return temp
'''调高德API获取站点经纬度'''

def coords(city):
    url = 'your url'   # 输入API问号前固定不变的部分
    params = { 'key': 'your key',                 
               'address': '淮安市'+city   }                    # 将两个参数放入字典
    res = requests.get(url, params)
    jd =  json.loads(res.text)
    if 'geocodes' not in jd.keys() or 'location' not in jd['geocodes'][0].keys():
        return 0
    return jd['geocodes'][0]['location']

def find_direct(location,direct,i):
    num1 = direct[i].count({location:0})
    num2 = direct[i].count({location:1})
    if num1 >= num2:
        return 0
    else:
        return 1

def oodd1(erweima_no,chepai,jingweidu,erweima,mycol,backValue,backValue2,backValue3,backValue4):
    a = {}
    bb = {}
    direct = {}
    not_pipei_dict = {}
    only_zaowan_oncebycar_dict = {}
    for i in erweima_no:
#        i = erweima_no[0]
        print(erweima_no.index(i))
        shijian = erweima[erweima['card_no'] == i].loc[:,'riding_time']
        if len(shijian) ==1:
            continue
        busPlate = erweima[erweima['card_no'] == i].loc[:,'busPlate']
        lunzi_id = []
        for j in busPlate.index:
            temp = chepai[chepai['plate'] == busPlate[int(np.array(j))]].loc[:,'vehicleId']
#            print(temp)
            lunzi_id.append(list(temp))
        for kkk in range(len(lunzi_id)):
            lunzi_id[kkk] = ''.join(lunzi_id[kkk])
            
        for p in range(len(lunzi_id)):
            if lunzi_id[p] == '':
                continue
            m = shijian.index[p]
            myquery = {'onboardId':int(lunzi_id[p]),'enterTime':{'$gte':find_enterTime_zao(shijian[m]),'$lte':find_enterTime(shijian[m])}}
            uuu = []
            for x in mycol.find(myquery):
                uuu.append(x)
            uuu = pd.DataFrame(uuu)
#            print(uuu)
            if len(uuu) == 0:
                continue
#            print(uuu)
            for u in range(len(uuu)):
                if split_date(uuu['enterTime'][u]) < split_date(shijian[m]) < split_date(uuu['leaveTime'][u])+120:
                    break
            if i not in a.keys() : 
                a[i] = [{shijian[m]:uuu['stopName'][u]}]
                direct[i] = [{uuu['stopName'][u]:uuu['direct'][u]}]
            else:
                a[i].append({shijian[m]:uuu['stopName'][u]})
                direct[i].append({uuu['stopName'][u]:uuu['direct'][u]})
    not_pipei_dict['not_pipei'] = len(erweima_no) - len(a)
    '''删除只乘一次车的乘客'''
    bbb = []          
    for i in a.keys():
        if len(a[i]) == 1:
            bbb.append(i)
    for i in bbb:
        del a[i]
    bb['bbb'] = len(bbb)
    
    '''早上晚上根据12点分开,找出每个乘客2月份早晚所有的乘车地点'''
    a_stop = {}
    a_stop_evening = {}
    
    for i in a.keys():
        for j in range(len(a[i])):
            if split_12dian(a[i][j].keys()):
                if i not in a_stop.keys():
                    a_stop[i] = [list(a[i][j].values())[0]]
                else:
                    a_stop[i].append(list(a[i][j].values())[0])
            else:
                if i not in a_stop_evening.keys():
                    a_stop_evening[i] = [list(a[i][j].values())[0]]
                else:
                    a_stop_evening[i].append(list(a[i][j].values())[0]) 
    '''找出规律,预测下车地点'''
    a_up_down = {}
    temp = {}
    temp_evening = {}
    temp_neither = {}
    for i in a.keys():
        if i in a_stop.keys() and i in a_stop_evening.keys():
            
            zhantai_quchong = list(set(a_stop[i]))
            zhantai_quchong_evening = list(set(a_stop_evening[i]))
            for j in zhantai_quchong:
                if i not in temp.keys():    
                    temp[i] = {j:a_stop[i].count(j)}
                else:
                    temp[i][j] = a_stop[i].count(j)
            for j in zhantai_quchong_evening:
                if i not in temp_evening.keys():    
                    temp_evening[i] = {j:a_stop_evening[i].count(j)}
                else:
                    temp_evening[i][j] = a_stop_evening[i].count(j)   
        if i in a_stop.keys() and i not in a_stop_evening.keys():
            zhantai_quchong_neither = list(set(a_stop[i]))
            for j in zhantai_quchong_neither:
                if i not in temp_neither.keys():
                    temp_neither[i] = {j:a_stop[i].count(j)}
                else:
                    temp_neither[i][j] = a_stop[i].count(j)
        if i in a_stop_evening.keys() and i not in a_stop.keys():
            zhantai_quchong_neither = list(set(a_stop_evening[i]))
            for j in zhantai_quchong_neither:
                if i not in temp_neither.keys():
                    temp_neither[i] = {j:a_stop_evening[i].count(j)}
                else:
                    temp_neither[i][j] = a_stop_evening[i].count(j)   
    '''开始预测上下车地点'''
    for i in temp.keys():
        result1 = sort_by_value(temp[i]) 
        result2 = sort_by_value(temp_evening[i])
        
        station_index1 = jingweidu[jingweidu['short_name'] == result1[0]].index
        station_index2 = jingweidu[jingweidu['short_name'] == result2[0]].index
        if result1[0] != result2[0]:
            if len(station_index1) == 0 or len(station_index2) == 0 :
                a_up_down[i] = {'上车':result1[0],'方向O':find_direct(result1[0],direct,i),'经纬度_morning':coords(result1[0]),\
                         '下车':result2[0],'方向D':-find_direct(result1[0],direct,i)+1,'经纬度_aft':coords(result2[0]),'概率':\
                     (temp[i][result1[0]]+temp_evening[i][result2[0]])/(p_xiache(temp[i])+p_xiache(temp_evening[i]))}
            else:
                code1_jing = jingweidu['latitude'][station_index1[0]]
                code1_wei = jingweidu['longitude'][station_index1[0]]
                code2_jing = jingweidu['latitude'][station_index2[0]]
                code2_wei = jingweidu['longitude'][station_index2[0]]  
                a_up_down[i] = {'上车':result1[0],'方向O':find_direct(result1[0],direct,i),'经纬度_morning':[code1_jing,code1_wei],\
                         '下车':result2[0],'方向D':-find_direct(result1[0],direct,i)+1,'经纬度_aft':[code2_jing,code2_wei],'概率':\
                     (temp[i][result1[0]]+temp_evening[i][result2[0]])/(p_xiache(temp[i])+p_xiache(temp_evening[i]))}
        else:
            a_up_down[i] = {'上车':result1[0],'方向O':find_direct(result1[0],direct,i),'经纬度_morning':coords(result1[0]),\
                     '下车':'','方向D':'','经纬度_aft':'','概率':''}
    '''只有早晚的'''
    for i in temp_neither.keys():
        if len(temp_neither[i]) == 1:
            continue
        result3 = sort_by_value(temp_neither[i])
        station_index3 = jingweidu[jingweidu['short_name'] == result3[0]].index
        station_index4 = jingweidu[jingweidu['short_name'] == result3[1]].index
        if result3[0] != result3[1]:
            if len(station_index3) == 0 or len(station_index4) == 0 :
                a_up_down[i] = {'上车':result3[0],'方向O':find_direct(result3[0],direct,i),'经纬度_morning':coords(result3[0]),\
                         '下车':result3[1],'方向D':-find_direct(result3[0],direct,i)+1,'经纬度_aft':coords(result3[1]),'概率':\
                     (temp_neither[i][result3[0]]+temp_neither[i][result3[1]])/p_xiache(temp_neither[i])}
            else:
                code1_jing = jingweidu['latitude'][station_index3[0]]
                code1_wei = jingweidu['longitude'][station_index3[0]]
                code2_jing = jingweidu['latitude'][station_index4[0]]
                code2_wei = jingweidu['longitude'][station_index4[0]]  
                a_up_down[i] = {'上车':result3[0],'方向O':find_direct(result3[0],direct,i),'经纬度_morning':[code1_jing,code1_wei],\
                         '下车':result3[1],'方向D':-find_direct(result3[0],direct,i)+1,'经纬度_aft':[code2_jing,code2_wei],'概率':\
                     (temp_neither[i][result3[0]]+temp_neither[i][result3[1]])/p_xiache(temp_neither[i])}
        else:
            a_up_down[i] = {'上车':result3[0],'方向O':find_direct(result3[0],direct,i),'经纬度_morning':coords(result3[0]),\
             '下车':'','方向D':'','经纬度_aft':'','概率':''}
    only_zaowan_oncebycar_dict['only_zaowan_oncebycar'] = len(a) - len(a_up_down)
    backValue.put(a_up_down)
    backValue2.put(bb)
    backValue3.put(not_pipei_dict)
    backValue4.put(only_zaowan_oncebycar_dict)
'''多线程'''

def multithreading(erweima_no,mean,chepai,jingweidu,erweima,mycol):
    # backValue中存放返回值，代替return的返回值
    backValue = Queue()
    backValue2 = Queue()
    backValue3 = Queue()
    backValue4 = Queue()
    # 线程集合
    threads = []
    # 数据数组
    data = [erweima_no[:mean],erweima_no[mean:2*mean],erweima_no[2*mean:3*mean],erweima_no[3*mean:4*mean],\
            erweima_no[4*mean:5*mean],erweima_no[5*mean:6*mean],erweima_no[6*mean:7*mean],erweima_no[7*mean:8*mean],\
            erweima_no[8*mean:9*mean],erweima_no[9*mean:10*mean],erweima_no[10*mean:11*mean],erweima_no[11*mean:12*mean],\
            erweima_no[12*mean:13*mean],erweima_no[13*mean:14*mean],erweima_no[14*mean:15*mean],erweima_no[15*mean:]]
            
    # 在多线程函数中定义四个线程，启动线程，将每个线程添加到多线程的列表中
    # 定义四个线程
    for i in range(len(data)):
        # Thread首字母要大写，被调用的job函数没有括号，只是一个索引，参数在后面
        t1 = td.Thread(target=oodd1, args=(data[i],chepai,jingweidu,erweima,mycol,backValue,backValue2,backValue3,backValue4))
        t1.start()  # 开始线程
        threads.append(t1)  # 把每个线程append到线程列表中
 
    # 分别join四个线程到主线程
    for i in threads:
        i.join()
 
    # 定义一个空的列表results，将四个线运行后保存在队列中的结果返回给空列表results
    results1 = []
    results2 = []
    results3 = []
    results4 = []
    for _ in range(len(data)):
        results1.append(backValue.get())  # q.get()按顺序从q中拿出一个值
        results2.append(backValue2.get())
        results3.append(backValue3.get())
        results4.append(backValue4.get())
           
    # 打印返回值
#    print(results)
    return results1,results2,results3,results4
def main(time):
    chepai = pd.read_csv('ha_bus.car_info.csv',keep_default_na=False)

    myclient = MongoClient('your localhost')#测试
    mydb = myclient["ha_bus"]
    '''站点经纬度坐标数据'''
    jingweidu = []
    mycol = mydb['stop']
    for x in mycol.find():
        jingweidu.append(x)
    jingweidu = pd.DataFrame(jingweidu)
    '''二维码数据'''
    erweima= []
    mycol = mydb['qrcode_paid_record']
    myquery = {'riding_time':re.compile(time)}
    for x in mycol.find(myquery):
        erweima.append(x)
    erweima = pd.DataFrame(erweima)
    erweima_no = list(set(erweima['card_no']))
    num = len(erweima_no)
    mean = int(num/16)
    
    mycol = mydb['stop_arrive_leave']
    
    a_up_down_list,bbb,not_pipei,onlyzaowan = multithreading(erweima_no,mean,chepai,jingweidu,erweima,mycol)
    a_up_down = {}
    for i in a_up_down_list:
        a_up_down.update(i)
    for i in a_up_down.keys():
        if a_up_down[i]['上车'] == a_up_down[i]['下车']:
#            print('aaa')
            del a_up_down[i]
    
    '''所有线路'''
    od_all_route = []
    mydict_all = {}
    for i in a_up_down.keys():
        mydict_all = {'rate':a_up_down[i]['概率'],'cardId':str(i),'lat_long_morning':a_up_down[i]['经纬度_morning'],\
                      'lat_long_aft':a_up_down[i]['经纬度_aft'],'up':a_up_down[i]['上车'],'down':a_up_down[i]['下车'],\
                      'date':time,'cardType':1,'up_direct':a_up_down[i]['方向O'],'down_direct':a_up_down[i]['方向D']}
        
        od_all_route.append(mydict_all)
    mycol = mydb['od_all_route']
    x = mycol.insert_many(od_all_route)
    




    