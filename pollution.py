import requests
import numpy as np
import pandas as pd
import json
import time
import pymysql
import config


def gettree(id=''):
    """
    获取该地区污水处理厂和污染企业信息
    id：地区code
    """
    companyurl = 'https://bsw.willsoft.com.cn//LFHBJPlatformDEV/pollutionSourceInfo/getPSListTree.do?psname=&attentiondegreecode=&psclasscode=1&regioncode='+id+'&pklx=1'
    treatmenturl = 'https://bsw.willsoft.com.cn//LFHBJPlatformDEV/pollutionSourceInfo/getPSListTree.do?psname=&attentiondegreecode=&psclasscode=4&regioncode='+id+'&pklx=1'
    header = {'Accept': '*/*',
              'Accept-Encoding': 'gzip, deflate, br',
              'Cache-Control': 'no-cache',
              'content-type': 'application/json',
              'Host': 'bsw.willsoft.com.cn',
              'Pragma': 'no-cache',
              'Referer': 'https://servicewechat.com/wx5c2c83c061b20265/devtools/page-frame.html',
              'Sec-Fetch-Dest': 'empty',
              'Sec-Fetch-Site': 'cross-site',
              'Sec-Fetch-User': '?F',
              'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1 wechatdevtools/1.02.1911180 MicroMessenger/7.0.4 Language/zh_CN webview/'
              }
    treatmentres = requests.get(treatmenturl, headers=header)
    companyres = requests.get(companyurl, headers=header)
    temp = json.loads(treatmentres.text)
    temp = temp['rows']
    treatmentid = {}
    for i in range(len(temp)):
        treatmentid[temp[i]['psname']] = temp[i]['pscode']
    temp = json.loads(companyres.text)
    temp = temp['rows']
    companyid = {}
    for i in range(len(temp)):
        companyid[temp[i]['psname']] = temp[i]['pscode']
    return treatmentid, companyid


def spyder():
    """
    获取该地区污水处理厂和污染企业排放信息
    return :字典
    key：企业或污水处理厂code
    value：排放信息
    """
    region_code = {
        '安次区': '131002000',
        '广阳区': '131003000',
        '固安县': '131022000',
        '永清县': '131023000',
        '香河县': '131024000',
        '大城县': '131025000',
        '文安县': '131026000',
        '大厂回族自治县': '131028000',
        '霸州市': '131081000',
        '三河市': '131082000'
    }
    head = {
        'Referer': 'https://servicewechat.com/wx5c2c83c061b20265/devtools/page-frame.html',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.96 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    ntime = time.localtime()
    starttime = time.strftime("%Y-%m-%d", ntime)
    endtime = time.strftime("%Y-%m-%d", ntime)
    save = {}
    for key in region_code:
        tid, cid = gettree(region_code[key])
        for pscode in tid:
            outurl = 'https://bsw.willsoft.com.cn/LFHBJPlatformDEV/PollutantSourceMonitor/getWaterOutPut.do?pscode=' + \
                str(tid[pscode])
            outres = requests.get(outurl, headers=head)
            while outres.status_code != 200:
                outres = requests.get(outurl, headers=head)
            temp = json.loads(outres.text)
            temp = temp['rows']
            for i in range(len(temp)):
                resurl = 'https://bsw.willsoft.com.cn//LFHBJPlatformDEV/PollutantSourceMonitor/getOutPutData.do?pscode='+str(tid[pscode]) + \
                    '&begintime=' + starttime + '&endtime=' + endtime + '&outputcode='+str(temp[i]['outputcode'])+'&outputtype='+str(temp[i]['outputtype']) + \
                    '&ConcernType=0&tTimeType=4&isExceed=1&page=1&rows=4800'
                outres = requests.get(resurl, headers=head)
                while outres.status_code != 200:
                    # print(outres.status_code)
                    time.sleep(20)
                    outres = requests.get(resurl, headers=head)
                tempwrite = json.loads(outres.text)
                if(len(tempwrite['rows'])):
                    write = tempwrite['rows'][0]
                    if ntime[3] - int(write['MonitorTime'][-8:-6]) < 3:
                        save[str(tid[pscode])+str(temp[i]['outputcode'])] = write
        for pscode in cid:
            outurl = 'https://bsw.willsoft.com.cn/LFHBJPlatformDEV/PollutantSourceMonitor/getWaterOutPut.do?pscode=' + \
                str(cid[pscode])
            outres = requests.get(outurl, headers=head)
            while outres.status_code != 200:
                outres = requests.get(resurl, headers=head)
            temp = json.loads(outres.text)
            temp = temp['rows']
            for i in range(len(temp)):
                resurl = 'https://bsw.willsoft.com.cn//LFHBJPlatformDEV/PollutantSourceMonitor/getOutPutData.do?pscode='+str(cid[pscode]) + \
                    '&begintime=' + starttime + '&endtime=' + endtime + '&outputcode='+str(temp[i]['outputcode'])+'&outputtype='+str(temp[i]['outputtype']) + \
                    '&ConcernType=0&tTimeType=4&isExceed=1&page=1&rows=4800'
                outres = requests.get(resurl, headers=head)
                while outres.status_code != 200:
                    # print(outres.status_code)
                    time.sleep(20)
                    outres = requests.get(resurl, headers=head)
                tempwrite = json.loads(outres.text)
                if len(tempwrite['rows']):
                    write = tempwrite['rows'][0]
                    if(write and ntime[3] - int(write['MonitorTime'][-8:-6]) < 3):
                        save[str(cid[pscode])+str(temp[i]['outputcode'])] = write
    return save


def change(data={}):
    """
    修改爬取数据格式以符合数据库表结构
    data：字典，爬取的污染源信息
    return :dateframe
    """
    type = {'011': 'COD', '060': 'nh3', '001': 'PH', '065': 'n', '101': 'p',
            '023': 'cr', '024': 'cr6', '028': 'ni', '029': 'cu'}
    cname = list(type.values())
    cname.append('Flow')
    cname.append('Time')
    cname.append('outcode')
    w = pd.DataFrame(columns=cname)
    for key, dc in data.items():
        nc = ['outcode']
        dic = {}
        dic['outcode'] = key
        for i, val in dc.items():
            if i.find('C') != -1:
                continue
            elif i[0] == 'M':
                nc.append('Time')
                dic['Time'] = val
            elif i[0] == 'W':
                nc.append('Flow')
                dic['Flow'] = float(val)
            elif i.find('i') != -1:
                if i[0:i.find('i')] in type:
                    nc.append(type[i[0:i.find('i')]])
                    dic[type[i[0:i.find('i')]]] = float(val)
            elif i.find('w') != -1:
                if i[0:i.find('w')] in type:
                    nc.append(type[i[0:i.find('w')]])
                    dic[type[i[0:i.find('w')]]] = float(val)
        for i in cname:
            if nc.count(i):
                continue
            else:
                dic[i] = None
        w = w.append(dic, True)
    return w


pymysql.install_as_MySQLdb()
db = pymysql.connect(
    host=config.HOST,
    port=config.PORT,
    user=config.USERNAME,
    password=config.PASSWORD,
    database=config.DATABASE,
    charset='utf8'
)
sql_table = "my_pollution_date"


def insertSQL(data, sql_field, sql_len):
    cursor = db.cursor()
    sql = """insert into {}({})values({});""".format(sql_table, sql_field, sql_len)
    try:
        cursor.execute(sql, data)
        db.commit()
    except Exception as e:
        db.rollback()
    finally:
        cursor.close()


if __name__ == '__main__':
    data = spyder()
    df = change(data)
    sql_field = ",".join(df.columns.to_list())
    sql_len = ",".join(['%s'] * len(df.columns.to_list()))
    for index, row in df.iterrows():
        insertSQL(row.values.tolist(), sql_field, sql_len)
    
