import pymysql
import requests
import datetime
import json
import re
import config

host_ = "172.18.10.93:8080"
url = "http://{}/GJZ/Ajax/Publish.ashx".format(host_)

payload="AreaID={}&RiverID=&MNName=&PageIndex=-1&PageSize=80&action=getRealDatas"
headers = {
  'Host': host_,
  'Accept': 'application/json, text/javascript, */*; q=0.01',
  'X-Requested-With': 'XMLHttpRequest',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.67 Safari/537.36 Edg/87.0.664.47',
  'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
  'Origin': 'http://{}'.format(host_),
  'Referer': 'http://{}/GJZ/Business/Publish/RealDatas.html'.format(host_),
  'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6'
}

dict_field = {
    "省份":"PROVINCE_NAME",
    "流域":"AREA_NAME",
    "所属地市":"CITY_NAME",
    "所属河流":"RIVER_NAME",
    "断面名称":"SURFACE_NAME",
    "监测时间":"DATE_TIME",
    "水质类别":"CURRENT_WATER_QUALITY",
    "水温":"WATER_TEMPERATURE",
    "pH":"PH_VALUE",
    "溶解氧":"DISSOLVED_OXYGEN",
    "电导率":"CONDUCTIVITY",
    "浊度":"TURBIDITY",
    "高锰酸盐指数":"PERMANGANATE_INDEX",
    "氨氮":"AMMONIA",
    "总磷":"TOTAL_PHOSPHORUS",
    "总氮":"TOTAL_NITROGEN",
    "叶绿素α":"CHLOROPHYLL",
    "藻密度":"ALGAL_DENSITY",
    "站点情况":"ONLINE",
    "采集日期":"COLLECT_DATE"
}

sql_field = ",".join([dict_field[key] for key in dict_field])
sql_len = ",".join(['%s'] * len(dict_field))
#sql_table = "t_water_monitor_auto"
sql_table = "t_water_monitor_auto_20210807"

# 钉钉警报
def push(details):
    try:
        webhook = "https://oapi.dingtalk.com/robot/send?access_token=e41253fcb4ed841e56c0045c5a3feb60dc149f66fea70288c7ee5bef20a17a59"
        message = "国家水质自动综合监管平台-图书馆\n" + details + "\n" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(message)
        data = {'msgtype': 'text', 'text': {'content': message}}
        data_string = json.dumps(data)
        headers = {"Content-Type": "application/json;charset=utf-8"}
        resp = requests.post(url=webhook, headers=headers, data=data_string, timeout=10, verify=True)
    except:
        pass

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
    return False


def get(code):
    try:
        response = requests.request("POST", url, headers=headers, data=payload.format(code))
        return response.text
    except:
        push("抓取失败")


# 返回json数据格式的dict
def api_json(thead_sub, tbody_sub):
    dict_array = []
    for item in tbody_sub:
        dict_sub = {}
        for head, body in zip(thead_sub, item):
            dict_sub[head] = body
        dict_array.append(dict_sub)
    return dict_array


# 数据解析
def analyse(data):
    try:
        dict_data = json.loads(data)
        thead = dict_data['thead']
        tbody = dict_data['tbody']
        thead_sub = []
        for item in thead:
            thead_sub.append(re.sub(r'<br/>.*?</span>', '', item))
        
        tbody_sub = []
        for item in tbody:
            item_sub = []
            flag = True # 站点正常标记
            for sub in item:
                if sub is not None: # 主要判断监测时间是否为空
                    if 'span' in sub:
                        ret = re.findall(r'<span data-toggle=\'tooltip\' data-placement=\'right\' title=\'(.*?)\'>(.*?)</span>', sub, re.S)
                        if ret and ret[0]: # 是否匹配到数据
                            if is_number(ret[0][1]): # 判断是否为数值
                                item_sub.append(float(ret[0][1]))
                            else:
                                mul = ret[0][0].split("&#10;")
                                if len(mul) != 1:
                                    item_sub.append(mul[0].split(":")[1]) # 所属地市
                                    item_sub.append(mul[1].split(":")[1]) # 所属河流
                                    item_sub.append(ret[0][1]) # 自动站名字
                                else:
                                    item_sub.append(0)
                        else:
                            item_sub.append(0)
                    else:
                        if sub == "--" or sub == "*":
                            item_sub.append(0)
                        elif ":00" in sub:
                            item_sub.append(str(datetime.datetime.now().year) + '-' + sub)
                        else:
                            item_sub.append(sub)
                else:
                    flag = False
                    break
            if flag:
                item_sub.append(item_sub[5].split(" ")[0])
                tbody_sub.append(item_sub)
        
        thead_sub.insert(2, '所属河流')
        thead_sub.insert(2, '所属地市')
        return thead_sub, tbody_sub # 返回字段名跟内容

    except:
        push("解析失败")

pymysql.install_as_MySQLdb()
db = pymysql.connect(
    host=config.HOST,
    port=config.PORT,
    user=config.USERNAME,
    password=config.PASSWORD,
    database=config.DATABASE,
    charset='utf8'
)

def insertSQL(data):
    for item in data:
        cursor = db.cursor()
        sql = """insert into {}({})values({});""".format(sql_table, sql_field, sql_len)
        try:
            cursor.execute(sql, item)
            db.commit()
        except Exception as e:
            db.rollback()
        finally:
            cursor.close()


if __name__ == '__main__':
    province = ["110000", "120000", "130000"]
    count = []
    for item in province:
        thead_sub, tbody_sub = analyse(get(item))
        insertSQL(tbody_sub)
        count.append(len(tbody_sub))
    push("【自动站定时抓取中】\n北京：{} 条\n天津：{} 条\n河北：{} 条".format(count[0], count[1], count[2]))
    db.close()
