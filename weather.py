import requests
import re
import json
import datetime
import pymysql
import config


class Weather:
    def __init__(self, city=None):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Mobile Safari/537.36",
            "content-type": "application/json;charset=utf-8"}
        self.city = city

    def get_weather(self, city=None, latlng=None):
        if latlng:
            self.city = self.latlng2code(latlng)
        if city:
            self.city = city
        if not self.city:
            raise Exception("Please input param city")
        if not self.city.isdigit():
            self.city = self.city2code()
        url = "http://www.weather.com.cn/weather/{}.shtml".format(self.city)
        resp = requests.get(url, headers=self.headers)
        resp.encoding = 'utf-8'
        re_we = re.findall(
            r'<h1>(.*?)</h1>.*?<big class="png40 (.*?)"></big>.*?<p title="(.*?)" class="wea">.*?<span>(.*?)</span>/<i>(.*?)</i>.*?<span title="(.*?)".*?<i>(.*?)</i>',
            resp.text, re.S)
        day7weather = []
        for item in re_we:
            day7weather.append({
                'date': item[0],
                'png': item[1],
                'status': item[2],
                'tem': item[3] + '~' + item[4],
                'wind': item[5],
                'level': item[6]
            })
        return json.dumps(day7weather, ensure_ascii=False)

    def city2code(self):
        url = "http://toy1.weather.com.cn/search?cityname={}&callback=success_jsonpCallback".format(
            self.city)
        try:
            resp = requests.get(url, headers=self.headers, timeout=5)
            resp.encoding = 'utf-8'
            re_result = re.findall(r'success_jsonpCallback\((.*?)\)', resp.text, re.S)
            result = json.loads(re_result[0])[0]['ref'].split("~")[0]
        except:
            result = ""
        return result

    def latlng2code(self, latlng=("", "")):
        url = "https://mpv2.weather.com.cn/loc?lat={}&lng={}".format(*latlng)
        try:
            resp = requests.get(url, headers=self.headers, timeout=5)
            resp.encoding = 'utf-8'
            result = json.loads(resp.text)['location']['station']
        except:
            result = ""
        return result

pymysql.install_as_MySQLdb()
db = pymysql.connect(
        host=config.HOST,
        port=config.PORT,
        user=config.USERNAME,
        password=config.PASSWORD,
        database=config.DATABASE,
        charset='utf8'
    )

def insertSQL(city, val):
    now = datetime.datetime.now()
    for index, item in enumerate(val):
        temp = item['tem'].replace("℃", "")
        try:
            maxTemp, minTemp = temp.split("~")
        except:
            maxTemp, minTemp = temp, temp
        row = [
            (now + datetime.timedelta(hours=24 * index)).strftime("%Y-%m-%d") + " 00:00:00",
            maxTemp, minTemp, '20', '0', '0',
            re.sub("\D" , "", item['level']),
            item['status'], item['png'], item['wind']
        ]

        cursor = db.cursor()
        sql = """insert into my_weather_{}(dateStr, maxTemp, minTemp, cloudAmount, rainFall, windDirect, windLevel, statusName, statusImage, windDirectName)values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        try:
            cursor.execute(sql.format(city), row)
            db.commit()
        except Exception as e:
            db.rollback()
        finally:
            cursor.close()

# 钉钉警报
def push(details):
    try:
        webhook = "https://oapi.dingtalk.com/robot/send?access_token=e41253fcb4ed841e56c0045c5a3feb60dc149f66fea70288c7ee5bef20a17a59"
        message = "中国天气平台Online\n" + details + "\n" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(message)
        data = {'msgtype': 'text', 'text': {'content': message}}
        data_string = json.dumps(data)
        headers = {"Content-Type": "application/json;charset=utf-8"}
        resp = requests.post(url=webhook, headers=headers, data=data_string, timeout=10, verify=True)
    except:
        pass


if __name__ == '__main__':
    weather = Weather()
    try:
        beijing = json.loads(weather.get_weather(city="北京市"))
        tianjin = json.loads(weather.get_weather(city="天津市"))
        langfang = json.loads(weather.get_weather(city="廊坊市"))
        insertSQL('beijing', beijing)
        insertSQL('tianjin', tianjin)
        insertSQL('langfang', langfang)
        push("数据获取成功\n北京: {} {}\n天津: {} {}\n廊坊: {} {}".format(beijing[0]['date'], beijing[0]['tem'], tianjin[0]['date'], tianjin[0]['tem'], langfang[0]['date'], langfang[0]['tem']))
    except:
        push("数据获取失败")
    db.close()
