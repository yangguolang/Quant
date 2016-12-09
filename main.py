import fred , requests
from bs4 import BeautifulSoup
import mysql.connector


def analysis_categories_child(categories):
    for categories_item in categories:
        insert_sql("category", categories_item)
        child = fred.children(categories_item["id"])['categories']
        if len(child):
            analysis_categories_child(child)
        else:
            analysis_series_child(categories_item["id"])


def analysis_series_child(series_id):
    seriess = fred.category_series(series_id)["seriess"]
    for item_series in seriess:
        item_series["parent_id"] = series_id
        insert_sql('series', item_series)
        analysis_observations_child(item_series['id'])


def analysis_observations_child(observations_id):
    observations = fred.observations(observations_id)["observations"]
    for item_observations in observations:
        item_observations["id"]=observations_id
        insert_sql('observations', item_observations)


def conten_mysql():
    config = {'host': '192.168.4.230',  # 默认127.0.0.1
              'user': 'root',
              'password': 'root',
              'port': 3306,  # 默认即为3306
              'database': 'stlouisfed.org',
              'charset': 'utf8'  # 默认即为utf8
              }
    try:
        conn = mysql.connector.connect(**config)
    except mysql.connector.Error as e:
        print('connect fails!{}'.format(e))
    return conn



def insert_sql(itemtype, data):
    if itemtype == 'category':
        sql_value = """insert into category(parent_id, id, name) values (%(parent_id)s, %(id)s, %(name)s)"""
    if itemtype == 'series':
        sql_value = """
insert into series(id, realtime_start, realtime_end, title ,observation_start, observation_end, frequency, frequency_short, units, units_short, seasonal_adjustment, seasonal_adjustment_short,
last_updated, popularity,  parent_id)
values ( %(id)s, %(realtime_start)s, %(realtime_end)s, %(title)s,%(observation_start)s, %(observation_end)s, %(frequency)s, %(frequency_short)s, %(units)s, %(units_short)s, %(seasonal_adjustment)s, %(seasonal_adjustment_short)s,%(last_updated)s, %(popularity)s,  %(parent_id)s)
"""
    if itemtype == "observations":
        sql_value = """insert into observations(id,value,realtime_start,realtime_end,date)values(%(id)s,%(value)s,%(realtime_start)s,%(realtime_end)s,%(date)s)"""
    print("The current write data type is  :", itemtype, "ID:", data["id"])
    sql_contenn.cursor().execute(sql_value, data)
    sql_contenn.commit()


# 初始数据收集 爬虫
headers = {'User-Agent': """Mozilla/5.0 (Windows NT 6.1; WOW64)
 AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"""}
all_url = """https://fred.stlouisfed.org/categories"""
start_html = requests.get(all_url, headers)
soup = BeautifulSoup(start_html.text, 'lxml')
all_parent = soup.find_all("p",  class_="large fred-categories-parent")

# fred 连接数据读取
categoriesParentList = []
sql_contenn = conten_mysql()
fred.key("6adc195cd52a987889011f75e9b3fa48")
for a in all_parent:
    for item in (fred.categories(a.find("a")['href'][12:]))['categories']:
        categoriesParentList.append(item)
analysis_categories_child(categoriesParentList)\

#用户与测试的鸟蛋



