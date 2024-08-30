import pandas as pd
# 导入datetime模块
import datetime
import requests
# 获取当前的时间
now = datetime.datetime.now()
# 生成XXXX-XX-XXTXX的格式
filepath = "%Y-%m-%d"
format = "%Y-%m-%dT%H"
#打开当前时段的csv文件
path = "/root/Archive/" + now.strftime(filepath) + "/" + now.strftime(format) + ".csv"
df = pd.read_csv(path)
#筛选出苏州大市范围内的站点数据
df2 = df[df.area=='苏州市']
df3 = df2[['positionname','pm2_5']]
pm2_5average = df3['pm2_5'].mean()
list = []
for i in range(0,11):
    difference = df3.iloc[i, 1] - df3['pm2_5'].mean()
    if difference >20:
        list.append(df3.iloc[i, 0])
if len(list) > 0:
    def send_notice(content):
        token = "d25f816e481b40aaaa239e0eb551aa1e"
        title = "出现高值站点"
        url = f"http://www.pushplus.plus/send?token={token}&title={title}&content={content}&template=html"
        response = requests.request("GET", url)
        print(response.text)
    send_notice(list)
    def send_notice(content):
        token = "d25f816e481b40aaaa239e0eb551aa1e"
        title = "站点具体情况"
        url = f"http://www.pushplus.plus/send?token={token}&title={title}&content={content}&template=html"
        response = requests.request("GET", url)
        print(response.text)
    send_notice(df3)
