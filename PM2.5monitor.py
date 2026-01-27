# -*- coding: utf8 -*-
import requests
import json
import pandas as pd
import numpy as np
from pathlib import Path
import datetime
import urllib.parse
import os

requests.packages.urllib3.disable_warnings()

headers = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Connection': 'keep-alive',
    'Origin': 'https://air.cnemc.cn:18007',
    'Referer': 'https://air.cnemc.cn:18007/',
    'User-Agent': 'Mozilla/5.0',
    'X-Requested-With': 'XMLHttpRequest',
}

# ============================
# ① 采集 + 清洗 + 归档
# ============================

r = requests.post(
    'https://air.cnemc.cn:18007/HourChangesPublish/GetAllAQIPublishLive',
    headers=headers,
    verify=False
)

data_dict = json.loads(r.content)
df = pd.DataFrame.from_dict(data_dict)

df.columns = df.columns.map(str.lower)

df['timepoint'] = (
    df['timepointstr']
    .str.replace('年', '-')
    .str.replace('月', '-')
    .str.replace('日 ', 'T')
    .str.replace('时', '00')
)

# 异常情况：同一批数据出现多个时间点
if len(df['timepoint'].unique()) > 1:
    for t in df['timepoint'].unique():
        timestamp = t[:13]
        daily_folder = Path('/data/Error') / timestamp[:10]
        daily_folder.mkdir(parents=True, exist_ok=True)
        df.to_csv(daily_folder / (timestamp + '.csv'), index=None)

# 规范字段
df_ = df[['timepoint', 'stationcode', 'longitude', 'latitude',
          'area', 'positionname', 'primarypollutant', 'aqi',
          'pm10', 'pm10_24h', 'pm2_5', 'pm2_5_24h',
          'o3', 'o3_24h', 'o3_8h', 'o3_8h_24h',
          'no2', 'no2_24h', 'so2', 'so2_24h',
          'co', 'co_24h']].copy()

df_ = df_.where(df_ != '—', np.nan)

# 保存到 Archive
timestamp = df['timepoint'].unique()[-1][:13]
daily_folder = Path('data') / timestamp[:10]
daily_folder.mkdir(parents=True, exist_ok=True)

csv_path = daily_folder / (timestamp + '.csv')

if csv_path.exists():
    df_.to_csv(csv_path, index=None, mode='a', header=False)
else:
    df_.to_csv(csv_path, index=None, mode='w')

print("数据已归档:", csv_path)

# ============================
# ② 读取刚生成的 CSV → 分析 → 推送
# ============================

df = pd.read_csv(csv_path)

# 筛选苏州市
df2 = df.loc[df.area == '苏州市', ['positionname', 'pm2_5']].copy()

if df2.empty:
    print("苏州市数据为空，跳过推送")
    exit()

# 计算平均值
pm2_5average = df2['pm2_5'].mean()

# 计算差值
df2['diff'] = df2['pm2_5'] - pm2_5average

# 筛选高值站点
high_df = df2[df2['diff'] > 8]

# 生成推送内容（换行格式）
content = "\n".join(
    f"{row['positionname']} PM2.5={row['pm2_5']} Δ={row['diff']:.1f}"
    for _, row in high_df.iterrows()
)

# Bark 推送
def send_notice(content, title="消息提醒"):
    bark_server = os.environ.get("BARK_SERVER")
    bark_key = os.environ.get("BARK_KEY")

    if not bark_server or not bark_key:
        print("错误: 未找到 BARK_SERVER 或 BARK_KEY 环境变量")
        return

    content_encoded = urllib.parse.quote(str(content))
    title_encoded = urllib.parse.quote(str(title))
    url = f"https://{bark_server}/{bark_key}/{title_encoded}/{content_encoded}?group=大气站点监控"

    # --- 重试机制开始 ---
    max_retries = 3  # 最大重试次数
    for attempt in range(1, max_retries + 1):
        try:
            # 增加 timeout 时间，防止因为太慢而报错
            response = requests.get(url, timeout=10)
            
            # 检查 HTTP 状态码，如果不是 200 也视为失败
            if response.status_code == 200:
                print(f"Bark 推送成功: {response.text}")
                return # 成功后直接结束函数
            else:
                print(f"Bark 返回非 200 状态: {response.status_code}")
                
        except Exception as e:
            print(f"第 {attempt} 次推送失败: {e}")
        
        # 如果还没到最后一次尝试，就等待一会儿再试
        if attempt < max_retries:
            wait_time = 5 * attempt # 第一次等5秒，第二次等10秒...
            print(f"等待 {wait_time} 秒后重试...")
            time.sleep(wait_time)
    
    print("重试多次后仍然失败，放弃推送。")
    # --- 重试机制结束 ---

if not high_df.empty:
    send_notice(content, title="出现高值站点")
else:
    print("无高值站点")








