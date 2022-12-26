from datetime import datetime 
import ssl
#ssl._create_default_https_context = ssl._create_unverified_context
import requests
import datetime
import pandas as pd
import numpy as np

# returns the dataframe object for the given url
url = "https://www1.nseindia.com/archives/equities/mto/MTO_" 
datamap = {}
volume_map = {}

def average(numList): 
  av = 0.0 
  n = 1.0 
  for num in numList: 
    av = (n - 1) / n * av + num / n 
    n += 1.0 
  return av 

def get_request(date_suffix):
    global url
    remote_url = url + date_suffix + ".DAT"
    response = requests.request("GET", remote_url,verify=False)
    print("Request for url : ", remote_url)
    print("Response status:", response.status_code)
    return response

def get_date_suffix(date_diff):
    date_delta = datetime.timedelta(days = date_diff)
    date_suffix = str((datetime.datetime.now() - date_delta).strftime ('%d%m%Y'))
    return date_suffix

def get_current_df(diff_days):
    date_suffix = get_date_suffix(diff_days)
    response = get_request(date_suffix)
    if response.status_code != 200:
        print("could not get a valid response for the given URL:", url)
        return pd.DataFrame()
    res = ''.join(response.text.splitlines(keepends=True)[4:])
    file1 = open("daily_data","w")
    file1.write(res)
    file1.close()
    df = pd.read_csv("daily_data")
    df.columns = ["type", "sno","name","segment","traded_total", "delivered_total", "percent_delivery"]
    return df

def compute_average_volume(no_of_days, current_df):
    # i signify the number of days
    # data count reperesent ki kitne dino ka data mil gaya hai
    diff_days = 1
    data_count = 0
    while data_count < no_of_days:
        date_suffix = get_date_suffix(diff_days)
        diff_days += 1
        response = get_request(date_suffix)
        if response.status_code != 200:
            print("could not get a valid response for the given URL:", url)
            continue
        res = ''.join(response.text.splitlines(keepends=True)[4:])
        file1 = open("temp_data","w")
        file1.write(res)
        file1.close()
        temp_df = pd.read_csv("temp_data")
        temp_df.columns = ["type", "sno","name","segment","traded_total", "delivered_total", "percent_delivery"]
        for ind in temp_df.index:
            scrip = temp_df['name'][ind]
            volume = temp_df['traded_total'][ind]
            if scrip in volume_map.keys() and volume_map[scrip] is not None:
                volume_map[scrip].append(volume)
            else:
                volume_map[scrip] = [volume]
        data_count += 1
        
# back_in_days = 2
# date_delta = datetime.timedelta(days = back_in_days)
# date_suffix = str((datetime.datetime.now() - date_delta).strftime ('%d%m%Y'))
# print(date_suffix)
# url = "https://www1.nseindia.com/archives/equities/mto/MTO_" + date_suffix + ".DAT"
# print("Url for todays data = %s",url)
# df = get_dataframe(url)
# if not df.empty:
#     print(df.head(10))
#     rslt_df = df[df['percent_delivery'] > 80]
#     print(rslt_df)
# else:
#     print('DataFrame is empty!')

volume_days = 36
current_df = get_current_df(0)
if current_df.empty:
    print("current diff is empty! exiting")
    exit(0)
# print(current_df.head(10))
compute_average_volume(volume_days, current_df)
current_map = {}
for ind in current_df.index:
    scrip = current_df['name'][ind]
    volume_traded = current_df['traded_total'][ind]
    delivery_percent = current_df['percent_delivery'][ind]
    current_map[scrip] = [scrip,volume_traded, delivery_percent]

# print("current df")
# current_df['average_volume'] = current_df['average_volume'].div(volume_days).round(2)
# print(current_df.head(10))
# rslt_df = current_df[current_df['percent_delivery'] > 80]
# print(rslt_df)
# print(rslt_df.to_string())
# print(current_map)
# print(len(current_map))
res = []

# print(res)
# print(len(res))
for k, v in volume_map.items():
    if len(v) < volume_days:
        continue
    if k not in datamap.keys():
        datamap[k] = []
        datamap[k].append(scrip)
        datamap[k].append(average(v))
# print(volume_map)
# print(datamap)

for scrip, value in current_map.items():
    if scrip in datamap.keys() and value[1] > (datamap[scrip][1]) and value[2] >= 40:
        value.append(datamap[scrip][1])
        res.append(value)
print(res)
print(len(res))