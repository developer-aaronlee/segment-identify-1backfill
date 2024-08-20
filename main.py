import pandas as pd
import requests
import json
import time

segment_identify = "https://api.segment.io/v1/identify"

qa_headers = {
    "Content-Type": "application/json",
    "Authorization": "Basic api_key"
}

prod_headers = {
    "Content-Type": "application/json",
    "Authorization": "Basic api_key"
}


def monitor(func):

    def wrapper(*args, **kwargs):
        start = time.time()
        print(f"You called function: {func.__name__}{args}")
        result = func(*args, **kwargs)
        total = time.time() - start
        print("Runtime:", total)
        print(f"Function returned: {result}")

    return wrapper


@monitor
def send_data():

    for x in range(len(all_data)):
        data = {
            columns[0]: all_data[x][0],
            "traits": {
                columns[1]: all_data[x][1],
                columns[2]: all_data[x][2],
                columns[3]: all_data[x][3],
                columns[4]: all_data[x][4],
                columns[5]: all_data[x][5],
                columns[6]: all_data[x][6],
                columns[7]: all_data[x][7],
                columns[8]: all_data[x][8],
                columns[9]: all_data[x][9],
                columns[10]: all_data[x][10],
                columns[11]: all_data[x][11],
                columns[12]: all_data[x][12],
                columns[13]: all_data[x][13],
                columns[14]: all_data[x][14],
            }
        }

        body = json.dumps(data)
        # print(body)

        response = requests.post(url=segment_identify, data=body, headers=prod_headers)
        print(f"Row {x + 1}: {all_data[x][0]} Response:", response.json())


df = pd.read_csv("backfill_user_equipment2.csv")
# print(df)

all_data = df.to_numpy()
# print(all_data)

columns = df.columns
# print(columns)

send_data()
