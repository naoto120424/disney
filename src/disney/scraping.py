"""
参考サイト: https://nttdocomo-developers.jp/entry/202212241200_2
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString
import re
import datetime
import time
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import os
import jpholiday


def isBizDay(date):
    if date.weekday() >= 5 or jpholiday.is_holiday(date):
        return 1
    else:
        return 0


# カラム名の定義をする関数
# 各表の最初の日付でのみ呼び出される
def define_df_columns(tables):
    df = pd.DataFrame()
    cols = []
    check_columns = []
    for tr in tables:
        if type(tr) is not NavigableString:
            for factor in tr:
                if type(factor) is not NavigableString:
                    check_columns = factor.find_all("th", attrs={"class": re.compile(r"^t_.*$")})  # カラム情報
                    for check_column in check_columns:
                        cols.append(check_column.get_text())
    if len(check_columns) > 0:
        df = pd.DataFrame(columns=cols)

    return df, cols


# アトラクション別 - 待ち時間の追加
def add_attraction_timedata(attraction_time_data, df, cols, date):
    tmp_list = []
    tmp_df = pd.DataFrame()
    for attraction_time in attraction_time_data:
        if (attraction_time.get_text()) != "時間":
            tmp_list.append(attraction_time.get_text())
    if len(tmp_list) > 0:
        tmp_df = pd.DataFrame(tmp_list).T
        tmp_df.columns = cols

        # tmp_dfに日毎の処理追加。"時間" → "日付 + 時刻"
        date = datetime.datetime.strptime(str(date), "%Y%m%d")
        date = date.strftime("%Y-%m-%d")
        tmp_df["時間"] = date + " " + tmp_df["時間"]

    df = pd.concat([df, tmp_df])

    return df


# テーブルをparseし, 必要情報の取得
def parse_tables(tables, df, cols, date):
    for tr in tables:
        if type(tr) is not NavigableString:
            for factor in tr:
                if type(factor) is not NavigableString:
                    attraction_time_data = factor.find_all(["th", "td"], attrs={"class": re.compile(r"^(?!t_).*$")})
                    df = add_attraction_timedata(attraction_time_data, df, cols, date)

    return df


def get_csv(year, month):
    # 使用する表の数を指定
    # 表6以降はグリーティング・スタンバイパス・プライオリティパス系列なので表5まで
    n_tables = 5

    # 初めの日と、最後の日をここで指定
    start_day = datetime.datetime(year, month, 1)
    end_day = start_day + relativedelta(months=1) - datetime.timedelta(days=1)

    print()
    print(f"Start Scraping {year}/{month}")
    print("-------------------------------------------------------")

    for i in tqdm(range((end_day - start_day).days + 1)):
        date = int(str((start_day + datetime.timedelta(i)).date()).replace("-", ""))
        urlName = "https://urtrip.jp/tds-past-info/?rm=" + str(date) + "#page_top"
        url = requests.get(urlName)
        soup = BeautifulSoup(url.content, "html.parser")
        df = pd.DataFrame()

        try:
            for n in range(n_tables):
                df_tmp = pd.DataFrame()
                cols = []
                tables = soup.findAll("table", "t_cool")[n]

                df_tmp, cols = define_df_columns(tables)
                df_tmp = parse_tables(tables, df_tmp, cols, date)

                df = df_tmp if n == 0 else pd.merge(df, df_tmp)
        except:
            pass

        # 各表からDataFrameに格納したデータを結合する
        df_dis = df if i == 0 else pd.concat([df_dis, df], ignore_index=True)
        time.sleep(0.1)  # サーバ処理負荷軽減のため, 各アクセス0.1秒は空ける

    print("-------------------------------------------------------")
    print("Finish Scraping")
    print()

    df_dis = df_dis.rename(columns=lambda a: a.replace("\n", ""))

    # datetimeへ変換
    df_dis["時間"] = pd.to_datetime(df_dis["時間"])

    # 月曜日 → 0、日曜日 → 6
    df_dis["曜日_数値"] = df_dis["時間"].dt.weekday

    # 運休 / 案内終了は0分とする
    for df_col in df_dis.columns:
        if df_col != "時間":
            df_dis.loc[((df_dis[df_col] == "案内終了") | (df_dis[df_col] == "－") | (df_dis[df_col] == "一時運休") | (df_dis[df_col] == "計画運休") | (df_dis[df_col] == "")), [df_col]] = 0
            df_dis[df_col] = df_dis[df_col].astype("int")

    # 休日判定
    df_dis["休日"] = df_dis["時間"].map(isBizDay)

    os.makedirs("data", exist_ok=True)
    df_dis.to_csv(os.path.join("/Users/naotonaka/Library/CloudStorage/OneDrive-HiroshimaUniversity/code/disney/data", f'{datetime.datetime.strftime(start_day, "%Y%m")}_DisneySea.csv'))


def main():
    start_year, start_month = 2020, 12
    end_year, end_month = 2024, 2

    start_day = datetime.datetime(start_year, start_month, 1)

    for i in range((end_year - start_year) * 12 + (end_month - start_month) + 1):
        now = start_day + relativedelta(months=i)
        get_csv(now.year, now.month)


if __name__ == "__main__":
    main()
