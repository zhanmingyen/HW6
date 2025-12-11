# app.py
import requests
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

# -----------------------------
# 設定區
# -----------------------------

# 中央氣象局 F-A0010-001 檔案 API
CWA_API_URL = "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/F-A0010-001"

# ★老師示範金鑰（你也可以改成自己的）
CWA_API_KEY = "CWA-2E3CED11-CE2F-419C-ABED-3EF61140BA06"

# SQLite 資料庫檔名
DB_PATH = Path("data.db")


# -----------------------------
# 第 1 步：下載中央氣象局 JSON
# -----------------------------
def download_weather_json() -> dict:
    params = {
        "Authorization": CWA_API_KEY,
        "downloadType": "WEB",
        "format": "JSON",
    }
    resp = requests.get(CWA_API_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


# -----------------------------
# 第 2 步：解析 JSON → Python list
# 每筆資料格式：
# {
#   "location": "臺北市",
#   "min_temp": 23.0,
#   "max_temp": 30.0,
#   "description": "多雲短暫陣雨"
# }
# -----------------------------

def _get_root_locations(data: dict):
    """
    同時處理兩種常見結構：
    1) fileapi 版本: data["cwaopendata"]["dataset"]["location"]
    2) rest api 版本: data["records"]["location"]
    作為保險，避免老師 JSON 結構略有差異。
    """
    if "cwaopendata" in data:
        dataset = data["cwaopendata"].get("dataset", {})
        return dataset.get("location", [])
    if "records" in data:
        return data["records"].get("location", [])
    return []


def _get_first_time_value(time_list):
    """
    從 time 陣列裡面，拿第一筆的數值。
    可能有兩種形式：
      - time[i]["parameter"]["parameterName"]
      - time[i]["elementValue"][0]["value"] 或 elementValue["value"]
    """
    if not time_list:
        return None

    t0 = time_list[0]

    # 形式 1：parameter
    if isinstance(t0.get("parameter"), dict):
        return t0["parameter"].get("parameterName")

    # 形式 2：elementValue（可能是 list 或 dict）
    ev = t0.get("elementValue")
    if isinstance(ev, list) and ev:
        return ev[0].get("value")
    if isinstance(ev, dict):
        return ev.get("value")

    return None


def parse_weather_json(data: dict):
    locations = _get_root_locations(data)
    result_rows = []

    for loc in locations:
        name = loc.get("locationName", "未知地點")
        weather_elements = loc.get("weatherElement", [])

        row = {
            "location": name,
            "min_temp": None,
            "max_temp": None,
            "description": None,
        }

        for elem in weather_elements:
            elem_name = elem.get("elementName")
            val = _get_first_time_value(elem.get("time", []))
            if val is None:
                continue

            if elem_name == "MinT":
                # 攝氏溫度，轉 float (失敗就先當作字串)
                try:
                    row["min_temp"] = float(val)
                except ValueError:
                    row["min_temp"] = val
            elif elem_name == "MaxT":
                try:
                    row["max_temp"] = float(val)
                except ValueError:
                    row["max_temp"] = val
            elif elem_name in ("Wx", "WeatherDescription"):
                row["description"] = val

        result_rows.append(row)

    return result_rows


# -----------------------------
# 第 3 步：建立 SQLite 資料庫 / 資料表
# -----------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            min_temp REAL,
            max_temp REAL,
            description TEXT
        );
        """
    )

    conn.commit()
    conn.close()


# -----------------------------
# 第 4 步：把資料寫進 SQLite
# -----------------------------
def save_weather_to_db(rows):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 先清空舊資料，保持資料庫內容是「本次最新下載」
    cur.execute("DELETE FROM weather;")

    for row in rows:
        cur.execute(
            """
            INSERT INTO weather (location, min_temp, max_temp, description)
            VALUES (?, ?, ?, ?);
            """,
            (
                row.get("location"),
                row.get("min_temp"),
                row.get("max_temp"),
                row.get("description"),
            ),
        )

    conn.commit()
    conn.close()


# -----------------------------
# 第 5 步：從 SQLite 把資料讀出來（給 Streamlit 使用）
# -----------------------------
def load_weather_from_db() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT id, location, min_temp, max_temp, description FROM weather;",
        conn,
    )
    conn.close()
    return df


# -----------------------------
# Streamlit 主程式
# -----------------------------
def main():
    st.set_page_config(page_title="CWA 天氣資料（SQLite + Streamlit）", layout="wide")

    st.title("中央氣象局 F-A0010-001 天氣資料 Demo")
    st.caption("Lecture 13 — 資料爬蟲 + SQLite + Streamlit（Part 1）")

    # 左右欄位：左邊控制下載 / 更新，右邊顯示資料表
    col_left, col_right = st.columns([1, 3])

    with col_left:
        st.subheader("資料更新")

        if st.button("下載最新 JSON 並寫入 SQLite"):
            try:
                st.write("⏬ 正在下載中央氣象局 JSON ...")
                data = download_weather_json()

                st.write("🧩 正在解析 JSON ...")
                rows = parse_weather_json(data)

                st.write("💾 正在寫入 SQLite（data.db）...")
                init_db()
                save_weather_to_db(rows)

                st.success(f"完成！共寫入 {len(rows)} 筆資料。")
            except Exception as e:
                st.error(f"發生錯誤：{e}")

        st.markdown("---")
        st.markdown("📌 **說明**")
        st.markdown(
            """
            - 使用資料集：`F-A0010-001`（中央氣象局 Open Data）
            - 先下載 JSON → 解析出各地區的 MinT / MaxT / Wx
            - 資料存進 `data.db` 的 `weather` 資料表
            - 右邊表格是「從 SQLite 讀出來」的結果
            """
        )

    with col_right:
        st.subheader("SQLite 中的天氣資料表")

        if not DB_PATH.exists():
            st.info("目前還沒有找到 `data.db`，請先在左邊按下「下載最新 JSON 並寫入 SQLite」。")
        else:
            df = load_weather_from_db()

            if df.empty:
                st.warning("資料表目前是空的，請先按左邊的更新按鈕。")
            else:
                st.dataframe(df, use_container_width=True)
                st.caption("↑ 從 SQLite `data.db` 讀出的 `weather` 資料表")

    st.markdown("---")
    st.caption("請記得截圖：畫面要包含 Streamlit 介面 + 天氣資料表。")


if __name__ == "__main__":
    # 確保第一次執行就有資料庫結構
    init_db()
    main()
