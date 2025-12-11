import requests
import pandas as pd
import streamlit as st
import sqlite3
from pathlib import Path
import urllib3

# 關閉 SSL 驗證警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -----------------------------
# 設定區
# -----------------------------

# API URL 設定為「局屬氣象站-氣象觀測資料」
CWA_API_URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0003-001"
CWA_API_KEY = "CWA-2E3CED11-CE2F-419C-ABED-3EF61140BA06"
DB_PATH = Path("data.db")

# -----------------------------
# 資料獲取與解析
# -----------------------------
def download_observation_json() -> dict:
    params = {"Authorization": CWA_API_KEY, "format": "JSON"}
    resp = requests.get(CWA_API_URL, params=params, timeout=15, verify=False)
    resp.raise_for_status()
    return resp.json()

def get_locations_from_records(data: dict):
    """從 records 中安全地取出 Station 列表"""
    return data.get("records", {}).get("Station", [])


def parse_observation_json(data: dict):
    """解析觀測資料，取出站名、站ID、溫度和觀測時間"""
    locations = get_locations_from_records(data)
    result_rows = []

    for loc in locations:
        temp_value = loc.get("WeatherElement", {}).get("AirTemperature")

        if temp_value is None or temp_value in ("-99", "-999"):
            continue

        row = {
            "station_id": loc.get("StationId"),
            "location_name": loc.get("StationName"),
            "temperature": None,
            "obs_time": loc.get("ObsTime", {}).get("DateTime"),
        }

        try:
            row["temperature"] = float(temp_value)
        except (ValueError, TypeError):
            row["temperature"] = temp_value
        
        result_rows.append(row)

    return result_rows

# -----------------------------
# 資料庫操作
# -----------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            location_name TEXT,
            temperature REAL,
            obs_time TEXT
        );
    """)
    conn.commit()
    conn.close()

def save_weather_to_db(rows):
    try:
        st.write(f"💾 save_weather_to_db: 收到 {len(rows)} 筆資料。")
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        cur.execute("DELETE FROM weather_observations;")
        st.write(f"  - 清空舊資料，影響 {cur.rowcount} 行。")
        
        for i, row in enumerate(rows):
            cur.execute(
                """
                INSERT INTO weather_observations (station_id, location_name, temperature, obs_time)
                VALUES (?, ?, ?, ?);
                """,
                (
                    row.get("station_id"),
                    row.get("location_name"),
                    row.get("temperature"),
                    row.get("obs_time"),
                ),
            )
            # Log every 20 inserts to avoid flooding the UI
            if (i + 1) % 20 == 0:
                st.write(f"  - 已插入 {i + 1} 筆...")

        conn.commit()
        st.write("✅ 資料庫 commit 成功。")
    except sqlite3.Error as e:
        st.error(f"資料庫錯誤: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def load_weather_from_db() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT id, station_id, location_name, temperature, obs_time FROM weather_observations;",
        conn,
    )
    conn.close()
    return df

# -----------------------------
# Streamlit 主程式
# -----------------------------
def main():
    st.set_page_config(page_title="CWA 即時溫度觀測", layout="wide")
    st.title("中央氣象署 - 即時溫度觀測資料")
    st.caption("資料來源：局屬氣象站-氣象觀測資料 (O-A0003-001)")

    col_left, col_right = st.columns([1, 3])

    with col_left:
        st.subheader("資料更新")
        if st.button("下載最新觀測資料並寫入資料庫"):
            try:
                st.write("⏬ 正在下載中央氣象署 JSON ...")
                data = download_observation_json()
                st.write("🧩 正在解析 JSON ...")
                rows = parse_observation_json(data)
                
                if rows:
                    st.write("💾 正在寫入 SQLite 資料庫...")
                    init_db()
                    save_weather_to_db(rows)
                    st.success(f"完成！共寫入 {len(rows)} 筆測站資料。")
                else:
                    st.warning("解析完成，但沒有收到任何有效的測站資料。")
                    st.subheader("API 原始回傳資料")
                    st.json(data)

            except Exception as e:
                st.error(f"發生錯誤：{e}")

    with col_right:
        st.subheader("資料庫中的天氣觀測資料")
        if not DB_PATH.exists():
            st.info("資料庫檔案不存在，請先點擊左側按鈕下載資料。")
        else:
            df = load_weather_from_db()
            if df.empty:
                st.warning("資料庫目前是空的，請先按左側按鈕更新。")
            else:
                st.dataframe(df, use_container_width=True)
                st.caption("↑ 從 SQLite data.db 讀出的 weather_observations 資料表")
    
    st.markdown("---")
    st.caption("請記得截圖：畫面要包含 Streamlit 介面 + 天氣資料表。")

if __name__ == "__main__":
    init_db() # 確保程式啟動時資料庫表格已建立
    main()