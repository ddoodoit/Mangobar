import streamlit as st
import xml.etree.ElementTree as ET
import time
import requests
import sqlite3
import pandas as pd
from rapidfuzz import fuzz
import re
import os

PAGE_SIZE = 200
I2861_SERVICE_ID = "I2861"
DB_URL = "https://drive.google.com/uc?export=download&id=1cjYTpM40hMOs817KvSOWq1HmLkvUdCXn"
DB_PATH = "mangobardata.db"

def download_db():
    if not os.path.exists(DB_PATH):
        r = requests.get(DB_URL)
        with open(DB_PATH, "wb") as f:
            f.write(r.content)

download_db()  # ✅ 앱 시작 시 자동 다운로드


st.set_page_config(page_title="MangoBar 웹 검색", layout="wide")


def load_data(selected_regions, query_addr, query_bssh, page=1):
    conn = sqlite3.connect(DB_PATH)

    # region_condition과 파라미터 분리 처리
    if selected_regions:
        region_conditions = []
        params_region = []
        for region in selected_regions:
            prefix = region[:4].lower() + '%'
            region_conditions.append("_ADDR_LOWER LIKE ?")
            params_region.append(prefix)
        region_condition = "(" + " OR ".join(region_conditions) + ")"
    else:
        region_condition = "1=1"
        params_region = []

    query_addr = query_addr.lower() if query_addr else ""
    query_bssh_norm = query_bssh.replace(" ", "").lower() if query_bssh else ""

    sql_i2500 = f"""
        SELECT LCNS_NO, INDUTY_CD_NM, BSSH_NM, ADDR, PRMS_DT
        FROM i2500
        WHERE {region_condition}
        AND _ADDR_LOWER LIKE ?
        AND _BSSH_NORM LIKE ?
    """

    sql_i2819 = f"""
        SELECT LCNS_NO, INDUTY_NM, BSSH_NM, LOCP_ADDR, PRMS_DT, CLSBIZ_DT, CLSBIZ_DVS_CD_NM
        FROM i2819
        WHERE {region_condition}
        AND _ADDR_LOWER LIKE ?
        AND _BSSH_NORM LIKE ?
    """

    params = params_region + [f"%{query_addr}%", f"%{query_bssh_norm}%"]

    df_i2500 = pd.read_sql_query(sql_i2500, conn, params=params)
    df_i2819 = pd.read_sql_query(sql_i2819, conn, params=params)

    conn.close()

    df_i2500_display = df_i2500.rename(columns={
        "LCNS_NO": "인허가번호",
        "INDUTY_CD_NM": "업종",
        "BSSH_NM": "업소명",
        "ADDR": "주소",
        "PRMS_DT": "허가일자",
    })

    df_i2819_display = df_i2819.rename(columns={
        "LCNS_NO": "인허가번호",
        "INDUTY_NM": "업종",
        "BSSH_NM": "업소명",
        "LOCP_ADDR": "주소",
        "PRMS_DT": "허가일자",
        "CLSBIZ_DT": "폐업일자",
        "CLSBIZ_DVS_CD_NM": "폐업상태",
    })

    df_i2500_display["_BSSH_NORM"] = df_i2500_display["업소명"].fillna("").str.replace(" ", "").str.lower()
    df_i2819_display["_BSSH_NORM"] = df_i2819_display["업소명"].fillna("").str.replace(" ", "").str.lower()

    return df_i2500_display, df_i2819_display



from rapidfuzz import fuzz

def fuzzy_search(df, query, threshold=75):
    query_norm = query.replace(" ", "").lower()
    results = []
    for idx, row in df.iterrows():
        name = row["_BSSH_NORM"]
        score = fuzz.token_set_ratio(query_norm, name)
        if score >= threshold:
            results.append(idx)
    return df.loc[results]


def main():
    st.title("🍊 MangoBar 웹 검색")

    if "api_key" not in st.session_state:
        st.session_state.api_key = None

    if "has_rerun" not in st.session_state:
        st.session_state.has_rerun = False

    if st.session_state.api_key is None:
        with st.form("api_key_form"):
            api_input = st.text_input("식품안전나라 인증키를 입력하세요", type="password")
            submit = st.form_submit_button("인증")
        if submit:
            clean_api_key = api_input.strip()
            if clean_api_key:
                st.session_state.api_key = clean_api_key
                if not st.session_state.has_rerun:
                    st.session_state.has_rerun = True
                    st.rerun()
            else:
                st.warning("인증키를 입력해주세요.")
        return

    with st.form("search_form"):
        selected_regions = st.multiselect("시·도를 선택하세요", options=[
            "서울특별시", "경기도", "인천광역시", "세종특별자치시", "부산광역시",
            "대구광역시", "광주광역시", "대전광역시", "울산광역시",
            "강원특별자치도", "충청북도", "충청남도",
            "전북특별자치도", "전라남도", "경상북도", "경상남도", "제주특별자치도"
        ])
        query_addr = st.text_input("주소를 입력하세요").strip().lower()
        query_bssh = st.text_input("업소명을 입력하세요").strip().replace(" ", "").lower()

        search_submitted = st.form_submit_button("검색")
    
    if search_submitted:  # ✅ 이 안에 검색 로직 전체 넣기
        if not selected_regions:
            st.warning("최소 하나의 시·도를 선택하세요.")
            return
        if not query_addr and not query_bssh:
            st.warning("주소 또는 업소명을 입력하세요.")
            return
    
        df_i2500, df_i2819 = load_data(selected_regions, query_addr, query_bssh, page=1)

        if query_bssh:
            query_words = re.findall(r'\w+', query_bssh.lower())

            # 단어 포함 검사만
            mask_unordered_2500 = df_i2500['_BSSH_NORM'].apply(
                lambda x: all(word in x for word in query_words)
            )
            df_i2500_filtered = df_i2500[mask_unordered_2500]
            df_i2500_filtered = fuzzy_search(df_i2500_filtered, query_bssh, threshold=80)
            df_i2500 = df_i2500_filtered

            mask_unordered_2819 = df_i2819['_BSSH_NORM'].apply(
                lambda x: all(word in x for word in query_words)
            )
            df_i2819_filtered = df_i2819[mask_unordered_2819]
            df_i2819_filtered = fuzzy_search(df_i2819_filtered, query_bssh, threshold=80)
            df_i2819 = df_i2819_filtered

        st.success(f"검색 완료: 정상 {len(df_i2500)}개 / 폐업 {len(df_i2819)}개")

        st.write("### 영업/정상")
        st.dataframe(df_i2500.drop(columns=["_BSSH_NORM", "_BSSH_LOWER"], errors='ignore'), use_container_width=True)


        st.write("### 폐업")
        st.dataframe(df_i2819.drop(columns=["_BSSH_NORM", "_BSSH_LOWER"], errors='ignore'), use_container_width=True)


if __name__ == "__main__":
    main()

