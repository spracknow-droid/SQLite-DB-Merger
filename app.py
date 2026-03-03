import streamlit as st
import sqlite3
import os
import tempfile

def get_table_names(db_path):
    """DB 파일 내의 모든 테이블 이름을 반환합니다."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables

def merge_databases(uploaded_files):
    """여러 DB 파일을 하나로 통합합니다."""
    # 임시 디렉토리에 통합될 결과 DB 생성
    temp_dir = tempfile.mkdtemp()
    merged_db_path = os.path.join(temp_dir, "merged_database.db")
    
    target_conn = sqlite3.connect(merged_db_path)
    
    try:
        for uploaded_file in uploaded_files:
            # 업로드된 파일을 임시 파일로 저장 (sqlite3 연결을 위해)
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            
            # 원본 DB에서 데이터를 읽어와 대상 DB로 복사
            source_conn = sqlite3.connect(tmp_path)
            # SQLite의 ATTACH DATABASE 기능을 사용하여 테이블 복사
            target_conn.execute(f"ATTACH DATABASE '{tmp_path}' AS source_db")
            
            tables = get_table_names(tmp_path)
            for table in tables:
                target_conn.execute(f"CREATE TABLE {table} AS SELECT * FROM source_db.{table}")
            
            target_conn.execute("DETACH DATABASE source_db")
            source_conn.close()
            os.unlink(tmp_path) # 임시 파일 삭제
            
        target_conn.commit()
    finally:
        target_conn.close()
        
    return merged_db_path

# --- UI 레이아웃 ---
st.title("🗂️ SQLite DB 통합 도구")
st.info("여러 개의 SQLite 파일을 업로드하면 테이블 중복을 확인하고 하나로 합쳐줍니다.")

# 사이드바에서 파일 업로드
with st.sidebar:
    st.header("설정")
    uploaded_files = st.file_uploader("DB 파일을 선택하세요 (.db, .sqlite)", 
                                    type=['db', 'sqlite'], 
                                    accept_multiple_files=True)

if uploaded_files:
    all_tables = {}
    has_duplicate = False
    
    # 1) 중복 검토
    for uploaded_file in uploaded_files:
        # 임시로 파일 저장하여 테이블 이름 추출
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = tmp.name
        
        tables = get_table_names(tmp_path)
        os.unlink(tmp_path)
        
        for table in tables:
            if table in all_tables:
                st.error(f"⚠️ 중복 발견: '{table}' 테이블이 '{all_tables[table]}'와 '{uploaded_file.name}'에 모두 존재합니다.")
                has_duplicate = True
            else:
                all_tables[table] = uploaded_file.name

    # 2) 중복이 없을 경우 처리
    if not has_duplicate:
        st.success(f"✅ 총 {len(uploaded_files)}개의 파일에서 중복이 발견되지 않았습니다.")
        
        if st.button("DB 통합하기"):
            with st.spinner("통합 작업 중..."):
                merged_path = merge_databases(uploaded_files)
                
                # 3) 다운로드 버튼 생성
                with open(merged_path, "rb") as f:
                    st.download_button(
                        label="통합된 DB 다운로드",
                        data=f,
                        file_name="merged_database.db",
                        mime="application/x-sqlite3"
                    )
else:
    st.warning("왼쪽 사이드바에서 DB 파일들을 먼저 업로드해주세요.")
