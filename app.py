import streamlit as st
import sqlite3
import os
import tempfile

def get_db_structure(db_path):
    """DB에서 테이블과 뷰를 구분하여 가져옵니다."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # 테이블 리스트
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    # 뷰의 생성 SQL 리스트
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='view' AND sql IS NOT NULL;")
    view_sqls = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables, view_sqls

def merge_databases(uploaded_files):
    temp_dir = tempfile.mkdtemp()
    merged_db_path = os.path.join(temp_dir, "merged_database.db")
    target_conn = sqlite3.connect(merged_db_path)
    
    all_view_sqls = []
    temp_file_paths = []

    try:
        # --- 1단계: 모든 테이블 통합 ---
        for uploaded_file in uploaded_files:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
                temp_file_paths.append(tmp_path)
            
            tables, view_sqls = get_db_structure(tmp_path)
            all_view_sqls.extend(view_sqls) # 뷰 SQL은 나중에 쓰기 위해 보관
            
            # 테이블 데이터 복사
            target_conn.execute(f"ATTACH DATABASE '{tmp_path}' AS source_db")
            for table in tables:
                target_conn.execute(f"CREATE TABLE {table} AS SELECT * FROM source_db.{table}")
            target_conn.execute("DETACH DATABASE source_db")

        # --- 2단계: 모든 테이블 생성 후 뷰 생성 ---
        for view_sql in all_view_sqls:
            try:
                target_conn.execute(view_sql)
            except sqlite3.Error as e:
                st.warning(f"뷰 생성 중 알림: {e}")

        target_conn.commit()
    finally:
        target_conn.close()
        # 임시 파일들 청소
        for path in temp_file_paths:
            if os.path.exists(path):
                os.unlink(path)
        
    return merged_db_path

# --- UI 레이아웃 (중복 체크 로직 포함) ---
st.title("🗂️ SQLite 완벽 통합 도구 (Table & View)")

with st.sidebar:
    st.header("설정")
    uploaded_files = st.file_uploader("DB 파일을 선택하세요", 
                                    type=['db', 'sqlite'], 
                                    accept_multiple_files=True)

if uploaded_files:
    all_objects = {} # 테이블과 뷰 이름을 모두 체크
    has_duplicate = False
    
    for uploaded_file in uploaded_files:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = tmp.name
        
        # 중복 체크 시 테이블과 뷰 이름을 모두 추출
        conn = sqlite3.connect(tmp_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view');")
        names = [row[0] for row in cursor.fetchall()]
        conn.close()
        os.unlink(tmp_path)
        
        for name in names:
            if name in all_objects:
                st.error(f"⚠️ 중복 발견: '{name}'(테이블/뷰)이 '{all_objects[name]}'와 '{uploaded_file.name}'에 공통으로 존재합니다.")
                has_duplicate = True
            else:
                all_objects[name] = uploaded_file.name

    if not has_duplicate:
        st.success(f"✅ 총 {len(uploaded_files)}개의 파일 검사 완료. 충돌 없음!")
        
        if st.button("DB 통합 및 다운로드 준비"):
            with st.spinner("테이블 및 뷰 통합 중..."):
                merged_path = merge_databases(uploaded_files)
                
                with open(merged_path, "rb") as f:
                    st.download_button(
                        label="통합된 DB 다운로드",
                        data=f,
                        file_name="merged_complete.db",
                        mime="application/x-sqlite3"
                    )
else:
    st.warning("파일을 업로드해주세요.")
