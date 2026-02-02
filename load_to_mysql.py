from sqlalchemy import create_engine  # 負責：建立資料庫連線引擎，是 Python 與 SQL 的橋樑
from sqlalchemy import text
from sqlalchemy import types #types用在python裡定義mysql的資料型別
import pymysql  # 代碼中沒直接用到它的 method，但它是底層驅動
import pandas as pd  # 負責：資料處理，把 CSV 轉成表格，進行切分與清洗
from config import (DB_URL,
                    GCP_DB_URL,
                    MY_IMPORT_TRY,
                    MAIN_TABLE_DICT as MTD,
                    ENVIRONMENT_TABLE_DICT as ETD,
                    HUMAN_BEAHAVIOR_DICT as HBD,
                    EVENT_PROCESS_PARTICIPATE_OBJECT_DICT as EPPOD,
                    EVENT_RESULT_DICT as ERD)
# ------------------------------------------------------------------
# 匯入mysql
# ------------------------------------------------------------------
def load_to_mysql(main_dict, party_dict):
    print(f"--- 階段三：匯入 MySQL ---")
    engine = create_engine(DB_URL)
    try:
        # 寫入主表
        main_dict['master'].to_sql('accident_sq1_main', con=engine, if_exists='append', index=False,dtype= MTD)
        # main_dict['env'].to_sql('accident_sq1_env', con=engine, if_exists='append', index=False,dtype= ETD)
        # main_dict['human'].to_sql('accident_sq1_human', con=engine, if_exists='append', index=False,dtype= HBD)
        # main_dict['process'].to_sql('accident_sq1_process', con=engine, if_exists='append', index=False,dtype= EPPOD)
        # main_dict['result'].to_sql('accident_sq1_res', con=engine, if_exists='append', index=False,dtype= ERD)
        # 寫入細節表
        party_dict['master'].to_sql('accident_sq2_sub', con=engine, if_exists='append', index=False,dtype= MTD)
        # party_dict['env'].to_sql('accident_sq2_env', con=engine, if_exists='append', index=False,dtype= ETD)
        # party_dict['human'].to_sql('accident_sq2_human', con=engine, if_exists='append', index=False,dtype= HBD)
        # party_dict['process'].to_sql('accident_sq2_process', con=engine, if_exists='append', index=False,dtype= EPPOD)
        # party_dict['result'].to_sql('accident_sq2_res', con=engine, if_exists='append', index=False,dtype= ERD)
        print("所有資料已成功寫入資料庫！")
        return engine
    except Exception as e:
        print(f"資料匯入失敗: {e}")
        return None

# ------------------------------------------------------------------
# 設定pk fk
# ------------------------------------------------------------------
def setting_pkfk(engine):
    if engine is None: return
    sub_tables = ['accident_sq1_env', 'accident_sq1_human', 'accident_sq1_process', 'accident_sq1_res',
                  'accident_sq2_sub','accident_sq2_env', 'accident_sq2_human', 'accident_sq2_process', 'accident_sq2_res']    
    with engine.connect() as conn:   
        check_pk = conn.execute(text("""
                SELECT count(*) FROM information_schema.TABLE_CONSTRAINTS 
                WHERE CONSTRAINT_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'accident_sq1_main' 
                AND CONSTRAINT_TYPE = 'PRIMARY KEY'
                """)).scalar()

        if check_pk == 0:
            try:
                conn.execute(text("ALTER TABLE accident_sq1_main MODIFY COLUMN accident_id VARCHAR(16) NOT NULL"))
                conn.execute(text("ALTER TABLE accident_sq1_main ADD PRIMARY KEY (accident_id)"))
                conn.commit() # 確保執行成功
                print("MySQL Primary Key 設定完成！")
            except Exception as e:
                print(f"PK 可能已存在或設定失敗: {e}")
            for table in sub_tables:
                try:
                    print(f"正在為 {table} 設定外鍵...")
                    # 統一修改欄位屬性
                    conn.execute(text(f"ALTER TABLE {table} MODIFY COLUMN accident_id VARCHAR(16) NOT NULL"))
                    # 統一建立 FK 連結到主表
                    conn.execute(text(f"""
                        ALTER TABLE {table} 
                        ADD CONSTRAINT fk_{table}_main 
                        FOREIGN KEY (accident_id) 
                        REFERENCES accident_sq1_main(accident_id)
                        ON DELETE CASCADE
                        """))
                    conn.commit()
                except Exception as e:
                        print(f"FK 可能已存在或設定失敗: {e}")
        else:
            print("✅ 資料庫結構（PK/FK）已存在，無需重複設定。")
#----------------------------------------------------------------
# 匯入 gcp mysql
# ------------------------------------------------------------------
def load_to_GCP_mysql(main_dict, party_dict):
    print(f"--- 階段三：匯入 gcp MySQL ---")
    engine = create_engine(GCP_DB_URL,pool_pre_ping=True,  # 核心：確保連線有效
                           pool_recycle=300,                # 每 5 分鐘強制重整連線
                           connect_args={'connect_timeout': 60})
    
        # 寫入主表
    try:
        with engine.begin() as connection:
        # 使用 Transaction 確保資料完整性
            main_dict['master'].to_sql('accident_main', con=connection, if_exists='append', index=False,dtype= MTD,chunksize=200)
            # main_dict['env'].to_sql('accident_sq1_env', con=connection, if_exists='append', index=False,dtype= ETD,chunksize=200)
            # main_dict['human'].to_sql('accident_sq1_human', con=connection, if_exists='append', index=False,dtype= HBD,chunksize=200)
            # main_dict['process'].to_sql('accident_sq1_process', con=connection, if_exists='append', index=False,dtype= EPPOD,chunksize=200)
            # main_dict['result'].to_sql('accident_sq1_res', con=connection, if_exists='append', index=False,dtype= ERD,chunksize=200)

                # 寫入細節表

            party_dict['master'].to_sql('accident_sub', con=connection, if_exists='append', index=False,dtype= MTD,chunksize=200)
            # party_dict['env'].to_sql('accident_sq2_env', con=connection, if_exists='append', index=False,dtype= ETD,chunksize=200)
            # party_dict['human'].to_sql('accident_sq2_human', con=connection, if_exists='append', index=False,dtype= HBD,chunksize=200)
            # party_dict['process'].to_sql('accident_sq2_process', con=connection, if_exists='append', index=False,dtype= EPPOD,chunksize=200)
            # party_dict['result'].to_sql('accident_sq2_res', con=connection, if_exists='append', index=False,dtype= ERD,chunksize=200)
            #要加chunksize=500,不然上傳雲端會卡住(一次上傳500列資料)
        print("所有資料已成功寫入資料庫！")
        
        return engine
    except Exception as e:
        print(f"匯入失敗: {e}")
        return None
    

def get_existing_years(engine):
    with engine.connect() as conn:
        # 檢查表格是否存在，避免第一次執行就噴錯
        table_exists = conn.execute(text("SHOW TABLES LIKE 'accident_sq1_main'")).scalar()
        if not table_exists:
            return []
            
        # 抓取所有已存在的年份
        result = conn.execute(text("SELECT DISTINCT accident_year FROM accident_sq1_main"))
        return [row[0] for row in result]

        