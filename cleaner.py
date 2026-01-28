import pandas as pd
import hashlib
from config import (COL_MAP
                    ,MAIN_COL as MC,
                    ENVIRONMENT_COL as EC,
                    HUMAN_BEAHAVIOR_COL as HBC,
                    EVENT_PROCESS_PARTICIPATE_OBJECT_COL as EPPOC,
                    EVENT_RESULT_COL as ERC)



# ------------------------------------------------------------------
# 建立對應字典,歷年車禍資料
# ------------------------------------------------------------------
def transform_data_dict(df_year_list)->list:    
    rename_data = []
    
    for df in df_year_list:
        df = df.rename(columns = COL_MAP)
        rename_data.append(df)
    
    return rename_data

# ------------------------------------------------------------------
# 歷年資料清洗
# ------------------------------------------------------------------
def car_crash_old_data_clean(df_year_list):
    primary_data_list = []
    all_parties_list = []
    def create_hash(row):
        input_data = (
            f"{row['accident_year']}|{row['accident_month']}|{row['accident_date']}|"
            f"{row['accident_time']}|{row['speed_limit_primary_party']}|{row['accident_type_minor']}|"
            f"{row['casualties_count']}|{row['longitude']}|{row['latitude']}"
            )
        return hashlib.sha256(input_data.encode('utf-8')).hexdigest()[:16]
    
    for df in df_year_list:
        
        if 'accident_id' not in df.columns:
         # Pandas 會自動把每一列塞進 create_hash 的第一個位置
            df.insert(0, 'accident_id', df.apply(create_hash, axis=1))
            #df.insert(loc, column, value)：
        if 'casualties_count' in df.columns:    
        # 直接找「數字」後面跟著「死」或「傷」的組合
            df['death_count'] = df['casualties_count'].str.extract(r'死亡(\d+)').fillna(0).infer_objects(copy=False).astype(int)
            df['injury_count'] = df['casualties_count'].str.extract(r'受傷(\d+)').fillna(0).infer_objects(copy=False).astype(int)
            df = df.drop(columns=['casualties_count'])

        if 'lane_edge_marking' in df.columns:
            df['lane_edge_marking'] = df['lane_edge_marking'].map({'有': True, '無': False}).fillna(False).infer_objects(copy=False).astype(bool)
        
        
        if 'accident_month' in df.columns:
            df['accident_month'] = df['accident_month'].astype(str).str.replace(r'\.0', '', regex=True)
            #原始字串 r'\.'：Python 看到 r 就會「兩手一攤」，把 \. 原封不動交給 Pandas 的 Regex 引擎。Regex 引擎認得 \. 是小數點
        if 'accident_date' in df.columns:                                       
            df['accident_date'] = df['accident_date'].astype(str).str.replace(r'\.0', '', regex=True).str[-2:]
        if 'accident_time' in df.columns:
            df['accident_time'] = df['accident_time'].astype(str).str.replace(r'\.0', '', regex=True).str.zfill(6)
            #zfill(6)從左側補滿6瑪'0', ex:'142'=>'000142'                       #'\.0', '' ==>把字串中的".0"替換成空字串 
            if 'accident_hour' not in df.columns:
                register=df['accident_time']#暫存在register省效能
                df.insert(4,'accident_hour',register.astype(str).str[0:2])
                df.insert(5,'accident_minute',register.astype(str).str[2:4])
                df.insert(6,'accident_second',register.astype(str).str[4:6])
                df=df.drop(columns=['accident_time'])
        
            if 'accident_datetime' not in df.columns:  
                temp_dt = pd.to_datetime({
                #pd.to_datetime不用寫insert也會自己插入表格
                        'year': df['accident_year'],
                        'month': df['accident_month'],
                        'day': df['accident_date'],
                        'hour': df['accident_hour'],
                        'minute': df['accident_minute'],
                        'second': df['accident_second']
                        }, errors='coerce')
                df.insert(7, 'accident_datetime',temp_dt)
        
        if 'weather_condition' in df.columns:
            df['weather_condition'] = df['weather_condition'].astype(str).str.slice(0, 10)
        
        if 'party_sequence' in df.columns:
            df_primary = df[df['party_sequence']==1].copy()
            #把只含df['party_sequence']==1 的條件存到df_primary這個新的dataframe
            primary_data_list.append(df_primary)
            df_other = df[df['party_sequence']!=1].copy()
            df_other = df_other[~df_other['accident_year'].astype(str).str.contains('資料提供日期|事故類別', na=False)]
            all_parties_list.append(df_other)
            
            
        
        if (not primary_data_list) and (not all_parties_list):
            return pd.DataFrame(), pd.DataFrame()
        

    final_main_df = pd.concat(primary_data_list, ignore_index=True).drop_duplicates(subset=['accident_id'], keep='first')
    final_all_party_df=pd.concat(all_parties_list, ignore_index=True).drop_duplicates(subset=['accident_id','party_sequence'], keep='first')

    return {
    "main": {
        "master": final_main_df[MC],
        "env": final_main_df[EC],
        "human": final_main_df[HBC],
        "process": final_main_df[EPPOC],
        "result": final_main_df[ERC]
    },
    "party": {
        "master": final_all_party_df[MC],
        "env": final_all_party_df[EC],
        "human": final_all_party_df[HBC],
        "process": final_all_party_df[EPPOC],
        "result": final_all_party_df[ERC]
    }
}
        


            
