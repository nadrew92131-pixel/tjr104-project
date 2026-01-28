from scraper import (auto_scrape_and_download_old_data,
                     auto_scrape_recent_data,
                     read_old_data_to_dataframe)
from cleaner import (car_crash_old_data_clean,
                     transform_data_dict)
from load_to_mysql import (load_to_mysql,
                           setting_pkfk,
                           load_to_GCP_mysql,
                           get_existing_years)
import os 
import pandas as pd
from config import (SAVE_OLD_DATA_DIR,
                    SEQ_PAGE_URL)
pd.set_option('future.no_silent_downcasting', True)#關閉警告

if __name__ == "__main__":
    print("程式開始執行...")
    files = os.listdir(SAVE_OLD_DATA_DIR)
    #os.listdir這個method會去路徑下看檔案
    if len(files)>0:
        for item in files:
            full_path = os.path.join(SAVE_OLD_DATA_DIR,item)
            old_list=read_old_data_to_dataframe(full_path)
            trans=transform_data_dict(old_list)
            cleaned=car_crash_old_data_clean(trans)
            clean1 = cleaned['main']
            clean2 = cleaned['party']
            db_engine=load_to_GCP_mysql(clean1,clean2)
            #db_engine=load_to_mysql(clean1,clean2)
        if db_engine:
            setting_pkfk(db_engine)
    else:
        for i in range(len(SEQ_PAGE_URL)):
            old=auto_scrape_and_download_old_data(SEQ_PAGE_URL[i])
            trans=transform_data_dict(old)
            cleaned=car_crash_old_data_clean(trans)
            clean1 = cleaned['main']
            clean2 = cleaned['party']
            db_engine=load_to_GCP_mysql(clean1,clean2)
            #db_engine=load_to_mysql(clean1,clean2)
        if db_engine:
            setting_pkfk(db_engine)

    # read_old_data_to_dataframe(SAVE_NEW_DATA_PATH)
    # auto_scrape_recent_data()