# -*- coding: utf-8 -*-

### Import Relevant Libraries
import os
PYTHON_FOLDER = os.environ['PYTHON_FOLDER']  # Requires an environment variable to be preset
os.chdir(PYTHON_FOLDER)

import pandas as pd
import configparser
import tvc_transform as tvct
import tvc_load_service_account as tvcload

### Set up Python output to show every dataframe column
pd.set_option('display.max_columns', 500)


### Set variables by reading from the config.ini file
company_name = 'ServBiz'
config = configparser.ConfigParser()
config.read('config.ini')
GOOGLE_CREDENTIALS_FILE = config[company_name]['GOOGLE_CREDENTIALS_FILE']
GOOGLE_SPREADSHEET_KEY = config[company_name]['GOOGLE_SPREADSHEET_KEY']
RAW_DATAFILE = config[company_name]['RAW_DATAFILE']

### Extract raw data
t = pd.read_csv(RAW_DATAFILE)

### Instantiate TVCLoad object with Google credentials file to write to Google Sheets
tvcl = tvcload.TVCLoad(GOOGLE_CREDENTIALS_FILE)


### Define segments. Each Segment name maps to a segment_col name
segments = {'Unsegmented' : None,
            'Channel' : 'segment'
            }


for seg in segments:
    print('Processing the', seg, 'segment')
    
    seg_col = segments[seg]
    if seg_col is None:
        use_seg = False
    else:
        use_seg = True
    
    ### Transform the raw data into dau_decorated
    dau = tvct.create_dau_df(t, 
                             user_id = 'client_id', 
                             activity_date = 'date', 
                             inc_amt = 'value_usd',
                             segment_col = seg_col
                            )
    
    dau_decorated = tvct.create_dau_decorated_df(dau)
    
    
    ### Calculate Weekly Growth Accounting and Cohort Analysis based on wau_decorated
    # WAU Decorated
    wau_decorated = tvct.create_xau_decorated_df(dau_decorated, 'week', use_segment=use_seg)
    
    # Weekly Growth Accounting
    w_ga = tvct.consolidate_all_ga(wau_decorated, 'week', 
                                     use_segment = use_seg, 
                                     growth_rate_periods = 12, 
                                     keep_last_period = False)
    tvcl.write_to_google_sheet(w_ga, seg + ' Weekly Growth Accounting', GOOGLE_SPREADSHEET_KEY)
    
    # Weekly Cohorts
    wau_cohorts = tvct.create_xau_cohort_df(wau_decorated, 'week', use_segment = use_seg)
    tvcl.write_to_google_sheet(wau_cohorts, seg + ' Weekly Cohorts', GOOGLE_SPREADSHEET_KEY)
    
    
    
    ### Calculate Monthly Growth Accounting and Cohort Analysis based on mau_decorated
    # MAU Decorated
    mau_decorated = tvct.create_xau_decorated_df(dau_decorated, 'month', use_segment=use_seg)
    
    # Monthly Growth Accounting
    m_ga = tvct.consolidate_all_ga(mau_decorated, 'month', 
                                     use_segment = use_seg, 
                                     growth_rate_periods = 12, 
                                     keep_last_period = False)
    tvcl.write_to_google_sheet(m_ga, seg + ' Monthly Growth Accounting', GOOGLE_SPREADSHEET_KEY)
    
    # Monthly Cohorts
    mau_cohorts = tvct.create_xau_cohort_df(mau_decorated, 'month', use_segment=use_seg)
    tvcl.write_to_google_sheet(mau_cohorts, seg + ' Monthly Cohorts', GOOGLE_SPREADSHEET_KEY)
    
    
    
    ### Calculate the Rolling 28-Day DAU/MAU ratios
    rolling_dau_mau = tvct.create_xau_window_df(dau_decorated, 
                                              time_period = 'day',
                                              window_days = 28, 
                                              breakouts = [2, 4, 8, 12, 16, 20],
                                              use_segment = use_seg,
                                              use_final_day = False)
    tvcl.write_to_google_sheet(rolling_dau_mau, seg + ' Rolling DAU/MAU', GOOGLE_SPREADSHEET_KEY)
    
    
    ### Calculate the Rolling 28-Day WAU/MAU ratios
    rolling_wau_mau = tvct.create_xau_window_df(dau_decorated, 
                                              time_period = 'week',
                                              window_days = 28, 
                                              breakouts = [2, 3, 4],
                                              use_segment = use_seg,
                                              use_final_day = False)
    tvcl.write_to_google_sheet(rolling_wau_mau, seg + ' Rolling WAU/MAU', GOOGLE_SPREADSHEET_KEY)











