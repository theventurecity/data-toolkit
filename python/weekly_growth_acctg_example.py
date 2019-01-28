# -*- coding: utf-8 -*-

### Import Relevant Libraries
import pandas as pd

### Ensure that the Python folder is in the Path to include the growth_accounting functions
PYTHON_FOLDER = 'C:\\Users\\DavidSmith\\Dropbox (TheVentureCity)\\David\\Python Scripts\\' # Edit for your environment
import sys
sys.path.append(PYTHON_FOLDER)
import growth_accounting as ga # Get latest version here: https://github.com/theventurecity/Analytics/python/growth_accounting.py

### Set up Python output to show every dataframe column
pd.set_option('display.max_columns', 500)

#Edit to include your local information
company_name = 'SampleCo'
folder = 'C:\\Users\\DavidSmith\\Dropbox (TheVentureCity)\\David\\' + company_name + '\\' 

# filename = folder + 'Edit this filename to your local file.csv' # If using a local CSV file
filename = 'https://raw.githubusercontent.com/theventurecity/Analytics/master/data/sampleco_transactions.csv'

### Read the data file into memory as a Pandas dataframe called "t"
t = pd.read_csv(filename)

### Take a look at t to make sure it loaded properly
t.head()
t.shape

### Create the DAU dataframe which rolls up user-level transactions by day
dau = ga.create_dau_df(t, 
                       user_id = 'user_id', 
                       activity_date = 'dt', 
                       inc_amt = 'inc_amt')
dau.head()

### Calculate the first activity date for each user_id in the dataset
### This step is optional. If you do not include a dataframe in the  
### first_dt_df optional paramter as part of the create_dau_decorated function
### below, it will run create_first_dt_df as part of create_dau_decorated_df
### It is included here because it is also used in the create_wau_decorated_df
### function below.
first_dt = ga.create_first_dt_df(dau)
first_dt.head()

### This merges the first_dt dataframe with the dau dataframe to enable growth
### accounting and cohort calculations
dau_decorated = ga.create_dau_decorated_df(dau, first_dt_df = first_dt)
dau_decorated.head()

### Growth Accounting: WAU and WRR
### Aggregates DAU Decorated by Week
wau_decorated = ga.create_xau_decorated_df(dau_decorated, 'week', use_segment = False)

### Calculate growth accounting metrics for each week in the wau_decorated
### dataframe and write to a CSV file in order to visualize (Visualization
### code not included here).
w_all_ga = ga.consolidate_all_ga(wau_decorated, 'week', keep_last_period = False)
w_all_ga.to_csv(folder + company_name + '_weekly_all_ga.csv', index = False)

### Calcualted weekly cohort retention curves and write to an output CSV file
### for visualization(s)
wau_retention_by_cohort = ga.xau_retention_by_cohort_df(wau_decorated, 'week')
wau_retention_by_cohort.to_csv(folder + company_name + '_wau_retention_by_cohort.csv', index = False)

### Engagement: Calculated days active in the last 100 days for each user ID
### and write it to a CSV output file
### so it can be plotted as a histogram (visulaization code not included here)
usage_hist_L100 = ga.calc_user_daily_usage(dau_decorated, 
                                           max(dau_decorated.activity_date), 
                                           window_days = 100, breakouts = [],
                                           use_segment = False)
usage_hist_L100.to_csv(folder + company_name + '_usage_hist_L100.csv', index = False)


### Engagement: Calculated the DAU/MAU Ratio for the 28-day trailing window with 
### various minimum days active ratios also calculated
rolling_dau_xau = ga.create_dau_window_df(dau_decorated, 
                                          window_days = 28, 
                                          breakouts = [2, 4, 7, 14, 21, 28],
                                          use_segment = False)
rolling_dau_xau.to_csv(folder + company_name + '_rolling_dau_xau.csv', index = False)


### Growth Accounting: Rolling L28 Quick Ratio, unsegmented
####### WARNING: THIS CAN TAKE A LONG TIME TO RUN #######
rolling = ga.calc_rolling_qr_window(dau_decorated, window_days = 28, use_segment = False)
rolling.to_csv(folder + company_name + '_rolling_qr.csv', index = False)
