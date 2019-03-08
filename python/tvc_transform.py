# -*- coding: utf-8 -*-
"""
Created on Wed Aug 29 22:43:50 2018

@author: dksmi
"""

import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import math

### For discrete time period calculations, this helps set the variable names
### in the different dataframes 
def get_time_period_dict(time_period):
    
    time_fields_dict = {
                        'week' : {'grouping_col' : 'Week',
                                  'first_period_col' : 'first_week',
                                  'frequency' : 'Weekly',
                                  'unit' : 'Week',
                                  'period_abbr' : 'W',
                                  'python_period' : 'weeks'
                                  },
                        'month' : {'grouping_col' : 'Month_Year',
                                   'first_period_col' : 'first_month',
                                   'frequency' : 'Monthly',
                                   'unit' : 'Month',
                                   'period_abbr' : 'M',
                                   'python_period' : 'months'
                                  }
                        }
                    
    if time_period in time_fields_dict:
        time_fields = time_fields_dict[time_period]
    else:
        time_fields = None
    
    return time_fields



### Create Daily Active Users dataframe that aggregates all activity by user and day
### If the segmentation column is specified, this function includes the segment
### in the final dataframe
def create_dau_df(transactions, 
                  user_id = 'user_id', 
                  activity_date = 'activity_date', 
                  inc_amt = 'inc_amt', 
                  segment_col = None,
                  include_zero_inc = False):
    
    transactions[activity_date] = pd.to_datetime(transactions[activity_date]).dt.date
    
    if include_zero_inc:
        trans_df = transactions
    else:
        trans_df = transactions.loc[transactions[inc_amt] > 0]
    
    if segment_col is None:
        dau = (trans_df
               .groupby([user_id, activity_date], as_index = False)[inc_amt]
               .sum()
               .rename(columns = {user_id : 'user_id', 
                                  activity_date : 'activity_date', 
                                  inc_amt : 'inc_amt'})
                            )
    else:
        dau = (trans_df
               .groupby([user_id, activity_date, segment_col], as_index = False)[inc_amt]
               .sum()
               .rename(columns = {user_id : 'user_id', 
                                  activity_date : 'activity_date', 
                                  inc_amt : 'inc_amt', 
                                  segment_col : 'segment'})
                            )
        
    return dau



### Using the DAU dataframe created in the function above, this creates a
### Weekly Active Users (WAU) dataframe
def create_wau_df(dau_df):
    print('Creating WAU dataframe')
    dau = dau_df.copy()
    dau['Week'] = pd.to_datetime(dau['activity_date']).dt.to_period('W')
    wau = dau.groupby(['user_id', 'Week'], as_index = False)['inc_amt']\
            .sum()
    return wau




### Using the DAU dataframe created in the function above, this creates a
### Monthly Active Users (MAU) dataframe
def create_mau_df(dau_df):
    print('Creating MAU dataframe')
    dau = dau_df.copy()
    dau['Month_Year'] = pd.to_datetime(dau['activity_date']).dt.to_period('M')
    mau = dau.groupby(['user_id', 'Month_Year'], as_index = False)['inc_amt']\
            .sum()
    return mau




### Using the DAU dataframe created in the function above, this creates a
### dataframe that contains the first usage day, week, and month for each user
def create_first_dt_df(dau_df):
    print('Creating first_dt dataframe')
    dau = dau_df.copy()
    first_dt = dau.groupby(['user_id'], as_index = False)['activity_date']\
            .min()\
            .rename(columns = { 'activity_date' : 'first_dt' })
    first_dt['first_dt'] = pd.to_datetime(first_dt['first_dt']).dt.date
    first_dt['first_week'] = pd.to_datetime(first_dt['first_dt']).dt.to_period('W')
    first_dt['first_month'] = pd.to_datetime(first_dt['first_dt']).dt.to_period('M')
    return first_dt



### Using the DAU dataframe created in the function above, this calls
### create_first_df and then merges it with the DAU dataframe to add the user's
### first usage date to the DAU dataframe
### dau_decorated is used in the subsequent functions below
### Using the segmented column from a segmented DAU dataframe is a T|F option
def create_dau_decorated_df(dau_df, first_dt_df = None):
    print('Creating DAU Decorated dataframe')
    if first_dt_df is None:
        first_dt_df = create_first_dt_df(dau_df)
    dau_decorated_df = pd.merge(dau_df, first_dt_df, how = 'left', on = 'user_id')
    dau_decorated_df['activity_date'] = pd.to_datetime(dau_decorated_df['activity_date']).dt.date
    dau_decorated_df['first_dt'] = pd.to_datetime(dau_decorated_df['first_dt']).dt.date
    
    
    return dau_decorated_df



### Merging the WAU and first_dt dataframes created in the functions above, this 
### adds the user's first week to the WAU dataframe
### wau_decorated is used in the subsequent functions below
### Using the segmented column from a segmented DAU dataframe is not an option at this time
#def create_wau_decorated_df(wau_df, first_dt_df):
#    print('Creating WAU Decorated dataframe')
#    wau_decorated = pd.merge(wau_df, first_dt_df, how = 'left', on = 'user_id')
#    wau_decorated['start_of_next_week'] = pd.to_datetime(pd.PeriodIndex(wau_decorated['Week']).start_time + timedelta(weeks = 1))
#    wau_decorated['Next_Week'] = wau_decorated['start_of_next_week'].dt.to_period('W')
#    wau_decorated = wau_decorated[['Week', 'user_id', 'inc_amt', 'first_week', 'Next_Week']]
#    return wau_decorated

    


def increment_period(xau_grouping_col, time_period):
    time_fields = get_time_period_dict(time_period)
    period_abbr = time_fields['period_abbr']
    
    if time_period == 'week':
        start_of_next_period = pd.to_datetime(pd.PeriodIndex(xau_grouping_col).start_time + timedelta(weeks = 1))
    elif time_period == 'month':
        start_of_next_period = pd.to_datetime(pd.PeriodIndex(xau_grouping_col, freq = period_abbr).start_time) + pd.DateOffset(months = 1)
    else:
        start_of_next_period = None
        
    if start_of_next_period is not None:
        next_period = pd.Series(start_of_next_period).dt.to_period(period_abbr)
    else:
        next_period = None
    
    return next_period




def create_xau_decorated_df(dau_decorated_df, time_period, use_segment):
    
    time_fields = get_time_period_dict(time_period)
    grouping_col = time_fields['grouping_col']
    frequency = time_fields['frequency']
    first_period_col = time_fields['first_period_col']
    period_abbr = time_fields['period_abbr']
    
    print('Creating ' + frequency + ' Active Users Decorated dataframe')
    
    groupby_cols = [grouping_col, 'user_id', first_period_col]
    if use_segment:
        groupby_cols = groupby_cols + ['segment']
        
    dau_decorated = dau_decorated_df.copy()
    dau_decorated[grouping_col] = pd.to_datetime(dau_decorated['activity_date']).dt.to_period(period_abbr)
    xau = (dau_decorated.groupby(groupby_cols, as_index = False)['inc_amt'].sum())
    xau['Next_' + grouping_col] = increment_period(xau[grouping_col], time_period)
    
    output_cols = [grouping_col, 'user_id', 'inc_amt', first_period_col, 'Next_' + grouping_col]
    if use_segment:
        output_cols = output_cols + ['segment']
    xau = xau[output_cols]
    
    return xau




### Merging the MAU and first_dt dataframes created in the functions above, this 
### adds the user's first month to the MAU dataframe
### mau_decorated is used in the subsequent functions below
### Using the segmented column from a segmented DAU dataframe is not an option at this time
#def create_mau_decorated_df(mau_df, first_dt_df):
#    print('Creating MAU Decorated dataframe')
#    mau_decorated = pd.merge(mau_df.copy(), first_dt_df.copy(), how = 'left', on = 'user_id')
#    mau_decorated['start_of_next_month'] = pd.to_datetime(pd.PeriodIndex(mau_decorated['Month_Year'], freq = 'M').start_time) + pd.DateOffset(months = 1)
#    print(mau_decorated['start_of_next_month'])
#    mau_decorated['Next_Month_Year'] = mau_decorated['start_of_next_month'].dt.to_period('M')
#    mau_decorated = mau_decorated[['Month_Year', 'user_id', 'inc_amt', 'first_month', 'Next_Month_Year']]
#    return mau_decorated



### This takes one to many rows of growth accounting figures for a specific
### date and calculates the user quick ratio
### It is used when calculating rolling quick ratio
def calc_user_qr(row, new_col = 'new', res_col = 'resurrected', churned_col = 'churned'):
    new_users = row[new_col] if hasattr(row, new_col) and pd.notnull(row[new_col]) else 0
    res_users = row[res_col] if hasattr(row, res_col) and pd.notnull(row[res_col]) else 0
    churned_users = row[churned_col] if hasattr(row, churned_col) and pd.notnull(row[churned_col]) else 0
    if churned_users < 0:
        user_qr = -1 * (new_users + res_users) / churned_users
    else:
        user_qr = math.nan
        
    return user_qr




### This takes one or more rows of growth accounting figures for a specific
### date and calculates the user quick ratio
### It is used when calculating rolling quick ratio
def calc_rev_qr(row, new_col = 'new', res_col = 'resurrected', 
                churned_col = 'churned', exp_col = 'expansion', 
                con_col = 'contraction'):
    new_rev = row[new_col] if hasattr(row, new_col) and pd.notnull(row[new_col]) else 0
    res_rev = row[res_col] if hasattr(row, res_col) and pd.notnull(row[res_col]) else 0
    churned_rev = row[churned_col] if hasattr(row, churned_col) and pd.notnull(row[churned_col]) else 0
    expansion_rev = row[exp_col] if hasattr(row, exp_col) and pd.notnull(row[exp_col]) else 0
    contraction_rev = row[con_col] if hasattr(row, con_col) and pd.notnull(row[con_col]) else 0
    if churned_rev + contraction_rev < 0:
        rev_qr = -1 * (new_rev + res_rev + expansion_rev) / (churned_rev + contraction_rev)
    else:
        rev_qr = math.nan
    return rev_qr




### This takes a dataframe of transactions grouped by a particular date period
### and returns active, retained, new, resurrected, and churned users for that
### time period
def calc_user_ga(x, grouping_col, first_period_col):
    au = x.loc[~x[grouping_col + '.t'].isnull(), 'user_id'].nunique() 
    ret_users = x.loc[(x['inc_amt.t'] > 0) & (x['inc_amt.l'] > 0), 'user_id'].nunique()
    new_users = x.loc[x[first_period_col + '.t'] == x[grouping_col + '.t'], 'user_id'].nunique()
    res_users = x.loc[(x[first_period_col + '.t'] != x[grouping_col + '.t']) & ~(x['inc_amt.l'] > 0), 'user_id'].nunique()
    churned_users = -1 * x.loc[~(x['inc_amt.t'] > 0), 'user_id'].nunique()

    vals = [au, ret_users, new_users, res_users, churned_users]
    return vals




### This takes a dataframe of transactions grouped by a particular date period
### and returns total, retained, new, expansion, resurrected, contraction, and 
### churned revenuefor that time period
def calc_rev_ga(x, grouping_col, first_period_col):
    rev = x.loc[~x[grouping_col + '.t'].isnull(), 'inc_amt.t'].sum() 
    ret_rev = x.loc[(x['inc_amt.t'] > 0) & (x['inc_amt.l'] > 0), ['inc_amt.t', 'inc_amt.l']].min(axis=1).sum()
    new_rev = x.loc[x[first_period_col + '.t'] == x[grouping_col + '.t'], 'inc_amt.t'].sum()
    res_rev = x.loc[(x[first_period_col + '.t'] != x[grouping_col + '.t']) & ~(x['inc_amt.l'] > 0), 'inc_amt.t'].sum()
    churned_rev = -1 * x.loc[~(x['inc_amt.t'] > 0), 'inc_amt.l'].sum()
    exp_rev_set = x.loc[(x[first_period_col + '.t'] != x[grouping_col + '.t']) & 
                        (x['inc_amt.t'] > 0) & (x['inc_amt.l'] > 0) &
                        (x['inc_amt.t'] > x['inc_amt.l']), ['inc_amt.t', 'inc_amt.l']]
    exp_rev = exp_rev_set['inc_amt.t'].sum() - exp_rev_set['inc_amt.l'].sum()

    con_rev_set = x.loc[(x[first_period_col + '.t'] != x[grouping_col + '.t']) & 
                        (x['inc_amt.t'] > 0) & (x['inc_amt.l'] > 0) &
                        (x['inc_amt.t'] < x['inc_amt.l']), ['inc_amt.t', 'inc_amt.l']]
    con_rev = con_rev_set['inc_amt.t'].sum() - con_rev_set['inc_amt.l'].sum()
    
    vals = [rev, ret_rev, new_rev, res_rev, exp_rev, con_rev, churned_rev]
    return vals




### Produces the "final" growth accounting dataframe with both user and
### revenue numbers for each time period in the "decorated" dataframe
def create_growth_accounting_dfs(xau_decorated_df, 
                                 time_period, 
                                 use_segment = False,
                                 keep_last_period = True, 
                                 date_limit = None,
                                 include_zero_inc = False):
    print('Creating Growth Accounting dataframes')
    time_fields = get_time_period_dict(time_period)
    grouping_col = time_fields['grouping_col']
    first_period_col = time_fields['first_period_col']
    frequency = time_fields['frequency']
    
    xau_decorated_df_last = xau_decorated_df.copy()
    xau_decorated_df_last[grouping_col + '_join'] = xau_decorated_df_last['Next_' + grouping_col]
    xau_decorated_df[grouping_col + '_join'] = xau_decorated_df[grouping_col]
    
    interim_join_cols = ['user_id', grouping_col + '_join']
    if use_segment:
        interim_join_cols = interim_join_cols + ['segment']
    
    xga_interim = pd.merge(xau_decorated_df, xau_decorated_df_last, 
                           suffixes = ['.t', '.l'],
                           how = 'outer', 
                           left_on = interim_join_cols, 
                           right_on = interim_join_cols)
    
    groupby_cols = [grouping_col + '_join']
    if use_segment:
        groupby_cols = groupby_cols + ['segment']
        
    user_xga = (xga_interim.groupby(groupby_cols)
                .apply(lambda x: pd.Series(calc_user_ga(x, 
                                                        grouping_col, 
                                                        first_period_col),
                                           index = [frequency + ' Active Users', 
                                                    'Retained Users', 
                                                    'New Users', 
                                                    'Resurrected Users',
                                                    'Churned Users'
                                                    ]))
                .reset_index()
                .rename(columns = {grouping_col + '_join' : grouping_col}))
                
    rev_xga = (xga_interim.groupby(groupby_cols)
                .apply(lambda x: pd.Series(calc_rev_ga(x, 
                                                       grouping_col, 
                                                       first_period_col),
                                           index = [frequency + ' Revenue',
                                                    'Retained Revenue',
                                                    'New Revenue',
                                                    'Resurrected Revenue',
                                                    'Expansion Revenue',
                                                    'Contraction Revenue',
                                                    'Churned Revenue'
                                                    ]))
                .reset_index()
                .rename(columns = {grouping_col + '_join' : grouping_col}))
                
    user_xga = user_xga[user_xga[frequency + ' Active Users'] > 0]
    if not include_zero_inc:
        rev_xga = rev_xga[rev_xga[frequency + ' Revenue'] > 0]
    
    user_xga[grouping_col] = pd.to_datetime(pd.PeriodIndex(user_xga[grouping_col]).start_time) + timedelta(hours = 7) 
    rev_xga[grouping_col] = pd.to_datetime(pd.PeriodIndex(rev_xga[grouping_col]).start_time) + timedelta(hours = 7)
    
    if not keep_last_period:
        user_xga = user_xga[:-1]
        rev_xga = rev_xga[:-1]
        
    if date_limit is not None:
        user_xga = user_xga[user_xga[grouping_col] <= date_limit]
        rev_xga = rev_xga[rev_xga[grouping_col] <= date_limit]

    return user_xga, rev_xga




### Using the numbers in the "final" growth accounting dataframe, calculate
### the number of users at the beginning of the period (BOP), the  
### period-over-period user retention ratio, and the user quick ratio
def calc_user_ga_ratios(user_xga_df, time_period, use_segment = False, growth_rate_periods = 12):
    
    time_fields = get_time_period_dict(time_period)
    frequency = time_fields['frequency']
    per = time_fields['period_abbr']
    
    if use_segment:
        segments = user_xga_df.segment.unique()
    else:
        segments = ['All']

    ratio_df = pd.DataFrame()    
    for s in segments:
        if s != 'All':
            this_ratio_df = user_xga_df.copy().loc[user_xga_df['segment'] == s].reset_index()
        else:
            this_ratio_df = user_xga_df.copy().reset_index()
            this_ratio_df['segment'] = s
            
        this_ratio_df['Users BOP'] = this_ratio_df[frequency + ' Active Users'].shift(1)
        this_ratio_df[per + 'o' + per + ' User Retention'] = this_ratio_df['Retained Users'] / this_ratio_df['Users BOP']
        this_ratio_df['User Quick Ratio'] = this_ratio_df.apply(lambda x: calc_user_qr(x, 
                     new_col = 'New Users', res_col = 'Resurrected Users', 
                     churned_col = 'Churned Users'), axis = 1)
    
        cgr_col = 'T%s%s User C%sGR' % (growth_rate_periods, per, per)
        this_ratio_df[cgr_col] = np.power((this_ratio_df[frequency + ' Active Users'] / \
                     this_ratio_df[frequency + ' Active Users'].shift(growth_rate_periods)), 1/growth_rate_periods)-1
        
        ratio_df = ratio_df.append(this_ratio_df)
    
    return ratio_df




### Using the numbers in the "final" growth accounting dataframe, calculate
### the revenue at the beginning of the period (BOP), the  
### period-over-period revenue retention ratio, and the revenue quick ratio
def calc_rev_ga_ratios(rev_xga_df, time_period, use_segment = False, growth_rate_periods = 12):
    
    time_fields = get_time_period_dict(time_period)
    frequency = time_fields['frequency']
    per = time_fields['period_abbr']
    
    if use_segment:
        segments = rev_xga_df.segment.unique()
    else:
        segments = ['All']
        
    ratio_df = pd.DataFrame()
    for s in segments:
        if s!= 'All':
            this_ratio_df = rev_xga_df.copy().loc[rev_xga_df['segment'] == s].reset_index()
        else:
            this_ratio_df = rev_xga_df.copy().reset_index()
            this_ratio_df['segment'] = s
            
        this_ratio_df['Revenue BOP'] = this_ratio_df[frequency + ' Revenue'].shift(1)
        this_ratio_df[per + 'o' + per + ' Revenue Retention'] = this_ratio_df['Retained Revenue'] / this_ratio_df['Revenue BOP']
        this_ratio_df['Revenue Quick Ratio'] = this_ratio_df.apply(lambda x: calc_rev_qr(x,
                new_col = 'New Revenue', res_col = 'Resurrected Revenue', 
                exp_col = 'Expansion Revenue', churned_col = 'Churned Revenue', 
                con_col = 'Contraction Revenue'), axis = 1)
        this_ratio_df['Net Expansion Revenue'] = this_ratio_df['Expansion Revenue'] + \
                                            this_ratio_df['Contraction Revenue']
        
        cgr_col = 'T%s%s Revenue C%sGR' % (growth_rate_periods, per, per)
        this_ratio_df[cgr_col] = np.power((this_ratio_df[frequency + ' Revenue'] / \
                this_ratio_df[frequency + ' Revenue'].shift(growth_rate_periods)), 1/growth_rate_periods)-1
        
        ratio_df = ratio_df.append(this_ratio_df)
    
    return ratio_df




### Join the user growth accounting dataframe with the revenue growth accounting
### dataframe
def consolidate_ga_dfs(user_ga_df, rev_ga_df, time_period):
    
    time_fields = get_time_period_dict(time_period)
    grouping_col = time_fields['grouping_col']
    
    print('Joining user and revenue dataframes')
    
    consolidated_ga_df = pd.merge(user_ga_df, rev_ga_df,
                                  how = 'inner', on = [grouping_col, 'segment'])

    return consolidated_ga_df




### Bring together all the Weekly/Monthly Growth Accounting Functions into a complete dataframe
def consolidate_all_ga(xau_decorated_df, 
                       time_period, 
                       use_segment = False, 
                       growth_rate_periods = 12,
                       keep_last_period = True, 
                       date_limit = None,
                       include_zero_inc = False):
    
    user_ga, rev_ga = create_growth_accounting_dfs(xau_decorated_df, time_period, 
                                                   use_segment, keep_last_period, 
                                                   date_limit, include_zero_inc)
    user_ga_with_ratios = calc_user_ga_ratios(user_ga, time_period, use_segment, growth_rate_periods)
    rev_ga_with_ratios = calc_rev_ga_ratios(rev_ga, time_period, use_segment, growth_rate_periods)
    all_ga_df = consolidate_ga_dfs(user_ga_with_ratios, rev_ga_with_ratios, time_period)
    
    time_fields = get_time_period_dict(time_period)
    frequency = time_fields['frequency']
    all_ga_df['Revenue per User'] = all_ga_df[frequency + ' Revenue'] / \
                                    all_ga_df[frequency + ' Active Users']
    
    return all_ga_df



def add_period_n_cum_inc_per_cohort_cust_columns(cohort_df, since_col, unit):
    c_df = cohort_df.copy()
    since_vector = c_df[since_col].unique()
    for n in since_vector:
        new_column_name = unit + ' %s' % n
        c_df[new_column_name] = c_df['cum_inc_per_cohort_cust'] * (c_df[since_col] == n)
        c_df[new_column_name] = c_df[new_column_name].replace(0, np.nan)
    return c_df
    


### Calculate the user retention by cohort defined by any weekly or monthly time period
def xau_retention_by_cohort_df(xau_decorated_df, time_period, use_segment = False,
                               recent_periods_back_to_exclude = 1, date_limit = None):
    
    time_fields = get_time_period_dict(time_period)
    grouping_col = time_fields['grouping_col']
    first_period_col = time_fields['first_period_col']
    unit = time_fields['unit']
    period_abbr = time_fields['period_abbr']
        
    since_col = '%ss Since First' % unit
    
    if date_limit is not None:
        xau_d = xau_decorated_df[pd.PeriodIndex(xau_decorated_df[grouping_col], freq = period_abbr).start_time <= date_limit].copy()
    else:
        xau_d = xau_decorated_df.copy()
    
    xau_d[since_col] = xau_d[grouping_col] - xau_d[first_period_col]
    
    first_groupby_cols = [first_period_col, grouping_col, since_col]
    if use_segment:
        first_groupby_cols = first_groupby_cols + ['segment']
    
    xau_d = xau_d.groupby(first_groupby_cols)\
                    .agg({'inc_amt' : 'sum', 
                          'user_id' : 'nunique'})\
                    .rename(columns = { 'user_id' : 'cust_ct' })
                    
    second_groupby_cols = [first_period_col]
    if use_segment:
        second_groupby_cols = second_groupby_cols + ['segment']
    
    xau_d['cohort_cust_ct'] = xau_d.groupby(second_groupby_cols)['cust_ct'].transform('first')
    xau_d['cum_inc_amt'] = xau_d.groupby(second_groupby_cols)['inc_amt'].cumsum()
    
    xau_d['cum_inc_per_cohort_cust'] = xau_d['cum_inc_amt'] / xau_d['cohort_cust_ct']
    xau_d['cust_ret_pct'] = xau_d['cust_ct'] / xau_d['cohort_cust_ct']
    
    xau_d = xau_d.reset_index()
    
    if time_period == 'month':
#        td = timedelta(months = recent_periods_back_to_exclude)
        td = pd.DateOffset(months = recent_periods_back_to_exclude)
    elif time_period == 'week':
        td = timedelta(weeks = recent_periods_back_to_exclude)
    
    last_period = pd.to_datetime(datetime.today() - td).to_period(period_abbr)
    xau_d = xau_d.loc[xau_d[grouping_col] <= last_period]
    
    xau_d[first_period_col] = pd.PeriodIndex(xau_d[first_period_col], freq = period_abbr).start_time + timedelta(hours = 7)
    xau_d[grouping_col] = pd.PeriodIndex(xau_d[grouping_col], freq = period_abbr).start_time + timedelta(hours = 7)
    
    if use_segment:
        xau_d['segment_first_' + time_period] = xau_d[first_period_col].dt.strftime('%Y-%m') + '-' + xau_d['segment']  
    
    xau_d = add_period_n_cum_inc_per_cohort_cust_columns(xau_d, since_col, unit)
    
   
    return xau_d




### For a particular "last_date" of a time period window, assigns a designation
### to each DAU in the DAU dataframe. The options are "this_period," which is in
### the last window_days days; "first_this_period," which indicates that the user 
### new in the window in question; and "last_period," which means it was between
### 1X and 2X windows_days in the past
def assign_ga_date_range(row, last_date, window_days):
    
    ga_date_range = 'this_period'
    curr_period_start_dt = last_date - timedelta(days = window_days-1)
    prev_period_start_dt = last_date - timedelta(days = 2*window_days-1)
    
    if row['first_dt'] >= curr_period_start_dt:
        ga_date_range = 'first_this_period'
    elif row['activity_date'] >= prev_period_start_dt and row['activity_date'] < curr_period_start_dt:
        ga_date_range = 'last_period'
    
    return ga_date_range



### After grouping by the date ranges in assign_ga_date_range, this determines
### how the aggregate count and sum should be classified according to growth
### accounting definitions
def assign_user_status(x):
    is_last_period = pd.notnull(x.last_period) if hasattr(x, 'last_period') else False
    is_this_period = pd.notnull(x.this_period) if hasattr(x, 'this_period') else False
    is_first_this_period = pd.notnull(x.first_this_period) if hasattr(x, 'first_this_period') else False
    
    if is_first_this_period:
        status = 'new'
    elif is_last_period and is_this_period:
        status = 'retained'
    elif is_this_period and not is_last_period:
        status = 'resurrected'
    elif is_last_period and not is_this_period:
        status = 'churned'
    else:
        status = 'prior'

    return status



### Combines the above functions to determine the growth accounting for a 
### window specified by its end date. If use_segment is False, it returns a
### dataframe of one row. If use_segment is True, it returns a dataframe with 
### one row per segment
def calc_ga_for_window(dau_decorated_df, last_date, window_days, use_segment):
    window_start_date = last_date - timedelta(days = 2*window_days-1)
    dau_dec = (dau_decorated_df
            .loc[(dau_decorated_df['activity_date'] >= window_start_date) & (dau_decorated_df['activity_date'] <= last_date)]
            .copy()
            )
    
    dau_dec['ga_date_range'] = dau_dec.apply(lambda x: assign_ga_date_range(x, last_date, window_days), axis = 1)
    
    groupings = ['user_id', 'ga_date_range']
    if use_segment:
        groupings.insert(1, 'segment')
        
    dau_grouped = (dau_dec.groupby(groupings)['inc_amt']
                                .sum()
                                .unstack()
                                .reset_index()
                                )
          
    dau_grouped['user_status'] = dau_grouped.apply(assign_user_status, axis = 1)

    groupings2 = ['user_status']
    if use_segment == True:
        groupings2.insert(0, 'segment')
        
        dau_grouped_2 = (dau_grouped.groupby(groupings2)['user_id']
                            .count()
                            .unstack(level = 'user_status')
                            .reset_index()
                            )
        dau_grouped_2['window_end_date'] = last_date
    else:
        dau_grouped_2 = (dau_grouped.groupby(groupings2)['user_id']
                            .count()
                            .to_frame(name = last_date)
                            .T
                            .reset_index()
                            .rename(columns = { 'index' : 'window_end_date'})
                            )  
    
    new_users = dau_grouped_2.new if hasattr(dau_grouped_2, 'new') else 0 
    res_users = dau_grouped_2.resurrected if hasattr(dau_grouped_2, 'resurrected') else 0
    dau_grouped_2.churned = -1 * dau_grouped_2.churned if hasattr(dau_grouped_2, 'churned') else 0
    churned_users = dau_grouped_2.churned if hasattr(dau_grouped_2, 'churned') else 0
    ret_users = dau_grouped_2.retained if hasattr(dau_grouped_2, 'retained') else 0
    
    dau_grouped_2['active_users'] = new_users + res_users + ret_users
    dau_grouped_2['user_quick_ratio'] = dau_grouped_2.apply(calc_user_qr, axis = 1)
    dau_grouped_2['user_retention_rate'] = ret_users / (ret_users - churned_users)
    dau_grouped_2['window_days'] = window_days
    dau_grouped_2['Growth Threshold'] = 1
        
    return dau_grouped_2
    

    
### Calls calc_ga_for_window for each available window of length window_days
### and compiles the resultant dataframe into a single dataframe for plotting
### and analysis
def calc_rolling_qr_window(dau_decorated_df, window_days = 28, use_segment = False):
    start_dt = min(dau_decorated_df['activity_date']) + timedelta(days = 2*window_days)
    end_dt = max(dau_decorated_df['activity_date'])
    date_range = pd.date_range(start = start_dt, end = end_dt, freq = 'D')

    rolling_qr_df = pd.DataFrame()
    for d in date_range:
        d2 = d.date()
        print(window_days, d2)
        this_window = calc_ga_for_window(dau_decorated_df, d2, window_days, use_segment)
        rolling_qr_df = rolling_qr_df.append(this_window)
        
    rolling_qr_df['window_end_date'] = pd.to_datetime(rolling_qr_df['window_end_date'])
    return rolling_qr_df

################### NEW AS OF 11/5/18

    

def calc_user_daily_usage(dau_decorated_df, last_date, window_days, breakouts, use_segment):
    window_start_date = last_date - timedelta(days = window_days-1)
    dau_dec = (dau_decorated_df
               .loc[(dau_decorated_df['activity_date'] >= window_start_date) & 
                    (dau_decorated_df['activity_date'] <= last_date)]
               .copy()
               )
    
    if not use_segment:
        dau_dec['segment'] = 'All'
        
    groupby_cols = ['user_id', 'segment']
    
    dau_grouped_df = (dau_dec
                      .groupby(groupby_cols)['inc_amt']
                      .agg(['count', 'sum'])
                      .reset_index()
                      .rename(columns = {'count' : 'active_days', 'sum': 'inc_amt' })
                      )
    
    for b in breakouts:
        col_name = '%sd+ users' % b
        dau_grouped_df[col_name] = (dau_grouped_df['active_days'] >= b)
        
    dau_grouped_df_sorted = dau_grouped_df.sort_values('inc_amt', ascending = False)  
    
    dau_grouped_df_sorted['cum_inc_amt'] = dau_grouped_df_sorted.groupby(['segment']).inc_amt.cumsum()
    total_inc_amt_df = (dau_grouped_df_sorted.groupby(['segment'])
                        .inc_amt
                        .agg(['sum', 'count'])
                        .reset_index()
                        .rename(columns = {'sum' : 'total_inc_amt',
                                           'count' : 'total_user_count'})
                        )
    dau_grouped_df_sorted = dau_grouped_df_sorted.merge(total_inc_amt_df, how = 'left', on = 'segment')
    dau_grouped_df_sorted['cum_inc_amt_pct_of_total'] = dau_grouped_df_sorted.cum_inc_amt / dau_grouped_df_sorted.total_inc_amt
    revenue_80pct_df = (dau_grouped_df_sorted[dau_grouped_df_sorted.cum_inc_amt_pct_of_total <= .80]
                        .groupby(['segment'])
                        .cum_inc_amt_pct_of_total
                        .count()
                        .reset_index()
                        .rename(columns = {'cum_inc_amt_pct_of_total' : 'revenue_80pct_user_count'})
                        )
    dau_grouped_df_sorted = dau_grouped_df_sorted.merge(revenue_80pct_df, how = 'left', on = 'segment')
    dau_grouped_df_sorted['revenue_80pct_ratio'] = dau_grouped_df_sorted['revenue_80pct_user_count'] / dau_grouped_df_sorted['total_user_count']
    
    return dau_grouped_df_sorted
        



def calc_dau_xau_ratio_for_window(dau_decorated_df, last_date, window_days, breakouts, use_segment):
    dau_grouped = calc_user_daily_usage(dau_decorated_df, last_date, window_days, breakouts, use_segment)
    
    dau_agg = pd.DataFrame()
    
    if use_segment:
        grouped_df = dau_grouped.groupby('segment')
    else:
        grouped_df = dau_grouped

    dau_agg['active_days'] = pd.Series(grouped_df.active_days.sum())
    dau_agg['1d+ users'] = grouped_df.user_id.nunique()
    dau_agg['dau_window_ratio'] = (dau_agg['active_days'] / window_days) / dau_agg['1d+ users']
    dau_agg['window_frequency'] = dau_agg['dau_window_ratio'] * window_days
    for b in breakouts:
        col_name = '%sd+ users' % b
        dau_agg[col_name] = grouped_df[col_name].sum()
        ratio_col_name = '%sd+ users / total %sd users' % (b, window_days)
        dau_agg[ratio_col_name] = dau_agg[col_name] / dau_agg['1d+ users']
    dau_agg['window_end_dt'] = last_date
    dau_agg = dau_agg.reset_index()
    
    return dau_agg




def create_dau_window_df(dau_decorated_df, window_days = 28, breakouts = [2, 4], use_segment = False):
    start_dt = min(dau_decorated_df['activity_date']) + timedelta(days = window_days)
    end_dt = max(dau_decorated_df['activity_date'])
    date_range = pd.date_range(start = start_dt, end = end_dt, freq = 'D')

    rolling_dau_xau_df = pd.DataFrame()
    for d in date_range:
        d2 = d.date()
        print(window_days, d2)
        this_window = calc_dau_xau_ratio_for_window(dau_decorated_df, 
                                                    last_date = d2, 
                                                    window_days = window_days, 
                                                    breakouts = breakouts,
                                                    use_segment = use_segment)
        rolling_dau_xau_df = rolling_dau_xau_df.append(this_window)
        
    rolling_dau_xau_df['window_end_dt'] = pd.to_datetime(rolling_dau_xau_df['window_end_dt'])
    return rolling_dau_xau_df

    
    
    
    
    