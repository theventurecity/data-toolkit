# -*- coding: utf-8 -*-
"""
Created on Wed Aug 29 22:43:50 2018

@author: DavidSmith
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
                        'day' : {'grouping_col' : 'activity_date',
                                  'first_period_col' : 'first_dt',
                                  'frequency' : 'Daily',
                                  'unit' : 'Day',
                                  'period_abbr' : 'D',
                                  'python_period' : 'days',
                                  'days' : 1
                                  },
                        'week' : {'grouping_col' : 'Week',
                                  'first_period_col' : 'first_week',
                                  'frequency' : 'Weekly',
                                  'unit' : 'Week',
                                  'period_abbr' : 'W',
                                  'python_period' : 'weeks',
                                  'days' : 7
                                  },
                        'month' : {'grouping_col' : 'Month_Year',
                                   'first_period_col' : 'first_month',
                                   'frequency' : 'Monthly',
                                   'unit' : 'Month',
                                   'period_abbr' : 'M',
                                   'python_period' : 'months',
                                   'days' : 28
                                  }
                        }
                    
    # if time_period passed in is a valid choice, then return the dictionary
    # associated with that choice from the dictionary above
    if time_period in time_fields_dict:
        time_fields = time_fields_dict[time_period]
    else:
        time_fields = None
    
    return time_fields



# The create_dau_df function takes as inputs a dataframe of transactions and 
# the names of the three key event log columns: User ID, Activity Date, and 
# Income Amount (could be revenue or contribution margin). It can handle a 
# fourth event log column that designates a segment. Next it ensures that
# the Activity Date column is a date and the User ID is a string. Then it groups
# all of the transaction records to calculate the sum of the Income Amount
# by User ID and Activity Date (and Segment, if chosen) combination

def create_dau_df(transactions, 
                  user_id = 'user_id', 
                  activity_date = 'activity_date', 
                  inc_amt = 'inc_amt', 
                  segment_col = None,
                  include_zero_inc = False):
    
    # Ensure correct data types
    # If the activity_date is in date-time format, it gets rolled up into the
    # day on which that event occurred. 
    transactions[activity_date] = pd.to_datetime(transactions[activity_date]).dt.date
    transactions[user_id] = transactions[user_id].astype('str')
    
    # If there is no inc_amt available in the data set, add a column of ones
    # Set the value of the inc_amt variable to 'inc_amt'
    if inc_amt is None:
        transactions['inc_amt'] = 1
        inc_amt = 'inc_amt'
    
    # By default, this function only allows transactions where the inc_amt > 0
    # This means it excludes things with negative amounts, like returns, for
    # example. The include_zero_inc allows us to include those transactions
    # if we see fit
    if include_zero_inc:
        trans_df = transactions
    else:
        trans_df = transactions.loc[transactions[inc_amt] > 0]
        
    # By default we group by user_id and activity_date. If a segment column is
    # specified when the function is called, we include that column's name in
    # the groupby as well. We also make sure that the segment is a string type
    groupby_cols = [user_id, activity_date]
    if segment_col is not None:
        groupby_cols += [segment_col]
        transactions[segment_col] = transactions[segment_col].astype('str')
    
    
    # Group by user_id and activity_date, calculate the sum of the inc_amt
    # and return standardized names for each column
    dau = (trans_df
           .groupby(groupby_cols, as_index = False)
           .agg({inc_amt : 'sum'})
           .rename(columns = {user_id : 'user_id', 
                              activity_date : 'activity_date', 
                              inc_amt : 'inc_amt'})
                        )

    # If we are using a segment column, it gets its own standardized name 'segment'
    if segment_col is not None:
        dau = dau.rename(columns = {segment_col : 'segment'})
        
    return dau




# The create_first_dt_df function takes as its input the DAU dataframe created
# above. After creating a copy of the original DAU dataframe so as not to 
# affect the original, it creates a new first_dt dataframe. Using the groupby
# and agg functions, it finds the minimum Activity Date for each User ID. Then 
# it specifies the week ('first_week') and month ('first_month') in which the 
# first Activity Date is found. 

def create_first_dt_df(dau_df):
    print('Creating first_dt dataframe')
    
    # Create copy of input dataframe
    dau = dau_df.copy()
    
    # Use groupby to find the minimum activity_date for each user_id
    first_dt = (dau.groupby(['user_id'], as_index = False)
                .agg({'activity_date' : 'min'})
                .rename(columns = { 'activity_date' : 'first_dt' })
               )
    
    # Ensure that the first_dt field is a date
    first_dt['first_dt'] = pd.to_datetime(first_dt['first_dt']).dt.date
    
    # Add two new columns with the first_week and first_month of the first_dt
    first_dt['first_week'] = pd.to_datetime(first_dt['first_dt']).dt.to_period('W')
    first_dt['first_month'] = pd.to_datetime(first_dt['first_dt']).dt.to_period('M')
    
    return first_dt



# The create_dau_decorated_df takes the two data frames created above, DAU and
# first_dt, and merges them together based on user_id. This results in a DAU
# dataframe "decorated" with information about the user's first activity date,
# first week, and first month, as shown below. Note: it is not necessary to 
# pass in the first_dt dataframe. If none is provided, the function will run
# create_first_dt_df so it has something to merge to the DAU dataframe.

def create_dau_decorated_df(dau_df, first_dt_df = None):
    print('Creating DAU Decorated dataframe')
    
    # If no first_dt_df is provided, create it
    if first_dt_df is None:
        first_dt_df = create_first_dt_df(dau_df)
        
    # Do a left merge of first_dt_df into dau_df on User ID
    dau_decorated_df = dau_df.merge(first_dt_df, how = 'left', on = 'user_id')

    # If segment is included in this dataframe, ensure that it is a string type
    if 'segment' in dau_decorated_df.columns:
        dau_decorated_df['segment'] = dau_decorated_df['segment'].astype('str')
    
    return dau_decorated_df


    


### This is another helper function that allows us to determine the next week
### or month for any given week of month. We need these because the Pandas
### date math is not consistent. You can use timedelta to add weeks, but you 
### have to use DateOffset to add months.
def increment_period(xau_grouping_col, time_period):
  
    # Call the get_time_period_dict function above, passing in the time period
    time_fields = get_time_period_dict(time_period)
    
    # Set the one-letter period abbreviation to whatever that function returns
    period_abbr = time_fields['period_abbr']
    
    # Depending on the time period, increment the week or month by one after
    # first converting it to the date time of the start of the period
    if time_period == 'week':
        start_of_next_period = pd.to_datetime(pd.PeriodIndex(xau_grouping_col).start_time + timedelta(weeks = 1))
    elif time_period == 'month':
        start_of_next_period = pd.to_datetime(pd.PeriodIndex(xau_grouping_col, freq = period_abbr).start_time) + pd.DateOffset(months = 1)
    else:
        start_of_next_period = None
        
    # Convert the date time from the previous step back into the appropriate 
    # period (week or month) 
    if start_of_next_period is not None:
        next_period = pd.Series(start_of_next_period).dt.to_period(period_abbr)
    else:
        next_period = None
    
    # Return the next period
    return next_period




### create_xau_decorated_df is a generic function that allows us to find WAU
### Decorated or MAU Decorated from a DAU Decorated based on the time period
### that gets passed in
def create_xau_decorated_df(dau_decorated_df, time_period, use_segment):
    
    # These are the parameters that are set from the get_time_period_dict 
    # function above
    time_fields = get_time_period_dict(time_period)
    grouping_col = time_fields['grouping_col']
    frequency = time_fields['frequency']
    first_period_col = time_fields['first_period_col']
    period_abbr = time_fields['period_abbr']
    
    # Print a notification message indicating that this function has been called
    print('Creating ' + frequency + ' Active Users Decorated dataframe')
    
    # We are grouping by the grouping_col (which is either "Week" or "Month_Year"),
    # the user_id, and the first_period_col (either "first_week" or "first_month")
    # For each user_id, there is one and only one first_period_col
    groupby_cols = [grouping_col, 'user_id', first_period_col]
    if use_segment: 
        groupby_cols = groupby_cols + ['segment']
        
    # Start by making a copy of the dataframe that gets passed in so as not to
    # affect the original
    dau_decorated = dau_decorated_df.copy()
    
    # Convert the activity_date for each transaction in dau_decorated to a 
    # period the same timeframe as the period in question (either a week or a 
    # month)
    dau_decorated[grouping_col] = pd.to_datetime(dau_decorated['activity_date']).dt.to_period(period_abbr)
    
    # Group dau_decorated into the grouping_cols defined above and aggregate the
    # sum of the inc_amt field
    xau = (dau_decorated.groupby(groupby_cols, as_index = False)['inc_amt'].sum())
    
    # Set a new column with the next time period by calling the increment_period
    # function defined above
    xau['Next_' + grouping_col] = increment_period(xau[grouping_col], time_period)
    
    # Select a subset of the resultant columns from the groupby to output
    output_cols = [grouping_col, 'user_id', 'inc_amt', first_period_col, 'Next_' + grouping_col]
    if use_segment:
        output_cols = output_cols + ['segment']

    xau = xau[output_cols]
    
    # Return the resultant dataset
    return xau





### This takes one to many rows of growth accounting figures for a specific
### date and calculates the user quick ratio
### It is used when calculating standard or rolling quick ratio
def calc_user_qr(row, 
                 new_col = 'new_users', 
                 res_col = 'resurrected_users', 
                 churned_col = 'churned_users'):
    new_users = row[new_col] if hasattr(row, new_col) and pd.notnull(row[new_col]) else 0
    res_users = row[res_col] if hasattr(row, res_col) and pd.notnull(row[res_col]) else 0
    churned_users = row[churned_col] if hasattr(row, churned_col) and pd.notnull(row[churned_col]) else 0
    if churned_users < 0:
        user_qr = -1 * (new_users + res_users) / churned_users
    else:
        user_qr = math.nan
        
    return user_qr




### This takes a row of growth accounting figures for a specific
### date and calculates the user quick ratio
def calc_rev_qr(row, new_col = 'new_revenue', res_col = 'resurrected_revenue', 
                churned_col = 'churned_revenue', exp_col = 'expansion_revenue', 
                con_col = 'contraction_revenue'):
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
### Reminder: the .t suffix stands for "this month" and .l stands for "last month"
def calc_user_ga(x, grouping_col, first_period_col):
  
    # au = Active Users = the count of unique user_id's this period
    au = x.loc[~x[grouping_col + '.t'].isnull(), 'user_id'].nunique() 
    
    # ret_users = retained_users = unique user_ids that transacted this period 
    # and last
    ret_users = (x.loc[(x['inc_amt.t'] > 0) & (x['inc_amt.l'] > 0), 
                       'user_id']
                 .nunique())
    
    # new_users = unique user_id's for whom  this period's first_period_col is 
    # the same as this period's grouping_col
    new_users = (x.loc[x[first_period_col + '.t'] == x[grouping_col + '.t'], 
                       'user_id']
                 .nunique())
    
    # res_users = resurrected users = transacted this period but not last, and
    # this is not their first period to transact
    res_users = (x.loc[(x[first_period_col + '.t'] != x[grouping_col + '.t']) & 
                       ~(x['inc_amt.l'] > 0), 
                       'user_id']
                 .nunique())
    
    # churned_users = transacted last period but not this one
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
                                 add_hours = False,
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
    
    if add_hours:
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
def calc_user_ga_ratios(user_xga_df, 
                        time_period, 
                        use_segment = False, 
                        growth_rate_periods = 12):
    
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
            this_ratio_df = user_xga_df.copy().loc[user_xga_df['segment'] == s].reset_index(drop=True)
        else:
            this_ratio_df = user_xga_df.copy().reset_index(drop=True)
            this_ratio_df['segment'] = s
            
        this_ratio_df['Users BOP'] = this_ratio_df[frequency + ' Active Users'].shift(1)
        this_ratio_df[per + 'o' + per + ' User Retention'] = this_ratio_df['Retained Users'] / this_ratio_df['Users BOP']
        this_ratio_df['User Quick Ratio'] = this_ratio_df.apply(lambda x: calc_user_qr(x, 
                     new_col = 'New Users', res_col = 'Resurrected Users', 
                     churned_col = 'Churned Users'), axis = 1)
    
        cgr_col = 'User C%sGR%s' % (per, growth_rate_periods)
        this_ratio_df[cgr_col] = np.power((this_ratio_df[frequency + ' Active Users'] / \
                     this_ratio_df[frequency + ' Active Users'].shift(growth_rate_periods)), 1/growth_rate_periods)-1
        
        ratio_df = ratio_df.append(this_ratio_df)
    
    # The Growth Threshold and growth rate target are constants for display purposes
    ratio_df['Growth Threshold'] = 1.0
    ratio_df[cgr_col + ' Target'] = 0.1
    
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
        
        cgr_col = 'Revenue C%sGR%s' % (per, growth_rate_periods)
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
                       include_zero_inc = False,
                       add_hours = False,
                       use_standard_col_names = True):
    
    user_ga, rev_ga = create_growth_accounting_dfs(xau_decorated_df, time_period, 
                                                   use_segment, keep_last_period, 
                                                   date_limit, add_hours, include_zero_inc)
    user_ga_with_ratios = calc_user_ga_ratios(user_ga, time_period, use_segment, growth_rate_periods)
    rev_ga_with_ratios = calc_rev_ga_ratios(rev_ga, time_period, use_segment, growth_rate_periods)
    all_ga_df = consolidate_ga_dfs(user_ga_with_ratios, rev_ga_with_ratios, time_period)
    
    time_fields = get_time_period_dict(time_period)
    frequency = time_fields['frequency']
    all_ga_df['Revenue per User'] = all_ga_df[frequency + ' Revenue'] / \
                                    all_ga_df[frequency + ' Active Users']
                                    
    if use_standard_col_names:
        mapping = {'month_year' : 'month',
                   'users_bop' : 'active_users_bop'
                   }
        
        cols = all_ga_df.columns
        new_all_ga_cols = []
        for c in cols:
            new_c = '_'.join(c.lower().split(' '))
            if 'active_users' in new_c:
                new_c = 'active_users'
            if 'revenue' in new_c:
                new_c = new_c.replace('revenue', 'rev')
            if 'mom_' in new_c:
                new_c = new_c.replace('mom_', '')
            if 'wow_' in new_c:
                new_c = new_c.replace('wow_', '')
            if new_c in mapping:
                new_c = mapping[new_c]
            new_all_ga_cols.append(new_c)
        all_ga_df.columns = new_all_ga_cols
    
    return all_ga_df




def add_period_n_cum_inc_per_cohort_cust_columns(cohort_df, since_col, unit):
    c_df = cohort_df.copy()
    since_vector = c_df[since_col].unique()
    for n in since_vector:
        new_column_name = unit + ' %s' % n
        c_df[new_column_name] = c_df['cum_inc_per_cohort_cust'] * (c_df[since_col] == n)
        c_df[new_column_name] = c_df[new_column_name].replace(0, np.nan)
    return c_df
    



### Calculate the user retention and empirical CLTV using inc_amtby cohort 
### defined by any weekly or monthly time period
def create_xau_cohort_df(xau_decorated_df, 
                         time_period, 
                         use_segment = False,
                         recent_periods_back_to_exclude = 1, 
                         date_limit = None,
                         create_period_n_inc_cols = False,
                         add_hours = False,
                         use_standard_col_names = False):
    
    # These are the parameters that are set from the get_time_period_dict 
    # function above    
    time_fields = get_time_period_dict(time_period)
    grouping_col = time_fields['grouping_col']
    first_period_col = time_fields['first_period_col']
    unit = time_fields['unit']
    period_abbr = time_fields['period_abbr']
        
    # If a date_limit is set, set that date as the max date in the 
    # dau_decorated_df dataframe passed into the function.
    # Otherwise, make a copy of the entire input dau_decorated_df.
    if date_limit is not None:
        xau_d = (xau_decorated_df[pd.PeriodIndex(xau_decorated_df[grouping_col], 
                                                 freq = period_abbr)
                                  .start_time <= date_limit]
                 .copy()
                )
    else:
        xau_d = xau_decorated_df.copy()
    
    # Set the since_col variable to say "Months Since First" or "Weeks Since First"
    since_col = '%ss Since First' % unit
    
    # Calculate the value in the since_col to be the number of periods between
    # the current period and the user's first period
    xau_d[since_col] = xau_d[grouping_col] - xau_d[first_period_col]
    
    # Since we are aggregating it all by the cohort of users that started in a
    # particular period, we set the group by columns for the first aggregation
    # as the first_period_col, grouping_col, and since_col
    # first_period_col = {'first_month' | 'first_week' | 'first_day'}
    # grouping_col = {'Month_Year' | 'Week' | 'activity_date'}
    # since_col = {'Months Since First' | 'Weeks Since First' | 'Days Since First'}
    first_groupby_cols = [first_period_col, grouping_col, since_col]
    
    # If we are including the segment in the calculations, include it in the 
    # groupby columns
    if use_segment:
        first_groupby_cols = first_groupby_cols + ['segment']
    
    # Group xau_d by the first_groupby_cols to find the sum of inc_amt and
    # the number of unique user_ids in each grouping
    xau_d = xau_d.groupby(first_groupby_cols)\
                    .agg({'inc_amt' : 'sum', 
                          'user_id' : 'nunique'})\
                    .rename(columns = { 'user_id' : 'cust_ct' })
                    
    # For the second groupby, we reduce the columns down to the first_period_col
    # and the segment (if applicable)
    second_groupby_cols = [first_period_col]
    if use_segment:
        second_groupby_cols = second_groupby_cols + ['segment']
    
    # The second groupby is used to make calculations about the cohort as a whole
    # rather than at the individual period level
    # The first of such calculations is to take the first value for cust_ct
    # (customer count) as being the number of customers in the cohort
    xau_d['cohort_cust_ct'] = xau_d.groupby(second_groupby_cols)['cust_ct'].transform('first')
    
    # The second calculation at the cohort level is to get the cumulative sum
    # of the inc_amt for each period's cohort
    xau_d['cum_inc_amt'] = xau_d.groupby(second_groupby_cols)['inc_amt'].cumsum()
    
    # These ratios are calculated per period using the per-cohort numbers calculated
    # using the second groupby
    # First we calculate cum_inc_per_cohort_cust (cumulative income per cohort
    # customer) as that period's cumulative inc_amt divided by the total number 
    # of customers in the first period of the cohort
    xau_d['cum_inc_per_cohort_cust'] = xau_d['cum_inc_amt'] / xau_d['cohort_cust_ct']
    
    # We also calculate how many users from the original count of cohort customers
    # is still active in the current period. This is the cust_ret_pct (customer
    # retention percentage)
    xau_d['cust_ret_pct'] = xau_d['cust_ct'] / xau_d['cohort_cust_ct']
    
    # Reset the index on the Pandas dataframe
    xau_d = xau_d.reset_index()
    
    # The code below removes rows, adds columns, or tweaks some of the time
    # columns to allow for presentation in certain cases.
    
    # If the time period in question is 'day', then the logic works somewhat
    # differently than it does if it is a 'week' or 'month'. Week/Month are in 
    # the top "if" clause, while Day is in the "else" clause.
    if time_period != 'day':
      
        # If the time period is a month, and we have recent periods back to 
        # exclude (an input parameter), we use this code to exclude those 
        # periods from xau_d. The periods back is measured from TODAY's date.
        if time_period == 'month':
            td = pd.DateOffset(months = recent_periods_back_to_exclude)
        elif time_period == 'week':
            td = timedelta(weeks = recent_periods_back_to_exclude)
        
        last_period = pd.to_datetime(datetime.today() - td).to_period(period_abbr)
        xau_d = xau_d.loc[xau_d[grouping_col] <= last_period]
        
        # If we want to add seven hours to the first_period_col and grouping_col
        # we do so here if the add_hours input parameter is True. It defaults
        # to False
        if add_hours:
            xau_d[first_period_col] = (pd.PeriodIndex(xau_d[first_period_col], 
                                                      freq = period_abbr)
                                       .start_time + timedelta(hours = 7))
            xau_d[grouping_col] = (pd.PeriodIndex(xau_d[grouping_col], 
                                                  freq = period_abbr)
                                   .start_time + timedelta(hours = 7))
        
        # Using the segment column requires us to specify which segment each
        # first_period_col goes with. 
        # Creator's note: needs string to handle the weekly case as well as the monthly
        if use_segment:
            xau_d['segment_first_' + time_period] = (xau_d[first_period_col]
                                                     .dt
                                                     .strftime('%Y-%m') + '-' + xau_d['segment'])  
        
        # If we want to add new columns for weekly trend analysis, we would do
        # so by setting the create_period_n_inc_cols equal to True, which would
        # trigger the add_period_n_cum_inc_per_cohort_cust_columns function
        # to run. The parameter defaults to False.
        if create_period_n_inc_cols:
            xau_d = add_period_n_cum_inc_per_cohort_cust_columns(xau_d, since_col, unit)

    else:
        
        # In the case of days, excluding the last period involves using 
        # timedelta to subtract days from today's date
        last_period = (datetime.today() - timedelta(days = recent_periods_back_to_exclude)).date()
        xau_d = xau_d.loc[xau_d[grouping_col] <= last_period]
        
        # The add_hours piece uses timedelta(hours = 7)
        if add_hours:
            xau_d[first_period_col] = xau_d[first_period_col] + timedelta(hours = 7)
            xau_d[grouping_col] = xau_d[grouping_col] + timedelta(hours = 7)
            
        # Change the data type on the since_col to be a number and not a timedelta    
        xau_d[since_col] = xau_d[since_col].astype(timedelta).map(lambda x: np.nan if pd.isnull(x) else x.days)
        
        # Using the segment column requires us to specify which segment each
        # first_period_col goes with. 
        if use_segment:
            xau_d['segment_first_' + time_period] = (xau_d[first_period_col]
                                                     .dt
                                                     .strftime('%Y-%m-%d') + '-' + xau_d['segment'])
        
        # Similar to above for adding weekly trend analysis, we would do
        # so by setting the create_period_n_inc_cols equal to True, which would
        # trigger the add_period_n_cum_inc_per_cohort_cust_columns function
        # to run. The parameter defaults to False.
        if create_period_n_inc_cols:
            xau_d = add_period_n_cum_inc_per_cohort_cust_columns(xau_d, since_col, unit)
            
    # This last bit of code changes column names if the input parameter
    # use_standard_col_names is True. It defaults to False
    if use_standard_col_names:
        mapping = {'month_year' : 'month',
                   'cust_ret_pct' : 'retained_pctg',
                   since_col : time_period + 's_since_first'
                   }
        
        cols = xau_d.columns
        new_xau_cols = []
        for c in cols:
            new_c = '_'.join(c.lower().split(' '))
            if new_c in mapping:
                new_c = mapping[new_c]
            new_xau_cols.append(new_c)
        
        xau_d.columns = new_xau_cols

    
    return xau_d

### ^^^^^^ Future enhancement: add ability to fill in empty months.



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



def classify_users_and_revenue(x):
    is_last_period = pd.notnull(x.last_period) if hasattr(x, 'last_period') else False
    is_this_period = pd.notnull(x.this_period) if hasattr(x, 'this_period') else False
    is_first_this_period = pd.notnull(x.first_this_period) if hasattr(x, 'first_this_period') else False
    
    ret_user = 0
    new_user = 0
    res_user = 0
    churned_user = 0
    
    ret_rev = 0
    new_rev = 0
    res_rev = 0
    exp_rev = 0
    churned_rev = 0
    con_rev = 0
    
    if is_first_this_period:
        new_user = 1
        new_rev = x['first_this_period']
    elif is_this_period and not is_last_period:
        res_user = 1
        res_rev = x['this_period']
    elif is_last_period and not is_this_period:
        churned_user = -1
        churned_rev = -1*x['last_period']
    elif is_last_period and is_this_period:
        ret_user = 1
        diff = x['this_period'] - x['last_period']
        if diff >= 0:
            ret_rev = x['last_period']
            exp_rev = diff
        else:
            ret_rev = x['this_period']
            con_rev = diff
    
    return [ret_user, new_user, res_user, churned_user, 
            ret_rev, new_rev, res_rev, exp_rev, churned_rev, con_rev]



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
    
    if not use_segment:
        dau_dec['segment'] = 'All'
        
    groupings = ['user_id', 'segment', 'ga_date_range']
    dau_grouped = (dau_dec.groupby(groupings)['inc_amt']
                                .sum()
                                .unstack()
                                .reset_index()
                                )
          
#    dau_grouped['user_status'] = dau_grouped.apply(assign_user_status, axis = 1)
    
    dau_grouped[['retained_users', 'new_users', 'resurrected_users',
                 'churned_users', 'retained_revenue', 'new_revenue', 
                 'resurrected_revenue', 'expansion_revenue', 'churned_revenue',
                 'contraction_revenue']] = dau_grouped.apply(classify_users_and_revenue, 
                                                             axis = 1, 
                                                             result_type='expand')

    dau_grouped_2 = dau_grouped.groupby('segment').sum().reset_index()
    dau_grouped_2['window_end_date'] = last_date
    
    this_per_users = dau_grouped_2['retained_users'] + dau_grouped_2['new_users'] + dau_grouped_2['resurrected_users']
    last_per_users = dau_grouped_2['retained_users'] - dau_grouped_2['churned_users']
       
    dau_grouped_2['active_users'] = this_per_users
    dau_grouped_2['user_quick_ratio'] = dau_grouped_2.apply(calc_user_qr, axis = 1)
    dau_grouped_2['user_retention_rate'] = dau_grouped_2['retained_users'] / last_per_users
    dau_grouped_2['pop_user_growth_rate'] = this_per_users / last_per_users - 1
    
    first_this_per_rev = dau_grouped_2['first_this_period'] if hasattr(dau_grouped_2, 'first_this_period') else 0
    this_period_rev = dau_grouped_2['this_period'] if hasattr(dau_grouped_2, 'this_period') else 0
    last_per_rev = dau_grouped_2['last_period'] if hasattr(dau_grouped_2, 'last_period') else 0
    this_per_rev = this_period_rev + first_this_per_rev 
        
    dau_grouped_2['revenue_quick_ratio'] = dau_grouped_2.apply(calc_rev_qr, axis = 1)
    dau_grouped_2['revenue_retention_rate'] = dau_grouped_2['retained_revenue'] / last_per_rev
    dau_grouped_2['pop_revenue_growth_rate'] = this_per_rev / last_per_rev - 1
    
    dau_grouped_2['window_days'] = window_days
    dau_grouped_2['Growth Threshold'] = 1
        
    return dau_grouped_2
    

    
### Calls calc_ga_for_window for each available window of length window_days
### and compiles the resultant dataframe into a single dataframe for plotting
### and analysis
def calc_rolling_qr_window(dau_decorated_df, 
                           window_days = 28, 
                           use_segment = False,
                           use_final_day = True
                           ):
    
    if use_final_day:
        end_dt = max(dau_decorated_df['activity_date'])
    else:
        end_dt = max(dau_decorated_df['activity_date']) - timedelta(days = 1)
    
    start_dt = min(dau_decorated_df['activity_date']) + timedelta(days = 2*window_days)
    
    date_range = pd.date_range(start = start_dt, end = end_dt, freq = 'D')

    rolling_qr_df = pd.DataFrame()
    for d in date_range:
        d2 = d.date()
        print(window_days, d2)
        this_window = calc_ga_for_window(dau_decorated_df, d2, window_days, use_segment)
        rolling_qr_df = rolling_qr_df.append(this_window)
        
    rolling_qr_df['window_end_date'] = pd.to_datetime(rolling_qr_df['window_end_date'])
    return rolling_qr_df


    
    
  
# The calc_user_periodic_usage function takes the dau_decorated dataframe 
# calculated above, determines a range of dates using the last_date and 
# window_days inputs, and calculates the total number of active periods
# and the total income amount for each user. The breakouts input allows us to 
# see if a user_id is above an active periods threshold (T|F). 
def calc_user_periodic_usage(dau_decorated_df, 
                             time_period, 
                             last_date, 
                             window_days, 
                             breakouts, 
                             use_segment 
                             ):
    
    # We need to know the start date of our window. We calculate it by
    # subtracting window_days-1 days from the last_date input parameter
    
    window_start_date = last_date - timedelta(days = window_days-1)
    
    # Create a copy of the dau_decorated_df input dataframe that isolates the
    # activity_dates between the start and end dates of the window. Call it xau.
    
    xau = (dau_decorated_df
           .loc[(dau_decorated_df['activity_date'] >= window_start_date) & 
                (dau_decorated_df['activity_date'] <= last_date)]
           .copy()
           )
    
    # Make sure the activity_date column is a datetime type
    xau['activity_date'] = pd.to_datetime(xau['activity_date'])
    
    # These are the parameters that are set from the get_time_period_dict 
    # function above
    time_fields = get_time_period_dict(time_period)
    period_abbr = time_period[0] # first letter of the time period (lowercase)
    active_col_name = 'active_' + time_period + 's'
    if time_fields is None:
        period_days = 1
    else:
        period_days = time_fields['days']
    
    # These three lines create a dataframe called periods_df that has one row
    # for each date in the window range, and determines which period (day, week,
    # or month) that date is in. The window_period_number is the ordinal number
    # of that period in the dataframe. In the case of days, there are as many
    # window_period_number's as there are days in the range.
    date_range = pd.date_range(start = window_start_date, end = last_date, freq = 'D')
    periods_df = pd.DataFrame({'activity_date' : date_range})
    periods_df['window_period_number'] = periods_df.index // period_days
    ### Future enhancement: Put in option for going with discrete time periods
            
    # If we are NOT using segment, we go ahead and set a columnn called segment
    # to make the later groupby's easier
    if not use_segment:
        xau['segment'] = 'All'
        
    # This chain of Pandas functions merges the xau dataframe (which starts at
    # the daily level from dau_decorated_df) with the periods_df dataframe
    # to attached a window_period_number to each record. Then it groups by the 
    # user_id, window_period_number, and segment and to find the sum of inc_amt
    # for each user in each period and segment. Then we do a second
    # group by, this time removing the window_period_number. This lets us count 
    # the number of periods per user/segment. After that we just reset the 
    # index and clean up column names
    xau_grouped = (xau.merge(periods_df, on = 'activity_date', how = 'left')
                    .groupby(['user_id', 'window_period_number', 'segment'], as_index = False)
                    ['inc_amt'].sum()
                    .groupby(['user_id', 'segment'])
                    ['inc_amt'].agg(['count', 'sum'])
                    .reset_index()
                    .rename(columns = {'count' : active_col_name, 'sum' : 'inc_amt'})
                    )
    
    # Breakouts allow us to see very easily which users are above the number
    # of periods specified in the breakout list. For example, if I want to know
    # which users are active 2+ periods and also 4+ periods, I would set
    # breakouts = [2,4]. 
    for b in breakouts:
        col_name = '%s%s+ users' % (b, period_abbr)
        xau_grouped[col_name] = (xau_grouped[active_col_name] >= b)
        
    # Before returning the dataframe to the calling function, sort the values
    # in descending order of inc_amt
    xau_grouped_sorted = xau_grouped.sort_values('inc_amt', ascending = False)  
    
    return xau_grouped_sorted     
    



def calc_inc_dist(dau_decorated_df, 
                  window_days, 
                  use_segment
                  ):
    
    xau_grouped = calc_user_periodic_usage(dau_decorated_df = dau_decorated_df,
                                           time_period = 'day',
                                           last_date = dau_decorated_df['activity_date'].max(),
                                           window_days = window_days,
                                           breakouts = [],
                                           use_segment = use_segment
                                           )
    
    
    xau_grouped_sorted = xau_grouped.sort_values('inc_amt', ascending = False)  
    xau_grouped_sorted['cum_inc_amt'] = (xau_grouped_sorted
                                          .groupby(['segment'])
                                          .agg({'inc_amt' : 'cumsum'})
                                          )
    
    total_inc_amt_df = (xau_grouped_sorted
                        .groupby(['segment'])['inc_amt']
                        .agg(['sum', 'count'])
                        .reset_index()
                        .rename(columns = {'sum' : 'total_inc_amt',
                                           'count' : 'total_user_count'})
                        )
    
    xau_grouped_sorted = xau_grouped_sorted.merge(total_inc_amt_df, 
                                                  how = 'left', 
                                                  on = 'segment')
    
    xau_grouped_sorted['cum_inc_amt_pct_of_total'] = (xau_grouped_sorted.cum_inc_amt / 
                                                      xau_grouped_sorted.total_inc_amt)
    
    revenue_80pct_df = (xau_grouped_sorted[xau_grouped_sorted.cum_inc_amt_pct_of_total <= .80]
                        .groupby(['segment'])['cum_inc_amt_pct_of_total']
                        .count()
                        .reset_index()
                        .rename(columns = {'cum_inc_amt_pct_of_total' : 'revenue_80pct_user_count'})
                        )
    
    xau_grouped_sorted = xau_grouped_sorted.merge(revenue_80pct_df, 
                                                  how = 'left', 
                                                  on = 'segment')
    
    xau_grouped_sorted['revenue_80pct_ratio'] = (xau_grouped_sorted['revenue_80pct_user_count'] / 
                                                  xau_grouped_sorted['total_user_count'])
    
    xau_grouped_sorted['inc_decile'] = (pd.qcut(xau_grouped_sorted['inc_amt'].rank(method='first'), 
                                          10, 
                                          labels=range(1,11))).astype('int64')
    
    return xau_grouped_sorted



def calc_xau_hist(dau_decorated, time_period, last_date, window_days, use_segment):
    
    # Call calc_user_periodic_usage, the function defined above
    xau_grouped = calc_user_periodic_usage(dau_decorated_df = dau_decorated, 
                                           time_period = time_period, 
                                           last_date = last_date, 
                                           window_days = window_days, 
                                           breakouts = [],
                                           use_segment = use_segment
                                           )
    
    # Define three column names based on the time period parameter
    active_col_name = 'active_' + time_period + 's'
    active_bin_name = active_col_name + '_bin'
    avg_active_name = 'avg_' + time_period + 's_active'
    
    # Create a dataframe called counts_df that counts the values of the 
    # column that holds the number of active periods. In this way it counts all
    # users with the same number of active periods as part of the same bin.
    counts_df = (xau_grouped[active_col_name]
                    .value_counts()
                    .reset_index()
                    .rename(columns = {'index' : active_bin_name, 
                                       active_col_name : 'user_count'})
                    .sort_values(active_bin_name, ascending=True)
                    )

    # Create a blank dataframe with bin names and zeros to handle the cases
    # where a bin has no users
    blank_hist_df = pd.DataFrame({active_bin_name : range(1, window_days + 1),
                                  'user_count' : 0})
            
    # This is like an SQL union that appends the two data frames together. Then
    # we take the max value in each bin
    hist_df = counts_df.append(blank_hist_df).groupby(active_bin_name, as_index=False).max()

    # Calculate the weighted average active days in the 28-day period and add
    # a column to hold that constant
    avg_active = ((hist_df[active_bin_name] * 
                   hist_df['user_count']).sum() / 
                    hist_df['user_count'].sum()
                    )                        
    hist_df[avg_active_name] = avg_active
    
    # Make sure the active_days_bin column is a string category, not an integer
    hist_df[active_bin_name] = hist_df[active_bin_name].astype('str', copy=False)
    
    return hist_df
  
    
    

### The calc_engagement_ratios_for_window function runs calc_user_periodic_usage
### on one window, which has a length defined by window_days. It then calculates
### summary statistics and stores them in a small dataframe just for that window

def calc_engagement_ratios_for_window(dau_decorated_df, 
                                      time_period, 
                                      last_date, 
                                      window_days, 
                                      breakouts, 
                                      use_segment
                                      ):
  
    # Call calc_user_periodic_usage, the function defined above
    xau_grouped = calc_user_periodic_usage(dau_decorated_df, 
                                           time_period, 
                                           last_date, 
                                           window_days, 
                                           breakouts, 
                                           use_segment)
    
  
    # These are the parameters that are set from the get_time_period_dict 
    # function above
    time_fields = get_time_period_dict(time_period)
    period_abbr = time_period[0]
    active_col_name = 'active_' + time_period + 's'
    if time_fields is None:
        period_days = 1
    else:
        period_days = time_fields['days']
    total_users_col = '1' + period_abbr + '+ users'
    
    # Create a blank dataframe to store the statistics about xau_grouped
    xau_agg = pd.DataFrame()
    
    # Set the grouped_df to either be ungrouped or grouped by segment
    if use_segment:
        grouped_df = xau_grouped.groupby('segment')
    else:
        grouped_df = xau_grouped

    # In the blank dataframe, store the sum of the active periods
    # in a column (one row per segment if applicable)
    xau_agg[active_col_name] = pd.Series(grouped_df[active_col_name].sum())
    
    # In another column, store the count of unique user_id's
    xau_agg[total_users_col] = grouped_df['user_id'].nunique()
    
    # In another column, store the average number of active days per user
    # during the window
    xau_agg[period_abbr + 'au_window_ratio'] = (xau_agg[active_col_name] / (window_days/period_days)) / xau_agg[total_users_col]
    
    # In another column store the window_frequency, which is the average number
    # of periods in the window. This is like the DAU/MAU ratio.
    xau_agg['window_frequency'] = xau_agg[period_abbr + 'au_window_ratio'] * (window_days/period_days)
    
    # For each of the breakouts, calculate the gross number and the number
    # as a percentage of all active users
    for b in breakouts:
        col_name = '%s%s+ users' % (b, period_abbr)
        xau_agg[col_name] = grouped_df[col_name].sum()
        ratio_col_name = '%s%s+ users / total %sd users' % (b, period_abbr, window_days)
        xau_agg[ratio_col_name] = xau_agg[col_name] / xau_agg[total_users_col]
        
    # Store the window end date, because this will be appended to other iterations
    # of this function call. Then reset the index and return the dataframe.
    xau_agg['window_end_dt'] = last_date
    xau_agg = xau_agg.reset_index()
    
    return xau_agg




### The create_xau_window_df is what iterates through all possible windows
### of length window_days in the data set and builds a dataframe with summary
### statistics of each window, as calculated by calc_engagement_ratios_for_window
### defined above. Be forewarned, it takes a few minutes to run. That's why
### we include progress statements periodically during the loop.
  
def create_xau_window_df(dau_decorated_df, 
                         time_period = 'day',
                         window_days = 28, 
                         breakouts = [2, 4], 
                         use_segment = False,
                         use_final_day = True
                         ):
    
    # Set the start date as window_days after the first activity_date in the
    # dau_decorated_df dataframe.
    start_dt = dau_decorated_df['activity_date'].min() + timedelta(days = window_days)
    
    # Set the final day as either the last activity_date in the data set
    # or the next-to-last activity_date. (You may want to set use_final_day
    # equal to False if you have an incomplete day of data.)
    if use_final_day:
        end_dt = dau_decorated_df['activity_date'].max()
    else:
        end_dt = dau_decorated_df['activity_date'].max() - timedelta(days = 1)
        
    # Set a Pandas date_range from the start date to the end date, by day
    date_range = pd.date_range(start = start_dt, end = end_dt, freq = 'D')

    # Initialize the dataframe that will house the engagement stats from every
    # window in the loop
    rolling_engagement_df = pd.DataFrame()
    i = 0
    total_dates = len(date_range)
    print(('%s total ' + time_period + 's to process...') % total_dates)
    
    # Loop through each date in the date range, calling calc_engagement_ratios_for_window
    # each time, storing it in the this_window dataframe, and then appending
    # this_window to rolling_engagement_df
    for d in date_range:
        if i % 100 == 0:
          print(('Processing ' + time_period + ' %s of %s...') % (i, total_dates))
      
        d2 = d.date()
        this_window = calc_engagement_ratios_for_window(dau_decorated_df, 
                                                    time_period = time_period,
                                                    last_date = d2, 
                                                    window_days = window_days, 
                                                    breakouts = breakouts,
                                                    use_segment = use_segment)
        rolling_engagement_df = rolling_engagement_df.append(this_window)
        i+=1
    print(('Finished processing all %s ' + time_period + 's!') % total_dates)
    
    # Make sure the window_end_dt field is a Pandas datetime
    rolling_engagement_df['window_end_dt'] = pd.to_datetime(rolling_engagement_df['window_end_dt'])
    
    return rolling_engagement_df
