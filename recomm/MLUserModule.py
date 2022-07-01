# ==================================================================================================
# MLUserModule
# ==================================================================================================

import pickle
import datetime
import os
import pandas as pd
import numpy as np
import dateutil
from dateutil.parser import parse
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import *
import time


# ==================================================================================================
# DataTime 계산
# ==================================================================================================

def DatetimeCalMonth(idx_date, idx_month):
    returnDate = datetime.strftime(datetime.strptime(idx_date, "%Y%m%d") 
                                  - relativedelta(months = idx_month), "%Y%m%d")
    return (returnDate)

# ==================================================================================================
# Binning 만들기
# ==================================================================================================

def MakeBinning(data, idxVar, idxName, listinterval):
    """data : input dataframe, idxVar : Cont Var, idxName : binning Name, listinterval: """
    returnData = data.copy()    
    bins = np.array(listinterval)
    bins = bins.astype(returnData[idxVar].dtype)
    bins = np.nextafter(bins, bins + (bins <= 0))

    returnData[idxName] = np.digitize(returnData[idxVar], bins)
    return (returnData)

# ==================================================================================================
# ==================================================================================================
# # Create Directory

def CreateDir(directory):
    try :
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print("Error: Failed to create the directory")

# ==================================================================================================
# DropColumns
# sorting 
# ==================================================================================================

def DropColumns(raw_dataframe, list_columns = None, conditions = None):
    dataframe = raw_dataframe.copy()
    if list_columns != None:
        dataframe = dataframe.loc[:, ~dataframe.columns.isin(list_columns)]        
    if conditions != None:
        dataframe = dataframe.loc[:, ~dataframe.columns.str.contains(conditions)]
    return(dataframe)

# ==================================================================================================
# SortColumns
# sorting 
# ==================================================================================================

def SortColumns(raw_dataframe, list_columns):
    dataframe = raw_dataframe.copy()
    dataframe = dataframe[list_columns + [x for x in dataframe.columns if x not in list_columns]]
    return(dataframe)

# ====================================================================================================
# 시작시간, 종료시간, 소요시간 확인
# ====================================================================================================
def print_start_time():
    start_time_now = datetime.now(timezone('Asia/Seoul'))
    start_time_str = datetime.strftime(start_time_now, format = '%Y-%m-%d %H:%M:%S')
    print(f'> START   TIME: {start_time_str}')
    print(f'> PROCESSING...')
    return (start_time_now)
    
def print_end_time(start_time_now):
    end_time_now = datetime.now(timezone('Asia/Seoul'))
    end_time_str = datetime.strftime(end_time_now, format = '%Y-%m-%d %H:%M:%S')
    print(f'> END     TIME: {end_time_str}')

    elp_time_now   = (end_time_now - start_time_now)
    hours, remainder = divmod(elp_time_now.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f'> ELAPSED TIME: {hours} hours, {minutes} minutes, {seconds} seconds')
