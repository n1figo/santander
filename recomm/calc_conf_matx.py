###########################################################################################
# 패키지 Import
###########################################################################################
import os
import pandas                 as pd
import numpy                  as np
from   datetime               import datetime, timedelta
from   dateutil.relativedelta import relativedelta

import warnings
warnings.filterwarnings('ignore')

#################################################
# 세그먼트별 cutoff 조정
#################################################

def calc_conf_matx(
          in_df      # 예측결과        > 필요컬럼 : ci번호, tgtyn(실제Target여부), prob(에측확률)
        , idx_mdl    # 모델명 넣기 RF, XG
        , idx_option # '10' > 10, 20, ..., 90, '1' > 1,2,3,4,5,,,,99
        , seg_df = pd.DataFrame()     # 세그먼트 데이터셋 > 필요컬럼 : ci번호, seg1, seg2, ...
    ):

    # 시작시간
    st_time = datetime.now()
    print(f'[LOG] calc_conf_matx > 시작시간 = {datetime.strftime(st_time, "%Y-%m-%d %H:%M:%S")}')      
        
    if seg_df.empty :
        seg_df = in_df[['ci번호']].copy()
        seg_df['segTotal'] = 'tot'

    in_df = in_df[['ci번호', 'tatyn', 'score']]

    # 시리즈 및 리스트 초기화
    df_rlt   = pd.DataFrame()
    sers_rlt = pd.Series()
    list_tot = []        
        
    # 입력데이터 복사
    in_df1 = in_df.copy()
    
    # 컬럼명 변환
    in_df1.columns = ['ci번호', 'y_real', 'y_pred_prob']
    
    # 입력데이터에 세그먼트 데이터 통합
    if 'DataFrame' in str(type(seg_df)):
        in_df2 = pd.merge(
              in_df1
            , seg_df
            , on = 'ci번호'
            , how = 'left'
        )
    else :
        in_df2 = in_df1.copy()
        
    # groupby 문장 생성
    tmp_col = pd.Series(in_df2.columns)

    if len(tmp_col[tmp_col.apply(lambda x: 'seg' in x)]) > 0:
        str_groupby = '['
        for i, x in enumerate(tmp_col[tmp_col.apply(lambda x: 'seg' in x)].tolist()):
            if i == 0: str_groupby += "'" + x + "'"
            else:      str_groupby += ", '" + x + "'" 
        str_groupby += ", 'y_real', 'y_pred']"
    else :
        str_groupby = "['y_real', 'y_pred']"
        
    # merge key 문장 생성
    if len(tmp_col[tmp_col.apply(lambda x: 'seg' in x)]) > 0:
        str_merge = '['
        for i, x in enumerate(tmp_col[tmp_col.apply(lambda x: 'seg' in x)].tolist()):
            if i == 0: str_merge += "'" + x + "'"
            else:      str_merge += ", '" + x + "'"         
        str_merge += ", 'type']"
    else :
        str_merge = "['type']"     
        
    # 세그먼트 문장 생성
    if len(tmp_col[tmp_col.apply(lambda x: 'seg' in x)]) > 0:
        str_seg = ''
        for i, x in enumerate(tmp_col[tmp_col.apply(lambda x: 'seg' in x)].tolist()):
            if i == 0: str_seg += "'" + x + "'"
            else:      str_seg += ", '" + x + "'"                 
    else :
        str_seg = ""      
    
    # 반복수행
    if idx_option == '10':
        idxRange = range(10, 100, 10)
    elif idx_option == '1':
        idxRange = range(1, 100)
    else :
        print(' put idx Range')
        
    for i in idxRange:
        if i % 50 == 0: print(f"[LOG] calc_conf_matx > 순서 = {i:2d}")

        # Cutoff 에 따른 예측결과 생성
        tmp = in_df2.copy()
        tmp['y_pred'] = np.where(tmp['y_pred_prob'] >= i / 100, '1', '0')
        
        # 레이아웃 생성
        tmp2_1 = pd.DataFrame({'type' : ['11', '10', '01', '00'], 'cnt' : [0, 0, 0, 0]})        
        if len(tmp_col[tmp_col.apply(lambda x: 'seg' in x)]) > 0:
            for j in tmp_col[tmp_col.apply(lambda x: 'seg' in x)].tolist():        
                tmp2_0 = in_df2[[j]].drop_duplicates()
                tmp2_0['tmp_key'] = '1'
                tmp2_1['tmp_key'] = '1'
                tmp2_1 = pd.merge(
                      tmp2_0
                    , tmp2_1
                    , on = 'tmp_key'
                ).drop('tmp_key', axis = 1) 
                        
        # 집계 : 세그먼트, 실제Y, 예측Y
        tmp2_2 = tmp.groupby(eval(str_groupby), as_index = False)['ci번호'].count().rename(columns = {'ci번호' : 'cnt'}) 
        tmp2_2['type'] = tmp2_2['y_real'] + tmp2_2['y_pred']                
        
        # 레이아웃에 집계결과 통합
        tmp3 = pd.merge(
              tmp2_1
            , tmp2_2.drop(['y_real', 'y_pred'], axis = 1)
            , on = eval(str_merge)
            , how = 'left'
        ).fillna(0)
        tmp3['cnt'] = tmp3['cnt_x'] + tmp3['cnt_y']  
                
        # 전치 : 유형(TP, FN, FP, TN)을 컬럼으로 변환
        if len(tmp_col[tmp_col.apply(lambda x: 'seg' in x)]) > 0:
            tmp4 = tmp3.pivot_table(index = eval(str_seg), columns = 'type', values = 'cnt').reset_index()
        else:
            tmp4 = tmp3.pivot_table(columns = 'type', values = 'cnt').reset_index().drop('index', axis = 1)
            
        # 추가정보 생성 : Cutoff 및 성능지표값
        tmp4['cutoff']    = i / 100       
        tmp4['accuracy']  = (tmp4['11'] + tmp4['00']) / (tmp4['11'] 
                                                        + tmp4['10'] + tmp4['01'] + tmp4['00'])
        tmp4['precision'] = (tmp4['11']) / (tmp4['11'] + tmp4['01'])
        tmp4['recall']    = (tmp4['11']) / (tmp4['11'] + tmp4['10'])
        tmp4['f1score']   = (2 * tmp4['precision'] * tmp4['recall']) / (tmp4['precision'] + tmp4['recall'])   
        
        tmp4['tgt_cnt']   = tmp4['11'] + tmp4['10']
        tmp4['pred_cnt']  = tmp4['11'] + tmp4['01']
        tmp4['tot_cnt']   = tmp4['11'] + tmp4['10'] + tmp4['01'] + tmp4['00']
        tmp4['pred_rt']   = (tmp4['11'] + tmp4['01']) / (tmp4['11'] + tmp4['10'] + tmp4['01'] + tmp4['00'])   
        
        # 최종데이터 생성
        keep_var = "'cutoff', '11', '10', '01', '00', 'accuracy', 'precision', 'recall', 'f1score', 'tgt_cnt', 'pred_cnt', 'tot_cnt', 'pred_rt'"
        if len(tmp_col[tmp_col.apply(lambda x: 'seg' in x)]) > 0:
            keep_var = str_seg + ',' + keep_var
            
        tmp4 = tmp4[eval('[' + keep_var + ']')].rename(columns = {'11' : '11_TP', '10' : '10_FN', '01' : '01_FP', '00' : '00_TN'})
        
        # 리스트 Append
        list_tot.append(tmp4)

    # 전체결과 생성
    df_rlt = pd.concat(list_tot)
    
    # 건수 int값으로 변환
    listInt = ['11_TP', '10_FN', '01_FP', '00_TN', 'tgt_cnt', 'pred_cnt', 'tot_cnt']
    df_rlt[listInt] = df_rlt[listInt].fillna(0).astype(int)
    
    # model 명 입력
    df_rlt['model'] = idx_mdl
    df_rlt = df_rlt[['model'] + [x for x in df_rlt.columns if x != 'model']]
    
    # 종료시간 및 소요시간
    end_time = datetime.now()
    el_time = end_time - st_time
    print(f'[LOG] calc_conf_matx > 종료시간 = {datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")}, 소요시간 = {str(el_time).split(".")[0]}')
    return df_rlt


def cfm_concat(dataframe, listMDL, cal_option, tmp_seg_df=pd.DataFrame()):
    tot_rslt = pd.DataFrame()
    for x in  listMDL:
        tmp_rslt = calc_conf_matx(dataframe, x, cal_option, tmp_seg_df)
        tot_rslt = pd.concat([tot_rslt, tmp_rslt], axis = 0)
    return (tot_rslt)

#################################################
# Rank 생성
#################################################

def RankStatIndex(raw_eval_data, idxStat, listgrp):
    eval_data = raw_eval_data.copy()
    eval_data['seg_cal'] = eval_data[listgrp].astype(str).sum(axis = 1)
    eval_data[f"{idxStat}_Rank"] = eval_data.groupby('seg_cal')[idxStat].rank(method = "first", ascending = False)
    return (eval_data)
