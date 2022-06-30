##########################################################################################################################
#    1.  Program Title  : BUMclrFRMdelDvlp.py
# random forest
#    3.  Source Data    : TrainSet_{idxRecCode}, TestSet_{idxRecCode}, PredSet_{idxRecCode}
#    4.  Target Data    : TCTAHMC17
#    5.  Create Date    : 2022/03/23
##########################################################################################################################
######################################################################################################################################################
# Module Import
######################################################################################################################################################

# 기본 모듈
import pandas as pd
import numpy as np
import re
import os, time
import warnings
import boto3
import sys
import tarfile
import json
import logging
from time import strftime
from dateutil.relativedelta import relativedelta
import datetime, time
from   datetime               import datetime, timedelta
from   dateutil.relativedelta import relativedelta
from sklearn import model_selection
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, roc_auc_score, r2_score
import warnings
from io import BytesIO
from pytz import timezone

# 모델 저장 및 불러오기에 사용된 모듈
import joblib

# 모델링에 사용된 모듈
from sklearn.neural_network import MLPClassifier
from xgboost import XGBClassifier
from sklearn.ensemble import RandomForestClassifier

# 옵션 설정
warnings.filterwarnings('ignore')
os.environ['TZ'] = 'Asia/Seoul'
time.tzset()

######################################################################################################################################################
# declare variables
######################################################################################################################################################
# 파티션 Drop 함수 호출
# bucket      = 'tah-prd-tah-s3-proj-3004'
# bucket      = 'tah-prd-tah-s3-proj-3003'
# bucket      = 'tah-prd-kl0-s3-proj-0005'
bucket      = 'tah-prd-kl0-s3-proj-0005'

s3_resource = boto3.resource("s3", region_name='ap-northeast-2')
s3_bucket   = s3_resource.Bucket(bucket)

# s3 에 저장
S3_CLIENT      = boto3.client("s3")

# DropPartition 함수 호출
dropPartition_object = s3_resource.Object(bucket, 'project_code/Functions/DropPartition.py')
exec(dropPartition_object.get()['Body'].read().decode('utf-8'))

# sqlToPandasDF 함수 호출
sqlToPandasDF_object  = s3_resource.Object(bucket, 'project_code/Functions/sqlToPandasDF.py')
exec(sqlToPandasDF_object.get()['Body'].read().decode('utf-8'))

# MLUserModule 함수 호출
MLUserModule_object  = s3_resource.Object(bucket, 'project_code/Functions/MLUserModule.py')
exec(MLUserModule_object.get()['Body'].read().decode('utf-8'))

# calc_conf_matx 함수 호출
calc_conf_matx_object  = s3_resource.Object(bucket, 'project_code/Functions/calc_conf_matx.py')
exec(calc_conf_matx_object.get()['Body'].read().decode('utf-8'))

# RUN CONFIG 설정
config_object = s3_resource.Object(bucket, 'project_code/run_config.json')
config_json   = json.load(config_object.get()['Body'])

# 공통변수 선언
workGrp    = config_json['WORK_GROUP']
srcDbNm    = config_json['SOURCE_DB_NAME']
workDbNm   = config_json['WORK_DB_NAME']


# PyAthena Import 및 접속(Import 에러 시 패키지 설치 후 Import)
def packageInstall(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    # 모듈 Import
    import pyathena
    from pyathena.pandas.util import to_sql
    
    # Athena 접속
    conn  = pyathena.connect(
          s3_staging_dir = f"s3://{bucket}/athena/output"
        , region_name    = "ap-northeast-2"
        , work_group     = f"{workGrp}"
    )    
except:
    # 패키지 설치
    # packageInstall("pyathena==2.4.1")
    os.system("pip install --no-index --find-link='/root/packages' pyathena")
    
    # 모듈 Import
    import pyathena
    from pyathena.pandas.util import to_sql
    
    # Athena 접속
    conn  = pyathena.connect(
          s3_staging_dir = f"s3://{bucket}/athena/output"
        , region_name    = "ap-northeast-2"
        , work_group     = f"{workGrp}"
    )     
 
##########################################################################################################################
# Parameter Setting
##########################################################################################################################
if (config_json['RUN_DATE'] == ''):
    baseYmd        = datetime.strftime(datetime.strptime(datetime.strftime(datetime.now(), "%Y%m") + '01', "%Y%m%d") - relativedelta(days = 1 ), "%Y%m%d")
else:
    baseYmd        = config_json['RUN_DATE']

baseYm         = baseYmd[0:6]

p_year         = baseYmd[0:4]
p_month        = baseYmd[4:6]
p_day          = baseYmd[6:8]
p_yyyymm       = baseYmd[0:6]

baseYmd_bfr_1m = datetime.strftime(datetime.strptime(baseYmd, "%Y%m%d") - relativedelta(months = 1 ), "%Y%m%d")
baseYmd_bfr_3m = datetime.strftime(datetime.strptime(baseYmd, "%Y%m%d") - relativedelta(months = 3 ), "%Y%m%d")
baseYmd_bfr_6m = datetime.strftime(datetime.strptime(baseYmd, "%Y%m%d") - relativedelta(months = 6 ), "%Y%m%d")
baseYmd_bfr_1y = datetime.strftime(datetime.strptime(baseYmd, "%Y%m%d") - relativedelta(months = 12), "%Y%m%d")

baseYmd_aft_1m = datetime.strftime(datetime.strptime(baseYmd, "%Y%m%d") + relativedelta(months = 1 ), "%Y%m%d")
baseYmd_aft_3m = datetime.strftime(datetime.strptime(baseYmd, "%Y%m%d") + relativedelta(months = 3 ), "%Y%m%d")
baseYmd_aft_6m = datetime.strftime(datetime.strptime(baseYmd, "%Y%m%d") + relativedelta(months = 6 ), "%Y%m%d")
baseYmd_aft_1y = datetime.strftime(datetime.strptime(baseYmd, "%Y%m%d") + relativedelta(months = 12), "%Y%m%d")

print(f'[LOG] Parameter Check: 기준년월 = {baseYm} , 기준년월일 = {baseYmd}')

# 재계산 + 1
baseYmd = DatetimeCalMonth(baseYmd, -1)

#################################################
# 계좌마트 및 개인화 마트 기준년월 Check
#################################################

baseYm_bfr_1m = DatetimeCalMonth(baseYmd, 1)[:6]
baseYm_bfr_2m = DatetimeCalMonth(baseYmd, 2)[:6]
baseYm_bfr_3m = DatetimeCalMonth(baseYmd, 3)[:6]

df_baseYmd = pd.read_sql(
f"""
select distinct "기준년월" from {srcDbNm[0:3]}_l2.TCTAHEA03
where concat(year, month) in ('{baseYm_bfr_1m}', '{baseYm_bfr_2m}', '{baseYm_bfr_3m}')
order by 1
""", conn)
listACCbaseYmd = df_baseYmd['기준년월'].tolist()

df_baseYmd2 = pd.read_sql(
f"""
select distinct "기준년월" from {workDbNm}.TCTAHMC01
where "p_기준년월" in ('{baseYm_bfr_1m}', '{baseYm_bfr_2m}', '{baseYm_bfr_3m}')
order by 1
"""
, conn)
listACCbaseYmd2 = df_baseYmd2['기준년월'].tolist()


#################################################
# 모델 훈련 함수
#################################################

def TrainData(train_data, mdl, listSelect = None):
    """
        train_data  : 신규가입여부 존재하는 string -> int변환 된 데이터
        mdl         : 모델
        listSelect  : 학습 변수 리스트
        train_model   : 학습 결과 모델
    """
    listKeyCol  = ['기준년월', 'ci번호', '추천코드', '신규가입여부', '샘플링비율', '나이', '연령', '예금투자자산', '수신만기도래보유여부']
    x_train     = train_data.loc[:,~train_data.columns.isin(listKeyCol)]
    x_train = x_train.loc[:, x_train.columns.isin(listSelect)].reset_index(drop = True)
    x_train = x_train[listSelect]
    
    y_train = train_data.loc[:, train_data.columns.isin(['신규가입여부'])].reset_index(drop = True)
    trainColumns = x_train.columns.tolist()        
    
    # 모델 생성
    RFM_model = RandomForestClassifier(random_state = 42)
    XGB_model = XGBClassifier(n_estimators = 500, random_state=42, learning_rate=0.03)
    MLP_model = MLPClassifier(hidden_layer_sizes=(10,), activation='logistic',
                              solver='sgd', alpha=0.01, batch_size=32,
                              learning_rate_init=0.1, max_iter=500)

    if mdl == 'RF':
        train_rfm_model = RFM_model.fit(x_train, y_train)
        train_model = train_rfm_model
    if mdl == 'XG':
        train_xgb_model = XGB_model.fit(x_train, y_train, eval_metric = 'logloss')
        train_model = train_xgb_model
    if mdl == 'DL':    
        train_mlp_model = MLP_model.fit(x_train, y_train)
        train_model = train_mlp_model
    return(train_model)

#################################################
# 모델 평가 함수
#################################################

def TestModel(
    test_data, 
    model, 
    trainColumns, 
    TestOrPred, 
    mdl, 
    ):
    """
        test_data      : 신규가입여부 존재
        champion_model : 학습 결과 모델
        listSelect     : 학습된 변수 리스트 및 변수 순서
    """
    listKeyCol  = ['기준년월', 'ci번호', '추천코드', '신규가입여부', '샘플링비율', '나이', '연령', '예금투자자산', '수신만기도래보유여부']    
    x_test = test_data.loc[:,~test_data.columns.isin(listKeyCol)].reset_index(drop = True)
    x_test  = x_test[trainColumns]
            
    if TestOrPred == 'Test':
        y_test  = test_data[['기준년월', 'ci번호', '신규가입여부']].reset_index(drop = True)
    elif TestOrPred == 'Pred':
        y_test  = test_data[['기준년월', 'ci번호']].reset_index(drop = True)

    y_test[f'score'] = model.predict_proba(x_test)[:,1]
    y_test['model'] = mdl

    y_test = y_test.rename(columns = {'신규가입여부' : 'tatyn', '예측확률' : 'prob'})
    return (y_test)

#################################################
# 모델 예측 함수
#################################################

def MLStep8ModelPred(
    idxRecCode,
    PredSet,
    Champion_columns,
    Champion_mdl,
    useMDL,
    df_segBestCutoff =pd.DataFrame()
    ):
    
    print(" 예측 시작 ")

    df_seg  = PredSet[['ci번호', '성별', 'lifestage', 'binningcat', 'n1개월만기도래거치식예금보유여부', 'n1개월만기도래적립식예금보유여부']]
    df_seg.loc[  (df_seg['n1개월만기도래거치식예금보유여부'] == 0)
                &(df_seg['n1개월만기도래적립식예금보유여부'] == 0), '수신만기도래보유여부'] = '만기도래X'   
    df_seg['수신만기도래보유여부'].fillna('만기도래O', inplace = True)    
    df_seg = df_seg.drop(['n1개월만기도래거치식예금보유여부', 'n1개월만기도래적립식예금보유여부'], axis = 1)

    df_seg.loc[df_seg['binningcat'] <= 4, 'seg2'] = '2000만원미만'
    df_seg.loc[df_seg['binningcat'] >  4, 'seg2'] = '2000만원이상'
    df_seg['seg1'] = df_seg['수신만기도래보유여부'].copy()

    if idxRecCode != '10':
        df_seg['seg1'] = '전체'

    df_seg_cal = df_seg[['ci번호', 'seg1', 'seg2']].copy()

    pred_rslt = TestModel(
        test_data     = PredSet,
        model         = Champion_mdl, 
        trainColumns  = Champion_columns, 
        TestOrPred    = 'Pred', 
        mdl           = useMDL, 
        )

    pred_rslt['score'] = pred_rslt['score'].astype(float)
    pred_rslt['score'] = round(pred_rslt['score'], 5)

    print("########## 예측 종료 ")
    pred_output = (pred_rslt.merge(df_seg_cal, on = 'ci번호', how = 'left'))
    pred_output['추천코드'] = idxRecCode
    pred_output = pred_output[['기준년월', '추천코드', 'ci번호', 'seg1', 'seg2', 'model', 'score']]
    pred_output = pred_output.rename(columns = {'score' : 'score값'})

    if df_segBestCutoff.empty:
        # 초기 개발된 모델 컷오프
        firstSegCutoff = pd.DataFrame({
              "추천코드"      : ['10'] * 12 + ['21'] * 6 + ['2200'] *6
            , "model"          : ['XG'] * 4 + ['RF'] * 4 + ['DL'] * 4 + (['XG'] * 2 + ['RF'] * 2 + ['DL'] * 2)*2
            , "seg1"          : ['만기도래O', '만기도래O', '만기도래X', '만기도래X'] * 3 + ['전체']*12
            , "seg2"          : ['2000만원미만', '2000만원이상'] * 12
            , "cutoff"    : [0.57, 0.69, 0.28, 0.40, 0.42, 0.53, 0.21, 0.36, 0.01, 0.01, 0.19, 0.27
                                 , 0.55, 0.47, 0.45, 0.39, 0.28, 0.25
                                , 0.77, 0.77, 0.70, 0.59, 0.20, 0.19]

        })
        pred_output =  pred_output.merge(firstSegCutoff, on = ['추천코드', 'model', 'seg1', 'seg2'], how = 'left')
    
    else:
        pred_output =  pred_output.merge(df_segBestCutoff[['추천코드', 'model', 'seg1', 'seg2', 'cutoff']], on = ['추천코드', 'model', 'seg1', 'seg2'], how = 'left')

    pred_output = pred_output.rename(columns = {'cutoff' : '추천컷오프', 'model' : '모델'})
    
    return (pred_output)

##################################################################################################
# 모델 학습 평가 예측 시작
##################################################################################################

idxMDL = 'RF'
for idxRecCode in [
                  '10'  
                , '21'
                , '2200'
                ]:
    
    # model dir 설정
    s3ChampionModelDir = f"project_code/Models/ML/{idxRecCode}/champion"


    # 모델 업로드 전 저장 공간
    modelDirectory   = f'/opt/ml/model/MLBatchTemp/{idxRecCode}'
    
    # 로컬 경로 create
    CreateDir(modelDirectory)

    # champion model Load
    model_object         = s3_resource.Object(bucket, f"{s3ChampionModelDir}/mdl_Rec{idxRecCode}Rat3.pkl")
    Champion_model       = joblib.load(BytesIO(model_object.get()['Body'].read()))
    columns_object       = s3_resource.Object(bucket, f"{s3ChampionModelDir}/columns_Rec{idxRecCode}Rat3.pkl")
    Champion_columns     = joblib.load(BytesIO(columns_object.get()['Body'].read()))
    
    # 컬럼명 소문자로 변경
    Champion_columns = [x.lower() for x in Champion_columns]
    
    # 챔피언 모델 리스트 중 RF, XG, DL 중 스크립트에 맞는 모델 진행
    #  최종 XGBoost 모델만 재학습 및 비교
#   if idxMDL == 'RF':
#       Champion_model = Champion_model[0]
#   elif idxMDL == 'XG':
#       Champion_model = Champion_model[1]
#   elif idxMDL == 'DL':
#       Champion_model = Champion_model[2]

    print('[LOG]' + "#" * 90)
    print("[LOG] 00 예측마트 및 Champion 모델 정보 LOAD")
    print('[LOG]' + "#" * 90)
    
    PredSet = sqlToPandasDF(conn, f"""select * from {workDbNm}.PredSet_{idxRecCode}""")
    

    #################################################
    # 학습월 충분히 존재
    #################################################
        
    if (len(listACCbaseYmd) <= 2) or (len(listACCbaseYmd2) <= 2):
        print('[LOG]' + "#" * 90)
        print("[LOG] 학습 및 평가 기준년월 부족 > 기존 모델로 예측")
        print('[LOG]' + "#" * 90)
        
        pred_rslt = MLStep8ModelPred(
            idxRecCode       = idxRecCode,
            PredSet          = PredSet,
            Champion_columns = Champion_columns,
            Champion_mdl     = Champion_model,
            useMDL           = idxMDL,
            df_segBestCutoff = pd.DataFrame(),   
            )
        
        # 재학습 안할시에 chamion 모델 
        joblib.dump(Champion_model, f"{modelDirectory}/mdl_Rec{idxRecCode}Rat3_{baseYm_bfr_1m}.pkl")
        emptyData = pd.DataFrame()
        emptyData.to_csv(f'{modelDirectory}/compare_eval.csv')
        
        
    elif (len(listACCbaseYmd) >= 3) and (len(listACCbaseYmd2) >= 3):
        print('[LOG]' + "#" * 90)
        print("[LOG] 01 학습 및 평가 기준년월 3개월 이상 타겟 추출")
        print('[LOG]' + "#" * 90)
        st_time = print_start_time()
        
        # s3에서 읽을 때
        TrainSet = sqlToPandasDF(conn, f"""select * from {workDbNm}.TrainSet_{idxRecCode}""")
        TestSet  = sqlToPandasDF(conn, f"""select * from {workDbNm}.TestSet_{idxRecCode}""")    

        TrainSet = TrainSet.loc[TrainSet['기준년월'] >  '202201']
        print_end_time(st_time)        
        
        if TrainSet.shape[0] == 0:
            print('[LOG]' + "#" * 90)
            print("[LOG] 정상 학습월 부족 > 기존 모델로 예측")
            print('[LOG]' + "#" * 90)

            pred_rslt = MLStep8ModelPred(
                idxRecCode       = idxRecCode,
                PredSet          = PredSet,
                Champion_columns = Champion_columns,
                Champion_mdl     = Champion_model,
                useMDL           = idxMDL,
                df_segBestCutoff = pd.DataFrame(),   
                )
            # 재학습 안할시에 chamion 모델 
            joblib.dump(Champion_model, f"{modelDirectory}/mdl_Rec{idxRecCode}Rat3_{baseYm_bfr_1m}.pkl")
            emptyData = pd.DataFrame()
            emptyData.to_csv(f'{modelDirectory}/compare_eval.csv')
            
        else:

            ##################################################################################################
            # Train & Test
            ##################################################################################################
            print('[LOG]' + "#" * 90)        
            print("[LOG] 재학습 프로세스 시작")
            print('[LOG]' + "#" * 90)        
            #################################################
            # Train Model
            #################################################
            print('[LOG]' + "#" * 45)        
            print("[LOG] 1. 재학습")
            
            st_time = print_start_time()        
            retrain_model = TrainData(train_data = TrainSet, mdl = idxMDL, listSelect = Champion_columns)
            
            # XG, RF, DNN 모델 로컬 저장
            joblib.dump(retrain_model, f"{modelDirectory}/mdl_Rec{idxRecCode}Rat3_{baseYm_bfr_1m}.pkl")
            
            #################################################
            # 재학습 프로세스 시작, seg1 : 만기도래구분(수신), seg2: 자산군 구분
            #################################################
            df_seg  = TestSet[['ci번호', '성별', 'lifestage', 'binningcat', '수신만기도래보유여부']]    
            df_seg.loc[df_seg['binningcat'] <= 4, 'seg2'] = '2000만원미만'
            df_seg.loc[df_seg['binningcat'] >  4, 'seg2'] = '2000만원이상'

            df_seg.loc[df_seg['수신만기도래보유여부'] == 1, 'seg1'] = '만기도래O'
            df_seg.loc[df_seg['수신만기도래보유여부'] == 0, 'seg1'] = '만기도래X'    

            if idxRecCode != '10':
                df_seg['seg1'] = '전체'
            df_seg_cal = df_seg[['ci번호', 'seg1', 'seg2']].copy()

            #################################################
            # Retrain(Stack) Score
            #################################################
            
            print('[LOG]' + "#" * 45)        
            print("[LOG] 2. 재학습 모델 평가")
            st_time = print_start_time()                
            retrain_rslt = TestModel(
                test_data    = TestSet, 
                model        = retrain_model, 
                trainColumns = Champion_columns, 
                TestOrPred   = 'Test', 
                mdl          = idxMDL, 
                )
            print_end_time(st_time)

            #################################################
            # Champion Score
            #################################################

            print('[LOG]' + "#" * 45)        
            print("[LOG] 3. 기존 Champion 모델 평가")
            
            champion_rslt  = TestModel(
                test_data    = TestSet, 
                model        = Champion_model, 
                trainColumns = Champion_columns, 
                TestOrPred   = 'Test', 
                mdl          = idxMDL, 
                )

            #################################################
            # Retrain Cutoff 탐색
            #################################################
            
            print('[LOG]' + "#" * 45)        
            print("[LOG] 4. 재학습 및 기존 모델 f1score, recall 비교 > f1score")
            
            # 평가셋
            retrain_eval = cfm_concat(retrain_rslt, [idxMDL], '1')
            retrain_eval = RankStatIndex(retrain_eval, 'f1score',  ['segTotal'])
            retrain_eval = RankStatIndex(retrain_eval, 'recall'  , ['segTotal'])    
            retrain_eval_f1max    = retrain_eval.loc[(retrain_eval['f1score_Rank'] <= 10)]
            retrain_eval_max      = retrain_eval_f1max[retrain_eval_f1max['recall_Rank'] == retrain_eval_f1max['recall_Rank'].min()]

            #################################################
            # Champion Cutoff 탐색
            #################################################

            champion_eval          = cfm_concat(champion_rslt, [idxMDL], '1')
            champion_eval          = RankStatIndex(champion_eval, 'f1score',  ['segTotal'])
            champion_eval          = RankStatIndex(champion_eval, 'recall'  , ['segTotal'])        
            champion_eval_f1max    = champion_eval.loc[(champion_eval['f1score_Rank'] <= 10)]
            champion_eval_max      = champion_eval_f1max[champion_eval_f1max['recall_Rank'] == champion_eval_f1max['recall_Rank'].min()]

            retrain_f1  = retrain_eval_max['f1score'].values[0]
            champion_f1 = champion_eval_max['f1score'].values[0]

            if retrain_f1 >= champion_f1:
                print('[LOG]' + "#" * 45)        
                print(f"[LOG] 5. 재학습 모델 -> 챔피언 모델 변경, 재학습({retrain_f1}) >= 챔피언({champion_f1})")

                test_rslt = retrain_rslt.copy()
                select_cutoff = retrain_eval_max['cutoff'].values[0]

                # 챔피언 모델 변경
                Champion_model = retrain_model
                 
            else:
                print('[LOG]' + "#" * 45)        
                print(f"[LOG] 5. 기존 챔피언 모델 챔피언 유지, 재학습({retrain_f1}) < 챔피언({champion_f1})")
                test_rslt = champion_rslt.copy()
                select_cutoff = champion_eval_max['cutoff'].values[0]  

            #################################################
            # Champion Cutoff 탐색
            #################################################
            print('[LOG]' + "#" * 45)        
            print(f"[LOG] 6. 선택된 챔피언 모델별 cutoff 조정")
            # 각 세그별 bestCutoff 계산
            test_seg_eval = cfm_concat(test_rslt, [idxMDL], '1', df_seg_cal)
            test_seg_eval = RankStatIndex(test_seg_eval, 'f1score',  ['seg1', 'seg2'])
            test_seg_eval = RankStatIndex(test_seg_eval, 'recall'  , ['seg1', 'seg2'])    
            test_seg_eval_f1max    = test_seg_eval.loc[(test_seg_eval['f1score_Rank'] <= 10)]

            # seg join
            test_output = test_rslt.merge(df_seg_cal, on = 'ci번호', how = 'left')

            seg_bestCutoff = pd.DataFrame()
            listSeg = test_seg_eval_f1max['seg_cal'].unique().tolist()
            for idx_seg in listSeg:
                temp         = test_seg_eval_f1max.loc[(test_seg_eval_f1max['seg_cal'] == idx_seg)]
                temp_segrslt = temp.loc[temp['recall_Rank']  == temp['recall_Rank'].min()]
                seg_bestCutoff = seg_bestCutoff.append(temp_segrslt)

            seg_bestCutoff['추천코드']       = idxRecCode
            test_output = test_output.merge(seg_bestCutoff[['seg1', 'seg2', 'cutoff', "추천코드"]]
                                            , on = ['seg1', 'seg2']
                                            , how = 'left')
            test_output = test_output.rename(columns = {"cutoff" : "추천컷오프"})
            
            #################################################
            # 모델 평가지표 결과 저장
            #################################################

            print('[LOG]' + "#" * 45)        
            print(f"[LOG] 7. 재학습 평가지표 저장 csv")
            
            retrainCutoff = retrain_eval_max['cutoff'].values[0]
            retrain_eval.loc[retrain_eval['cutoff'] == retrainCutoff, "비교컷오프"] = '1'
            retrain_eval['비교컷오프'] = retrain_eval['비교컷오프'].fillna('0')

            championCutoff = champion_eval_max['cutoff'].values[0]
            champion_eval.loc[champion_eval['cutoff'] == championCutoff, "비교컷오프"] = '1'
            champion_eval['비교컷오프'] = champion_eval['비교컷오프'].fillna('0')
            compare_eval = pd.concat([retrain_eval, champion_eval], axis = 0)
            
            
            compare_eval["기준년월"]       = baseYm_bfr_1m
            compare_eval["모델평가월"]     = baseYm_bfr_2m
            compare_eval["모델누적학습월"] = baseYm_bfr_3m
            compare_eval["누적학습구분"]   = 'Y누적학습'
            compare_eval['추천코드'] = idxRecCode
            compare_eval = compare_eval.rename(columns = {'model' : '모델'})
            
            compare_eval = SortColumns(compare_eval, ['기준년월', '추천코드', '모델', '누적학습구분'])
            compare_eval = compare_eval.drop(['segTotal', 'seg_cal'], axis = 1).rename(columns = {"model" : "모델"})
            
            # 로컬 저장
            compare_eval.to_csv(f'{modelDirectory}/compare_eval.csv')
            # s3에 저장할 때 
            
            #################################################
            # predict
            #################################################
            print('[LOG]' + "#" * 45)        
            print(f"[LOG] 7. 선택된 챔피언 모델로 예측마트 예측")

            pred_rslt = MLStep8ModelPred(
                idxRecCode       = idxRecCode,
                PredSet          = PredSet.fillna(0),
                Champion_columns = Champion_columns,
                Champion_mdl     = Champion_model,
                useMDL           = idxMDL,
                df_segBestCutoff = seg_bestCutoff,   
                )
            
    #################################################
    # 챔피언 모델 저장 (변경되든, 안되든)
    #################################################
    joblib.dump(Champion_model, f"{modelDirectory}/mdl_Rec{idxRecCode}Rat3.pkl")

    #################################################
    # 예측결과 Table로 저장
    #################################################    
    
    # 파티션컬럼 생성
    for idx in ['기준년월', '추천코드', '모델']:
        pred_rslt[f'p_{idx}'] = pred_rslt[idx].copy()
        
    # 아테나에 drop
    drop_athena_data(
          target = 'partition'
        , bucket = bucket
        , schema = workDbNm
        , tblNm  = 'TCTAHMC17'
        , p_col  = ['p_기준년월', 'p_추천코드', 'p_모델']
        , p_val  = [baseYm_bfr_1m, idxRecCode, idxMDL]
        , workGroup = workGrp  # pyathena 실행 workgroup(default: kl0-proj-0005)        
    )

    # 아테나에 적재    
    to_sql(
          pred_rslt
        , 'TCTAHMC17'
        , conn
        , f's3://{bucket}/athena/TCTAHMC17/'
        , if_exists   = 'append'
        , schema      = workDbNm
        , chunksize   = 10000
        , max_workers = 5
        , partitions  = ['p_기준년월', 'p_추천코드', 'p_모델']
    )

#################################################
# 로컬에 저장된 모델 > S3로
#################################################

for idxRecCode in [
      '10'
    , '21'
    , '2200'
    ]:
    print(f'[LOG] 로컬에 저장된 모델 정보 통합 추천코드 : {idxRecCode}')
    
    s3ChampionModelDir = f"project_code/Models/ML/{idxRecCode}/champion"
    s3modelDirectory   = f"project_code/Models/ML/{idxRecCode}/challenge"
    # 모델 업로드 전 저장 공간
    modelDirectory   = f'/opt/ml/model/MLBatchTemp/{idxRecCode}'

    print(f'[LOG] 추천코드 : {idxRecCode}, 모델 로컬 경로 : {modelDirectory}, 모델 S3 경로 : {s3modelDirectory}')    

    # s3에 저장
    S3_CLIENT.upload_file(Filename = f"{modelDirectory}/mdl_Rec{idxRecCode}Rat3.pkl"
                        , Bucket   = bucket
                        , Key      = f"{s3ChampionModelDir}/mdl_Rec{idxRecCode}Rat3_{idxMDL}.pkl")    

    S3_CLIENT.upload_file(Filename = f"{modelDirectory}/mdl_Rec{idxRecCode}Rat3_{baseYm_bfr_1m}.pkl"
                        , Bucket   = bucket
                        , Key      = f"{s3modelDirectory}/mdl_Rec{idxRecCode}Rat3_{idxMDL}_{baseYm_bfr_1m}.pkl")
                        
    S3_CLIENT.upload_file(Filename = f"{modelDirectory}/compare_eval.csv"
                        , Bucket   = bucket
                        , Key      = f"{s3modelDirectory}/모델평가지표_모니터링_{idxMDL}_{baseYm_bfr_1m}.csv")
