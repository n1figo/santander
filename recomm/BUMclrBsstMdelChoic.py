##########################################################################################################################
#    1.  Program Title  : BUMclrBsstMdelChoic.py
# ML 상품추천 최적모델 선정
#    3.  Source Data    : TCTAHMC17
#    4.  Target Data    : TCTAHMC18
#    5.  Create Date    : 2022/03/18
#    6.  Update History : 
##########################################################################################################################
######################################################################################################################################################
# Module Import
######################################################################################################################################################

# 기본모듈 Import
import pandas                 as pd
import numpy                  as np
import os, time
import warnings
import pytz
import boto3
import argparse
import subprocess
import sys
import calendar
import json
import joblib
from   datetime               import datetime, timedelta
from   dateutil.relativedelta import relativedelta
from io import BytesIO

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

# 재계산
baseYmd = DatetimeCalMonth(baseYmd, -1)
baseYm_bfr_1m = DatetimeCalMonth(baseYmd, 1)[:6]

################################################################################################
# XG Boost 선택
################################################################################################

BestModel = sqlToPandasDF(conn, f"""
select
    *
from {workDbNm}.TCTAHMC17
where 
    "p_기준년월" = '{baseYm_bfr_1m}'
and "p_모델"     = 'XG'
"""
)
BestModel["최종선택"] = '1'
BestModel  = BestModel[['기준년월', '추천코드', 'ci번호', 'seg1', 'seg2', '모델', 'score값', '추천컷오프', '최종선택', 'p_기준년월', 'p_추천코드', 'p_모델']]

for idxRecCode in  ['10', '21', '2200']:
    # 아테나에 drop
    drop_athena_data(
          target = 'partition'
        , bucket = bucket
        , schema = workDbNm
        , tblNm  = 'TCTAHMC18'
        , p_col  = ['p_기준년월', 'p_추천코드', 'p_모델']
        , p_val  = [baseYm_bfr_1m, idxRecCode, 'XG']
        , workGroup = workGrp  # pyathena 실행
    )

# 아테나에 적재
to_sql(
      BestModel
    , 'TCTAHMC18'
    , conn
    , f's3://{bucket}/athena/TCTAHMC18/'
    , if_exists   = 'append'
    , schema      = workDbNm
    , chunksize   = 10000
    , max_workers = 5
    , partitions  = ['p_기준년월', 'p_추천코드', 'p_모델']
)


################################################
# 모델 이동
################################################
# s3 에 저장
import tarfile

modelNames = ['10', '21', '2200']
modelKorNames = ['수신', '여신', '현금서비스']
for modelNamei, korNamei in zip(modelNames, modelKorNames):
    
    # SOURCE / TARGET PATH
    sourcePath      = f"project_code/Models/ML/{modelNamei}/champion"
    targetPath      = f"platform/model"  
    print(f"[LOG] Model 이동 {sourcePath}  >> {targetPath}")
    # FILE PATH
    columnsFilePath = f"columns_Rec{modelNamei}Rat3.pkl"
    modelFilePath   = f"mdl_Rec{modelNamei}Rat3.pkl"
    zipFilePath     = f"XGB_{korNamei}모델 및 사용Feature정보.tar.gz"

    # COMPRESS TO TAR.GZ
    with tarfile.open(f"./{zipFilePath}", 'w:gz') as tar:
        
        # PICKLE FILE DOWNLOAD
        S3_CLIENT.download_file(Bucket=bucket, Key=f"{sourcePath}/{columnsFilePath}", Filename=f"./{columnsFilePath}")
        tar.add(f"./{columnsFilePath}")
        
        S3_CLIENT.download_file(Bucket=bucket, Key=f"{sourcePath}/{modelFilePath}", Filename=f"./{modelFilePath}")
        tar.add(f"./{modelFilePath}")
    S3_CLIENT.upload_file(Filename=f"./{zipFilePath}", Bucket=bucket, Key=f"{targetPath}/{zipFilePath}")        
    

################################################
# TEMP 파일 삭제
################################################
for idxRecCode in ['10', '21', '2200']:
    # 아테나에 drop
    drop_athena_data(
          target = 'table'
        , bucket = bucket
        , schema = workDbNm
        , tblNm  = f"TrainSet_{idxRecCode}"
        , workGroup = workGrp  # pyathena 실행                
    )

    # 아테나에 drop
    drop_athena_data(
          target = 'table'
        , bucket = bucket
        , schema = workDbNm
        , tblNm  = f"TestSet_{idxRecCode}"
        , workGroup = workGrp  # pyathena 실행                
    )

    # 아테나에 drop
    drop_athena_data(
          target = 'table'
        , bucket = bucket
        , schema = workDbNm
        , tblNm  = f"PredSet_{idxRecCode}"
        , workGroup = workGrp  # pyathena 실행                
    )
