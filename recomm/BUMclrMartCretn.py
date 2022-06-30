##########################################################################################################################
#    1.  Program Title  : BUMclrMartCretn.py
# 상품추천 분석마트
#    3.  Source Data    : TCTAHMC01, TCTAHMC02,TCTAHMC03,TCTAHMC04,TCTAHMC05,TCTAHMC06,TCTAHMC07,TCTAHMC08,TCTAHMC11,TCTAHMC12, TCTAHMB04, TCTAHMC13, TCTAHMC14, 
#    4.  Target Data    : TCTAHMC14
#    5.  Create Date    : 2022/03/23

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
bucket      = 'tah-prd-kl0-s3-proj-0005'
# bucket      = 'tah-prd-kl0-s3-proj-0005'
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

########################
# 세그먼트 변수 seg_keys 할당
########################    

segment_mast = pd.read_sql(
f"""
select
      distinct "세그먼트코드"
    , "세그먼트명"
from {workDbNm}.TCTAHMB04
where "기준년월" > '202201'
order by 1
"""
, conn)

listExtractSeg = segment_mast.loc[
      ~(segment_mast['세그먼트명'].str.contains('기관'))
    & ~(segment_mast['세그먼트명'].isin(["카드_결제은행", "LifeStage", "연령구분", "성별구분"]))
    ]['세그먼트코드'].tolist()

seg_keys = []
for idxSegCode in listExtractSeg:
    idxSegName = segment_mast.loc[segment_mast['세그먼트코드'] == idxSegCode]['세그먼트명'].values[0]
    seg_keys.append((idxSegCode, idxSegName))   

########################
# TCTAHMC13 Mapping 확인 후 to_sql
########################    

print('[LOG]' + "#" * 90)
print("[LOG] TCTAHMC13 Mapping Load 후 to_sql")

TCTAHMC13 = pd.read_csv(s3_resource.Object(bucket, 'project_code/Mapping/TCTAHMC13.csv').get()['Body'], dtype = 'object')
listMC13yymm = TCTAHMC13['기준년월'].unique().tolist()

for idxBaseYm in listMC13yymm:
    for idxRecCode in ['10', '21', '2200']: 
        drop_athena_data(
              target = 'partition'
            , bucket = bucket
            , schema = workDbNm
            , tblNm  = 'TCTAHMC13'
            , p_col  = ['p_기준년월', 'p_추천코드']
            , p_val  = [idxBaseYm, idxRecCode]
            , workGroup = workGrp  # pyathena 실행                
        )

# 테이블 적재
to_sql(
      TCTAHMC13
    , 'TCTAHMC13'
    , conn
    , f's3://{bucket}/athena/TCTAHMC13/'
    , if_exists   = "append"
    , schema      = workDbNm
    , chunksize   = 10000
    , max_workers = 5
    , partitions  = ['p_기준년월', 'p_추천코드']
)

########################
# 타겟 생성 및 샘플링 학습마트 to sql
########################    

def MLStep1MakeTarget(
    batchdate, 
    idxlistRecCode,
    idxTgtTable,        
    MakeTableOption = 'y',
    idxRatio = 3,
    ):
    
    baseYm_bfr_1m = DatetimeCalMonth(batchdate, 1)[:6]
    baseYm_bfr_2m = DatetimeCalMonth(batchdate, 2)[:6]
    
    baseYm         = baseYm_bfr_1m[0:6]
    p_year         = baseYm_bfr_1m[0:4]
    p_month        = baseYm_bfr_1m[4:6]    

    print (f"신규입수 기준년월 {baseYm_bfr_1m}, 신규타겟 생성 기준년월 {baseYm_bfr_2m}")    

    # 고객 성별 잔액정보
    df_bfr_2m_CI = sqlToPandasDF(conn, f"""
    select
          b1."기준년월"
        , b1."ci번호"

        , es01."성별"
        , es01."lifestage"

        , bal."예금상품월말잔액"
        , bal."투자상품월말잔액"

        , coalesce(bal."예금상품월말잔액", 0)  + coalesce(bal."투자상품월말잔액", 0) as "예금투자자산"

        , mc03."n1개월만기도래거치식예금보유여부"
        , mc03."n1개월만기도래적립식예금보유여부"
        , mc03."수신만기도래보유여부"

    from (
      --전전달 존재
        select
              distinct "기준년월"
            , "ci번호"  
        from {srcDbNm[0:3]}_l2.TCTAHEA03
        where concat(year, month) = '{baseYm_bfr_2m}'
        and "계좌상태" = '1'
    ) as b1

    inner join(
      --전달 존재
        select
              distinct "ci번호"    
        from {srcDbNm[0:3]}_l2.TCTAHEA03
        where  1=1
        and concat(year, month) = '{baseYm_bfr_1m}'
        and "계좌상태" = '1'
    ) a1
    on b1."ci번호" = a1."ci번호"

    left join(
        select
              s01."ci번호"
            , case when "성별구분코드" = '1' then '1_남성'
                   when "성별구분코드" = '2' then '2_여성'
                   else                           '9_미분류'
                end as "성별"
            , case when {p_year} + 1 - "연령" <  32  and {p_year} + 1 - "연령" >= 19 then '0사회초년기'
                   when {p_year} + 1 - "연령" <  40  and {p_year} + 1 - "연령" >= 32 then '1가족형성기'
                   when {p_year} + 1 - "연령" <  50  and {p_year} + 1 - "연령" >= 40 then '2가족성장기'
                   when {p_year} + 1 - "연령" <  55  and {p_year} + 1 - "연령" >= 50 then '3은퇴예정기'
                   when {p_year} + 1 - "연령" <  65  and {p_year} + 1 - "연령" >= 55 then '4은퇴기'
                   when {p_year} + 1 - "연령" <  75  and {p_year} + 1 - "연령" >= 65 then '5연금수령기'
                   when {p_year} + 1 - "연령" <  120 and {p_year} + 1 - "연령" >= 75 then '6노년기'
                   else                                                                   '9분류불가'
              end as "lifestage"      
        from {srcDbNm}.TCTAHES01 as s01
    ) es01
    on b1."ci번호" = es01."ci번호"

    left join(
    select
          "ci번호"
        , "예금상품월말잔액"
        , "투자상품월말잔액"
    from {workDbNm}.TCTAHMC04
    where 1=1
    and "p_기준년월" = '{baseYm_bfr_2m}'
    ) as bal
    on b1."ci번호" = bal."ci번호"

    left join (
    select
          "ci번호"
        , "n1개월만기도래거치식예금보유여부"
        , "n1개월만기도래적립식예금보유여부"
        , case when "n1개월만기도래거치식예금보유여부" = '0' and 'n1개월만기도래거치식예금보유여부' = '0' then '1'
               else                                                                                           '0'
          end as "수신만기도래보유여부"
    from {workDbNm}.TCTAHMC03
    where  1=1
    and "p_기준년월" = '{baseYm_bfr_2m}'
    ) mc03
    on b1."ci번호" = mc03."ci번호"    
    """
    )

    # 고객 상품 타겟 정보
    df_tagt = sqlToPandasDF(conn, f"""
    select
          p1.*
        , mc03."n1개월만기도래거치식예금보유여부"
        , mc03."n1개월만기도래적립식예금보유여부"
        , mc03."수신만기도래보유여부"
    from (
        select
              distinct "ci번호"
            , case 
                when "그룹상품소분류코드" in ('1201')                                        then '1101'
                when "그룹상품소분류코드" in ('1202')                                        then '1102'
                when "그룹상품중분류코드" in ('21', '22', '23', '24', '25', '26', '27')      then '12'
                when "그룹상품소분류코드" in ('1203')                                        then '1300'

                when "그룹상품소분류코드" in ('6101', '6104')                                then '2101'

                when "그룹상품소분류코드" in ('6203', '6206', '6208', '6204')                 then '2102'
                when "그룹상품소분류코드" in ('6201', '6211', '6205', '6207', '6209', '6210') then '2103'
                when "그룹상품중분류코드" in ('63')                                           then '2104'

                when "그룹상품소분류코드" in ('6105', '6202')                                 then '2105'
                when "그룹상품소분류코드" = '6103'                                            then '2200'

                else                                                 '99' 
               end   as "추천대분류코드"
            , '1' as "신규가입여부"
        from {srcDbNm[0:3]}_l2.TCTAHEA03
        -- 전달 신규 가입 타겟 추출
        where concat(year, month) = '{baseYm_bfr_1m}'
        and   "기준년월" = substr("신규일자", 1, 6)        
        and   "계좌상태" = '1'
    ) p1

    -- 전전달에 ci존재
    inner join(
    select
          distinct "ci번호"    
    from {srcDbNm[0:3]}_l2.TCTAHEA03
    where  1=1
    and concat(year, month) = '{baseYm_bfr_2m}'
    ) a1
    on p1."ci번호" = a1."ci번호"

    left join (
    select
          "ci번호"
        , "n1개월만기도래거치식예금보유여부"
        , "n1개월만기도래적립식예금보유여부"
        , case when "n1개월만기도래거치식예금보유여부" = '0' and 'n1개월만기도래거치식예금보유여부' = '0' then '1'
               else                                                                                           '0'
          end as "수신만기도래보유여부"            
    from {workDbNm}.TCTAHMC03
    where  1=1
    and "p_기준년월" = '{baseYm_bfr_2m}'
    ) mc03
    on p1."ci번호" = mc03."ci번호"

    where p1."추천대분류코드" != '99'    
    """
    )

    ###################################
    # 투자의 경우 데이터 미존재 이슈
    ###################################    

    df_tagt10   = df_tagt[df_tagt['추천대분류코드'].isin(['1101', '1102', '12'])]
    df_tagt21   = df_tagt[df_tagt['추천대분류코드'].isin(['2101', '2102', '2103', '2104'])]

    df_tagt10['추천대분류코드'] = '10'
    df_tagt21['추천대분류코드'] = '21'

    df_tagt101 = df_tagt10.loc[df_tagt10['수신만기도래보유여부'] == '0']
    df_tagt102 = df_tagt10.loc[df_tagt10['수신만기도래보유여부'] == '1']
    df_tagt101['추천대분류코드'] = '101'
    df_tagt102['추천대분류코드'] = '102'    

    df_tagt = pd.concat([
                           df_tagt10 , df_tagt21,
                           df_tagt101, df_tagt102,
                           df_tagt,
    ], axis = 0).drop('수신만기도래보유여부', axis = 1).drop_duplicates()

    df_tagt = df_tagt.pivot_table(  index = 'ci번호'
                                      , columns = '추천대분류코드'
                                      , values = '신규가입여부'
                                      , aggfunc = lambda x: ''.join(x))
    listRecCode = df_tagt.columns.get_level_values(0).tolist()
    df_tagt   = df_tagt.reset_index()

    df_bfr_2m_CI = df_bfr_2m_CI.merge(df_tagt, on = 'ci번호', how = 'left')
    df_bfr_2m_CI[listRecCode] = df_bfr_2m_CI[listRecCode].fillna('0')

    listBaseCol = ['기준년월', 'ci번호', '성별', 'lifestage'
                   , '예금상품월말잔액', '투자상품월말잔액', '예금투자자산']

    ###################################
    # Binning  Using nextafter
    ###################################
    df_bfr_2m_CI = MakeBinning(df_bfr_2m_CI, '예금투자자산', 'binningcat', [100000, 1000000, 3000000, 7500000, 20000000, 50000000])
    df_bfr_2m_CI['binningcat'] = df_bfr_2m_CI['binningcat'].astype(str)
    # 성별 null 혹은 이상값인 경우 대상 제외
    df_bfr_2m_CI = df_bfr_2m_CI.loc[df_bfr_2m_CI['성별'].notnull()]

    # 추천코드 타겟 0보다 큰 경우만 추천코드 쌓음
    RecCode      = df_bfr_2m_CI[listRecCode].astype(int).sum(axis = 0).reset_index()
    RecCodeOver0 = RecCode.loc[RecCode[0] > 0]['index'].tolist()    

    if idxlistRecCode != None:
        RecCodeOver0 = idxlistRecCode.copy()

    # 샘플링 시작
    SamplingTable = pd.DataFrame()
    for idxRecCode in RecCodeOver0:

        if idxRecCode == '101':
            bfr_2m_CI_Inf = df_bfr_2m_CI.loc[df_bfr_2m_CI['수신만기도래보유여부'] == '0'].copy()
        elif idxRecCode == '102':
            bfr_2m_CI_Inf = df_bfr_2m_CI.loc[df_bfr_2m_CI['수신만기도래보유여부'] == '1'].copy()
        else:
            bfr_2m_CI_Inf = df_bfr_2m_CI.copy()
        # 해당 추천 코드 추출
        bfr_2m_CI_Inf = bfr_2m_CI_Inf[listBaseCol + ['binningcat', '수신만기도래보유여부'] + [idxRecCode]].rename(columns = {idxRecCode : '신규가입여부'})
        bfr_2m_CI_Inf['추천코드'] = idxRecCode

        # 타겟/NonTaget 분리
        tgtSelect       = bfr_2m_CI_Inf.loc[bfr_2m_CI_Inf['신규가입여부'] == '1'].reset_index(drop = True)
        nontgtSelect    = bfr_2m_CI_Inf.loc[bfr_2m_CI_Inf['신규가입여부'] == '0'].reset_index(drop = True)
        # Binning

        # 비율 별 계산
        tmp_Sampling = pd.DataFrame()

        # 층화추출 수 계산
        lenTarget       = len(tgtSelect)
        lenNonTarget    = len(nontgtSelect)
        print(f"추천코드 : {idxRecCode} 타겟수: {lenTarget} 목표 추출넌타겟수 {lenTarget * idxRatio} NonTaget Sampling Ratio {idxRatio} 비율 추출")

        idxNumb = lenNonTarget / (lenTarget * idxRatio)
        # 세그 생성
        nontgtSelect['seg1'] = nontgtSelect['lifestage'] + nontgtSelect['binningcat']

        # Samping Master 생성
        sample_mast = nontgtSelect['seg1'].value_counts().sort_index().reset_index()
        sample_mast['extract_cnt'] = np.around(sample_mast['seg1'] / idxNumb, 0).astype(int)
        sample_mast['extract_cnt'] = sample_mast['extract_cnt'].astype(int)
        sample_mast = sample_mast.rename(columns = {'seg1' : 'cnt', 'index' : 'seg1'})

        listSegValue = sample_mast['seg1'].tolist()
        listExtCnt   = sample_mast['extract_cnt'].tolist()

        # nontgt  층화추출 시작
        nontgtSample = pd.DataFrame()
        col_i = 0
        for idxValue, idxCnt in zip(listSegValue, listExtCnt):
            col_i += 1
            tmpSampling = (
                nontgtSelect[nontgtSelect['seg1'] == idxValue]
                          .reset_index(drop = True)
                          .sample(n = idxCnt, random_state = 1004)
            )
            nontgtSample = pd.concat([nontgtSample, tmpSampling], axis = 0)

        tmp_Sampling = pd.concat([tgtSelect, nontgtSample], axis = 0)
        tmp_Sampling['샘플링비율'] = idxRatio
        tmp_Sampling = tmp_Sampling[['기준년월', 'ci번호', '성별', 'lifestage', 'binningcat', '수신만기도래보유여부'
                                     , '예금투자자산', '추천코드', '신규가입여부', '샘플링비율']]
        # 파티션컬럼 생성
        for idx in ['기준년월', '추천코드', '샘플링비율']:
            tmp_Sampling[f'p_{idx}'] = tmp_Sampling[idx].copy()

        if MakeTableOption == 'y':
            # 아테나에 drop
            drop_athena_data(
                  target = 'partition'
                , bucket = bucket
                , schema = workDbNm
                , tblNm  = idxTgtTable
                , p_col  = ['p_기준년월', 'p_추천코드', 'p_샘플링비율']
                , p_val  = [baseYm_bfr_2m, idxRecCode, idxRatio]
                , workGroup = workGrp  # pyathena 실행                
            )

            tmp_Sampling['샘플링비율']   = tmp_Sampling['샘플링비율'].astype(float)
            tmp_Sampling['p_샘플링비율'] = tmp_Sampling['p_샘플링비율'].astype(float)

            # 아테나에 적재    
            to_sql(
                  tmp_Sampling
                , idxTgtTable
                , conn
                , f's3://{bucket}/athena/{idxTgtTable}/'
                , if_exists   = 'append'
                , schema      = workDbNm
                , chunksize   = 10000
                , max_workers = 5
                , partitions  = ['p_기준년월', 'p_추천코드', 'p_샘플링비율']
            )            


#################################################
# 세그먼트 변수 생성 함수
#################################################    

def GenSegPvtQuery(batchdate, seg_keys, set_cat, idxTgtTable, idxRecCode):
    g_start_time = datetime.now()
    print(f'[ {datetime.strftime(g_start_time, "%Y-%m-%d %H:%M:%S")} ] 작업 시작시간')

    baseYm         = batchdate[0:6]
    baseYm_bfr_1m = DatetimeCalMonth(batchdate, 1)[:6]
    baseYm_bfr_2m = DatetimeCalMonth(batchdate, 2)[:6]
    baseYm_bfr_3m = DatetimeCalMonth(batchdate, 3)[:6]

    trgtSegs = ''
    conditionColumns = ''
    for seg_key in seg_keys:
        tmp_conditionColumns = f"""coalesce(max(case when "p_세그먼트코드" = '{seg_key[0]}' then "세그먼트속성코드" end), '99') as "{seg_key[1]}" """
        conditionColumns     = f"""{ conditionColumns if len(conditionColumns) > 0 else '' }
            , {tmp_conditionColumns}"""

    # where conditions
    trgtSegs = f"""'{"','".join([x[0] for x in seg_keys])}'"""

    if set_cat == 'trainSet':
        conditionBaseYm = f"""and "p_기준년월" <= '{baseYm_bfr_3m}'"""
    elif set_cat == 'testSet':
        conditionBaseYm = f"""and "p_기준년월" = '{baseYm_bfr_2m}'"""
    elif set_cat == 'predSet':
        conditionBaseYm = f"""and "p_기준년월" = '{baseYm_bfr_1m}'"""

    if set_cat == 'trainSet':    
        sqlTCTAHMC = f"""
        select 
            b.*
        from (
            select
                  "기준년월"
                , "ci번호"
            from {workDbNm}.{idxTgtTable}
            where 1=1
            {conditionBaseYm}
            and "p_추천코드"   = '{idxRecCode}'
            and "p_샘플링비율" = 3
        ) as a
        left join(
            select
                  "기준년월"
                , "ci번호"  
                {conditionColumns} 
            from {workDbNm}.TCTAHMB04
            where 1=1
                {conditionBaseYm}
                and "p_세그먼트코드" in ({trgtSegs})
            group by "기준년월", "ci번호"
        ) as b
        on  a."기준년월" = b."기준년월"
        and a."ci번호"   = b."ci번호"
        """
    else :
        sqlTCTAHMC = f"""
        select
              "기준년월"
            , "ci번호"
            {conditionColumns} 
        from {workDbNm}.TCTAHMB04
        where 1=1
            --and "p_기준년월" = '{baseYm_bfr_3m}'
            {conditionBaseYm}
            and "p_세그먼트코드" in ({trgtSegs})
        group by "기준년월", "ci번호"
        """

    df_seg = sqlToPandasDF(conn, sqlTCTAHMC)

    df_seg = df_seg.set_index(['기준년월', 'ci번호']).fillna(0).astype(int).replace(99, 0)
    df_seg.columns =  'SEG_' + df_seg.columns
    df_seg = df_seg.reset_index()    

    g_end_time = datetime.now()
    g_el_time = g_end_time - g_start_time
    print(f'[ {datetime.strftime(g_end_time, "%Y-%m-%d %H:%M:%S")} ] 작업 종료, 소요시간: {str(g_el_time).split(".")[0]}')
    return df_seg

#################################################
# 모델 학습 마트 생성
#################################################

def MLStep2TrainMartCretn(
    batchdate,
    idxRecCode,
    idxRatio,
    idxTgtTable,
    idxVarTable,
    select_var_yn='y',
    ):

    f"""
    batchdate     : 배치일
    idxRecCode    : 타겟 코드 (10, 21, 2200)
    idxRatio      : 샘플링비율
    idxTgtTable   : 타겟 및 넌타겟샘플링 테이블
    idxVarTable   : 활용변수 테이블
    select_var_yn : "y" >> 전체 변수 활용 "n" >> 선택 변수만 활용
    """

    g_start_time = datetime.now()
    print(f'작업 시작시간: {datetime.strftime(g_start_time, "%Y-%m-%d %H:%M:%S")}')

    baseYm         = batchdate[0:6]
    baseYm_bfr_1m = DatetimeCalMonth(batchdate, 1)[:6]
    baseYm_bfr_2m = DatetimeCalMonth(batchdate, 2)[:6]
    baseYm_bfr_3m = DatetimeCalMonth(batchdate, 3)[:6]

    print(f"모델 학습 기간 : 타겟 추출 >> 추천코드 {idxRecCode}  샘플링비율: {idxRatio} / {datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}")

    # 활용변수명 추출 (각 모델마다 활용변수 다름)
    TCTAHMC13 = pd.read_sql(f"""
    select * 
    from (
        select 
              *
            , row_number() over(
                partition by "테이블명", "활용변수명", "추천코드", "선택유무"
                order by "기준년월" desc
              )                                      as seq
        from {workDbNm}.TCTAHMC13
    )
    where seq = 1
    and "추천코드" = '{idxRecCode}'
    and substr("테이블명", 1, 7) = 'TCTAHMC'
    """, conn)
    TCTAHMC13 = TCTAHMC13.drop('seq', axis = 1)


    print (f"활용변수 테이블 selection / {datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}")  

    listTCTAHMC = ['TCTAHMC01', 'TCTAHMC02', 'TCTAHMC03', 'TCTAHMC04', 'TCTAHMC05', 'TCTAHMC06', 'TCTAHMC07', 'TCTAHMC08', 'TCTAHMC12']
    conditionColumns = ''
    conditionTables  = ''
    for idxTable in listTCTAHMC:

        listKey   = ['기준년월', 'ci번호', '성별', 'lifestage', 'binningcat', '추천코드', '신규가입여부', '샘플링비율']

        # 변수 선택된 컬럼만 사용할지 y
        if select_var_yn == 'y':
            listTableColumns = TCTAHMC13.loc[(TCTAHMC13['테이블명'] == idxTable) & (TCTAHMC13['선택유무'] == '1')]['활용변수명'].tolist()
        # 모든 컬럼 사용할지
        elif select_var_yn == 'n':
            listTableColumns = TCTAHMC13.loc[(TCTAHMC13['테이블명'] == idxTable)]['활용변수명'].tolist()

        if len(listTableColumns) > 0:
            listTableColumns2 = [ x for x in listTableColumns if x not in listKey]

            tmp_conditionColumns = ', '.join([ str(f'{idxTable}."{x}"') for x in listTableColumns2])
            conditionColumns     = f"""{ conditionColumns if len(conditionColumns) > 0 else '' }
            , {tmp_conditionColumns}"""

            conditionTables  = f""" {conditionTables}
            inner join {workDbNm}.{idxTable}
            on
                t."기준년월" = {idxTable}."p_기준년월"
            and t."ci번호"   = {idxTable}."ci번호"
            """

    df_tgt = sqlToPandasDF(conn, f"""
        select
              t."기준년월"
            , t."ci번호"
            , t."성별"
            , t."lifestage"
            , t."binningcat"
            , t."수신만기도래보유여부"
            , t."신규가입여부"
            , t."예금투자자산"
            {conditionColumns}
        from {workDbNm}.{idxTgtTable} as t
        {conditionTables}
        where 1=1
            and t."p_기준년월"   <= '{baseYm_bfr_3m}'
            and t."p_추천코드"      =  '{idxRecCode}'
            and t."p_샘플링비율"    =   {idxRatio}
    """)

    # 성별 널값 Check
    df_tgt['성별']      = df_tgt['성별'].fillna('9')
    df_tgt['lifestage'] = df_tgt['lifestage'].fillna('9')

    for idx in ['성별', 'lifestage', 'binningcat']:
        df_tgt[idx] = df_tgt[idx].str[0].astype(int)

    print(f"컬럼수: {df_tgt.shape}")
    g_end_time = datetime.now()
    print(f'{batchdate} 작업 종료시간: {datetime.strftime(g_end_time, "%Y-%m-%d %H:%M:%S")}')
    g_el_time = g_end_time - g_start_time
    print(f'{batchdate} 작업 소요시간: {str(g_el_time).split(".")[0]}')

    return (df_tgt)

#################################################
# 모델 마트 생성
#################################################

def MLStep3TestMartCretn(
    batchdate, 
    idxRecCode,
    idxRatio,
    idxTgtTable,
    idxVarTable,
    select_var_yn,
    ):

    baseYm         = batchdate[0:6]
    baseYm_bfr_1m = DatetimeCalMonth(batchdate, 1)[:6]
    baseYm_bfr_2m = DatetimeCalMonth(batchdate, 2)[:6]

    print (f"신규입수 기준년월 {baseYm_bfr_1m}, 신규타겟 생성 기준년월 {baseYm_bfr_2m}")

    # 활용변수명 추출 (각 모델마다 활용변수 다름)
    TCTAHMC13 = pd.read_sql(f"""
    select * 
    from (
        select 
              *
            , row_number() over(
                partition by "테이블명", "활용변수명", "추천코드", "선택유무"
                order by "기준년월" desc
              )                                      as seq
        from {workDbNm}.TCTAHMC13
    )
    where seq = 1
    and "추천코드" = '{idxRecCode}'
    and substr("테이블명", 1, 7) = 'TCTAHMC'
    """, conn)
    TCTAHMC13 = TCTAHMC13.drop('seq', axis = 1)

    # 개인화 마트 Join
    listTCTAHMC = ['TCTAHMC01', 'TCTAHMC02', 'TCTAHMC03', 'TCTAHMC04', 'TCTAHMC05', 'TCTAHMC06', 'TCTAHMC07', 'TCTAHMC08', 'TCTAHMC12']

    conditionColumns = ''
    conditionTables  = ''
    for idxTable in listTCTAHMC:

        listKey2 =  ['기준년월', 'ci번호', '성별', 'lifestage']

        # 변수 선택된 컬럼만 사용할지 y
        if select_var_yn == 'y':
            listTableColumns = TCTAHMC13.loc[(TCTAHMC13['테이블명'] == idxTable) & (TCTAHMC13['선택유무'] == '1')]['활용변수명'].tolist()
        # 모든 컬럼 사용할지
        elif select_var_yn == 'n':
            listTableColumns = TCTAHMC13.loc[(TCTAHMC13['테이블명'] == idxTable)]['활용변수명'].tolist()

        if len(listTableColumns) > 0:
            listTableColumns2 = [ x for x in listTableColumns if x not in listKey2]

            tmp_conditionColumns = ', '.join([ str(f'{idxTable}."{x}"') for x in listTableColumns2])
            conditionColumns     = f"""{ conditionColumns if len(conditionColumns) > 0 else '' }
            , {tmp_conditionColumns}"""

            conditionTables  = f""" {conditionTables}
            inner join {workDbNm}.{idxTable}
            on
                b1."기준년월" = {idxTable}."p_기준년월"
            and b1."ci번호"   = {idxTable}."ci번호"
            """

    df_test_gen = sqlToPandasDF(conn, f"""
    select
          b1."기준년월"
        , b1."ci번호"

        , es01."성별"
        , es01."lifestage"

        , coalesce(TCTAHMC04."예금상품월말잔액", 0)  + coalesce(TCTAHMC04."투자상품월말잔액", 0) as "예금투자자산"

        {conditionColumns}
    from (
        select
              distinct "기준년월"
            , "ci번호"  
        from {srcDbNm[0:3]}_l2.TCTAHEA03
        where concat(year, month) = '{baseYm_bfr_2m}'
    ) as b1

    inner join(
      --전달 존재
        select
              distinct "ci번호"    
        from {srcDbNm[0:3]}_l2.TCTAHEA03
        where  1=1
        and concat(year, month) = '{baseYm_bfr_1m}'
        and "계좌상태" = '1'
    ) a1
    on b1."ci번호" = a1."ci번호"

    left join(
        select
              s01."ci번호"
            , case when "성별구분코드" = '1' then '1_남성'
                   when "성별구분코드" = '2' then '2_여성'
                   else                           '9_미분류'
                end as "성별"
            , case when {p_year} + 1 - "연령" <  32  and {p_year} + 1 - "연령" >= 19 then '0사회초년기'
                   when {p_year} + 1 - "연령" <  40  and {p_year} + 1 - "연령" >= 32 then '1가족형성기'
                   when {p_year} + 1 - "연령" <  50  and {p_year} + 1 - "연령" >= 40 then '2가족성장기'
                   when {p_year} + 1 - "연령" <  55  and {p_year} + 1 - "연령" >= 50 then '3은퇴예정기'
                   when {p_year} + 1 - "연령" <  65  and {p_year} + 1 - "연령" >= 55 then '4은퇴기'
                   when {p_year} + 1 - "연령" <  75  and {p_year} + 1 - "연령" >= 65 then '5연금수령기'
                   when {p_year} + 1 - "연령" <  120 and {p_year} + 1 - "연령" >= 75 then '6노년기'
                   else                                                                   '9분류불가'
              end as "lifestage"      
        from {srcDbNm}.TCTAHES01 as s01
    ) es01
    on b1."ci번호" = es01."ci번호"

    {conditionTables}
    """)

    df_test_gen.loc[ (df_test_gen['n1개월만기도래거치식예금보유여부'] == '0')
                &(df_test_gen['n1개월만기도래적립식예금보유여부'] == '0'), '수신만기도래보유여부'] = '0'
    df_test_gen['수신만기도래보유여부'].fillna('1', inplace = True)

    # test set / deploy set
    listTrainMonth = sorted(df_test_gen['기준년월'].unique().tolist(), reverse = True)
    print(f"모델 평가 기준년월 : {listTrainMonth}")
    # 성별 널값 Check
    df_test_gen['성별']      = df_test_gen['성별'].fillna('9')
    df_test_gen['lifestage'] = df_test_gen['lifestage'].fillna('9')

    # 예금투자 자산 비닝 실시
    df_test_gen = MakeBinning(df_test_gen, '예금투자자산', 'binningcat', [100000, 1000000, 3000000, 7500000, 20000000, 50000000])
    df_test_gen['binningcat'] = df_test_gen['binningcat'].astype(str)
    # 정렬
    sortVar = ['기준년월', 'ci번호', '성별', 'lifestage', 'binningcat', '수신만기도래보유여부','예금투자자산']
    df_test_gen = SortColumns(df_test_gen, sortVar)

    for idx in ['성별', 'lifestage', 'binningcat']:
        df_test_gen[idx] = df_test_gen[idx].str[0].astype(int)

    df_test   = df_test_gen.reset_index(drop = True)
    dfTestTgt = pd.read_sql(
    f"""
    select
          "ci번호"
        , "신규가입여부"
    from {workDbNm}.{idxTgtTable} as t
    where 1=1
        and t."p_기준년월"   = '{baseYm_bfr_2m}'
        and t."p_추천코드"   = '{idxRecCode}'
        and t."p_샘플링비율" =  {idxRatio}
    """, conn)
    listTestTgt = dfTestTgt.loc[dfTestTgt['신규가입여부'] == '1']['ci번호'].tolist()

    df_test.loc[df_test['ci번호'].isin(listTestTgt), "신규가입여부"] = '1'
    df_test['신규가입여부'].fillna('0', inplace = True)    
    sortVar = ['기준년월', 'ci번호', '성별', 'lifestage', 'binningcat', '수신만기도래보유여부', '신규가입여부','예금투자자산']
    df_test = SortColumns(df_test, sortVar)
    return (df_test)    

#################################################
# 모델 마트 생성
# test, deploy
#################################################

def MLStep4PredMartCretn(
    batchdate, 
    idxRecCode,
    idxRatio,
    idxVarTable,
    select_var_yn,
    ):

    baseYm         = batchdate[0:6]
    baseYm_bfr_1m = DatetimeCalMonth(batchdate, 1)[:6]
    baseYm_bfr_2m = DatetimeCalMonth(batchdate, 2)[:6]

    print (f"신규입수 기준년월 {baseYm_bfr_1m}, 신규타겟 생성 기준년월 {baseYm_bfr_2m}")

    # 활용변수명 추출 (각 모델마다 활용변수 다름)
    TCTAHMC13 = pd.read_sql(f"""
    select * 
    from (
        select 
              *
            , row_number() over(
                partition by "테이블명", "활용변수명", "추천코드", "선택유무"
                order by "기준년월" desc
              )                                      as seq
        from {workDbNm}.TCTAHMC13
    )
    where seq = 1
    and "추천코드" = '{idxRecCode}'
    and substr("테이블명", 1, 7) = 'TCTAHMC'
    """, conn)
    TCTAHMC13 = TCTAHMC13.drop('seq', axis = 1)


    # 개인화 마트 Join
    listTCTAHMC = ['TCTAHMC01', 'TCTAHMC02', 'TCTAHMC03', 'TCTAHMC04', 'TCTAHMC05', 'TCTAHMC06', 'TCTAHMC07', 'TCTAHMC08', 'TCTAHMC12']

    conditionColumns = ''
    conditionTables  = ''
    for idxTable in listTCTAHMC:

        listKey2 =  ['기준년월', 'ci번호', '성별', 'lifestage']

        # 변수 선택된 컬럼만 사용할지 y
        if select_var_yn == 'y':
            listTableColumns = TCTAHMC13.loc[(TCTAHMC13['테이블명'] == idxTable) & (TCTAHMC13['선택유무'] == '1')]['활용변수명'].tolist()
        # 모든 컬럼 사용할지
        elif select_var_yn == 'n':
            listTableColumns = TCTAHMC13.loc[(TCTAHMC13['테이블명'] == idxTable)]['활용변수명'].tolist()

        if len(listTableColumns) > 0:
            listTableColumns2 = [ x for x in listTableColumns if x not in listKey2]

            tmp_conditionColumns = ', '.join([ str(f'{idxTable}."{x}"') for x in listTableColumns2])
            conditionColumns     = f"""{ conditionColumns if len(conditionColumns) > 0 else '' }
            , {tmp_conditionColumns}"""

            conditionTables  = f""" {conditionTables}
            inner join {workDbNm}.{idxTable}
            on
                b1."기준년월" = {idxTable}."p_기준년월"
            and b1."ci번호"   = {idxTable}."ci번호"
            """

    df_pred_gen = sqlToPandasDF(conn, f"""
    select
          b1."기준년월"
        , b1."ci번호"

        , es01."성별"
        , es01."lifestage"

        , coalesce(TCTAHMC04."예금상품월말잔액", 0)  + coalesce(TCTAHMC04."투자상품월말잔액", 0) as "예금투자자산"

        {conditionColumns}
    from (
        select
              distinct "기준년월"
            , "ci번호"  
        from {srcDbNm[0:3]}_l2.TCTAHEA03
        where concat(year, month) = '{baseYm_bfr_1m}'
    ) as b1

    left join(
        select
              s01."ci번호"
            , case when "성별구분코드" = '1' then '1_남성'
                   when "성별구분코드" = '2' then '2_여성'
                   else                           '9_미분류'
                end as "성별"
            , case when {p_year} + 1 - "연령" <  32  and {p_year} + 1 - "연령" >= 19 then '0사회초년기'
                   when {p_year} + 1 - "연령" <  40  and {p_year} + 1 - "연령" >= 32 then '1가족형성기'
                   when {p_year} + 1 - "연령" <  50  and {p_year} + 1 - "연령" >= 40 then '2가족성장기'
                   when {p_year} + 1 - "연령" <  55  and {p_year} + 1 - "연령" >= 50 then '3은퇴예정기'
                   when {p_year} + 1 - "연령" <  65  and {p_year} + 1 - "연령" >= 55 then '4은퇴기'
                   when {p_year} + 1 - "연령" <  75  and {p_year} + 1 - "연령" >= 65 then '5연금수령기'
                   when {p_year} + 1 - "연령" <  120 and {p_year} + 1 - "연령" >= 75 then '6노년기'
                   else                                                                   '9분류불가'
              end as "lifestage"      
        from {srcDbNm}.TCTAHES01 as s01
    ) es01
    on b1."ci번호" = es01."ci번호"

    {conditionTables}
    """)

    print(f"컬럼수: {df_pred_gen.shape}")

    # test set / deploy set
    listTrainMonth = sorted(df_pred_gen['기준년월'].unique().tolist(), reverse = True)
    print(f"모델 평가 기준년월 : {listTrainMonth}")

    # 성별 널값 Check
    df_pred_gen['성별']      = df_pred_gen['성별'].fillna('9')
    df_pred_gen['lifestage'] = df_pred_gen['lifestage'].fillna('9')

    # 예금투자 자산 비닝 실시
    df_pred_gen = MakeBinning(df_pred_gen, '예금투자자산', 'binningcat', [100000, 1000000, 3000000, 7500000, 20000000, 50000000])
    df_pred_gen['binningcat'] = df_pred_gen['binningcat'].astype(str)
    # 정렬
    sortVar = ['기준년월', 'ci번호', '성별', 'lifestage', 'binningcat','예금투자자산']
    df_pred_gen = SortColumns(df_pred_gen, sortVar)

    for idx in ['성별', 'lifestage', 'binningcat']:
        df_pred_gen[idx] = df_pred_gen[idx].str[0].astype(int)

    df_pred   = df_pred_gen.reset_index(drop = True)

    sortVar = ['기준년월', 'ci번호', '성별', 'lifestage', 'binningcat', '예금투자자산']
    df_pred = SortColumns(df_pred, sortVar)
    return (df_pred)


###################################
# 전처리
###################################

def PreProcess(raw_data):
    returnData = raw_data.copy()
    listKeyCol  = ['기준년월', 'ci번호', '신규가입여부']

    # object to Int
    listobject  = returnData.columns[returnData.dtypes == 'object'].tolist()
    listobject2 = [x for x in listobject if x not  in listKeyCol]
    returnData[listobject2] = returnData[listobject2].fillna(0).astype(int)

    # int na 0
    listInt  = returnData.columns[returnData.dtypes == 'int'].tolist()
    returnData[listInt] = returnData[listInt].fillna(0).astype(int)

    # Na to 0
    returnData = returnData.set_index(['기준년월', 'ci번호']).fillna(0).reset_index()

    return (returnData)


#################################################
# 계좌마트 및 개인화 마트 기준년월 Check
#################################################

baseYm_bfr_1m = DatetimeCalMonth(baseYmd, 1)[:6]
baseYm_bfr_2m = DatetimeCalMonth(baseYmd, 2)[:6]
baseYm_bfr_3m = DatetimeCalMonth(baseYmd, 3)[:6]

df_baseYmd = pd.read_sql(
f"""
select distinct "기준년월" from {srcDbNm[0:3]}_l2.TCTAHEA03
where concat(year, month) in ('{baseYm_bfr_1m}', '{baseYm_bfr_2m}')
order by 1
""", conn)
listACCbaseYmd = df_baseYmd['기준년월'].tolist()

df_baseYmd2 = pd.read_sql(
f"""
select distinct "기준년월" from {workDbNm}.TCTAHMC01
where "p_기준년월" in ('{baseYm_bfr_1m}', '{baseYm_bfr_2m}')
order by 1
"""
, conn)
listACCbaseYmd2 = df_baseYmd2['기준년월'].tolist()

######################################################################
# 1. 타겟 생성, 훈련셋, 평가셋 생성
######################################################################

if (len(listACCbaseYmd) == 2) and  (len(listACCbaseYmd2) == 2):

    print('[LOG]' + "#" * 90)
    print("[LOG] 계좌마트, 개인화 마트 연속성 존재")
    
    tagtTableCreate = MLStep1MakeTarget(
            batchdate = baseYmd, 
            idxlistRecCode = ['10', '21', '2200'],
            idxTgtTable = 'TCTAHMC14',
            MakeTableOption = 'y',
            idxRatio = 3,
            )
else :
    print('[LOG]' + "#" * 90)
    print("[LOG] 계좌마트, 개인화 마트 연속성 미존재 > 타겟 생성 X")
print('[LOG]' + "#" * 90)



######################################################################
# 1. 훈련 마트 생성, 훈련셋, 평가셋 생성 기준년월 3개월 이상 존재 시 재학습을 위한 학습, 훈련 마트 및 예측 마트 생성
######################################################################

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



if (len(listACCbaseYmd) >= 3) and  (len(listACCbaseYmd2) >= 3):

    print('[LOG]' + "#" * 90)
    print("[LOG] 학습 및 평가 기준년월 3개월 이상 타겟 추출")
    print('[LOG]' + "#" * 90)

    # test_Seg : 공통
    test_Seg  = GenSegPvtQuery(batchdate = baseYmd, seg_keys = seg_keys , set_cat = 'testSet' , idxTgtTable = 'TCTAHMC14', idxRecCode = '10')
    pred_Seg  = GenSegPvtQuery(batchdate  = baseYmd, seg_keys = seg_keys, set_cat = 'predSet', idxTgtTable = 'TCTAHMC14', idxRecCode = '10')
    for idxRecCode in [
            '10',
            '21',
            '2200',
        ]:
        
        print('[LOG]' + "#" * 90)
        print(f"[LOG] {idxRecCode} 학습마트 생성")        

        TrainSet = MLStep2TrainMartCretn(
            batchdate = baseYmd,
            idxRecCode = idxRecCode,
            idxRatio = 3,
            idxTgtTable = 'TCTAHMC14',
            idxVarTable = 'TCTAHMC13',
            select_var_yn='y',
        )
        print(f"[LOG] {idxRecCode} 평가마트 생성")        
        TestSet = MLStep3TestMartCretn(
            batchdate     = baseYmd, 
            idxRecCode    = idxRecCode,
            idxRatio      = 3,
            idxTgtTable   = 'TCTAHMC14',
            idxVarTable   = 'TCTAHMC13',
            select_var_yn = 'y',
            )
        print(f"[LOG] {idxRecCode} 예측마트 생성")
        PredSet = MLStep4PredMartCretn(
            batchdate     = baseYmd,
            idxRecCode    = idxRecCode,
            idxRatio      = 3,
            idxVarTable   = 'TCTAHMC13',
            select_var_yn = 'y',
            )

        # 전처리            
        TrainSet  = PreProcess(TrainSet)
        TestSet   = PreProcess(TestSet)
        # 전처리
        PredSet  = PreProcess(PredSet)    

        # 훈련셋 세그먼트 변수 추출
        train_Seg = GenSegPvtQuery(batchdate = baseYmd, seg_keys = seg_keys, set_cat = 'trainSet', idxTgtTable = 'TCTAHMC14', idxRecCode = idxRecCode)

        # 활용 변수 및 Seg Load
        TCTAHMC13 = pd.read_sql(f"""
        select * 
        from (
            select 
                  *
                , row_number() over(
                    partition by "테이블명", "활용변수명", "추천코드", "선택유무"
                    order by "기준년월" desc
                  )                                      as seq
            from {workDbNm}.TCTAHMC13
        )
        where seq = 1
        and "추천코드" = '{idxRecCode}'
        and "선택유무" = '1'
        """, conn)
        TCTAHMC13 = TCTAHMC13.drop('seq', axis = 1)

        ########################
        # 선택 변수 
        ########################

        tmp_TCTAHMC13 = TCTAHMC13.loc[(TCTAHMC13['추천코드'] == idxRecCode)
                                     &(TCTAHMC13['테이블명'].str[:7] == 'TCTAHMC')
                                     ]
        ftSelect =  ['lifestage', 'binningcat'] + tmp_TCTAHMC13['활용변수명'].tolist()            
        listSelectSeg = TCTAHMC13.loc[(TCTAHMC13['추천코드'] == idxRecCode)
                                     &(TCTAHMC13['테이블명'].str[:7] == 'TCTAHMB')
                                     ]['활용변수명'].tolist()

        # 세그먼트명 변경
        listSelectSeg = ["SEG_" + x for x in listSelectSeg]
        ftSelect = ftSelect + listSelectSeg        

        # 생성안된 세그먼트 생성
        listSegCheck = [x for x in train_Seg.columns.tolist() if x not in ['기준년월', 'ci번호']]
        listSegNull = list(set(listSelectSeg) - set(listSegCheck))
        df_temp = train_Seg[['ci번호']]
        
        for idx in listSegNull:
            df_temp[idx] = 0
        train_Seg2 = train_Seg.merge(df_temp, on = 'ci번호', how = 'left')
        train_Seg2 = train_Seg2[['기준년월', 'ci번호'] + listSelectSeg]
        # 세그먼트 추출 및 조인
        TrainSet = TrainSet.merge(train_Seg2, on = ['기준년월', 'ci번호'], how = 'left')

        # 생성안된 세그먼트 생성
        listSegCheck = [x for x in test_Seg.columns.tolist() if x not in ['기준년월', 'ci번호']]
        listSegNull = list(set(listSelectSeg) - set(listSegCheck))
        df_temp = test_Seg[['ci번호']]
        
        for idx in listSegNull:
            df_temp[idx] = 0
        test_Seg2 = test_Seg.merge(df_temp, on = 'ci번호', how = 'left')
        test_Seg2 = test_Seg2[['기준년월', 'ci번호'] + listSelectSeg]
        # 세그먼트 추출 및 조인        
        TestSet  = TestSet.merge(test_Seg2  , on = ['기준년월', 'ci번호'], how = 'left')

        # 생성안된 세그먼트 생성
        listSegCheck = [x for x in pred_Seg.columns.tolist() if x not in ['기준년월', 'ci번호']]
        listSegNull = list(set(listSelectSeg) - set(listSegCheck))
        df_temp = pred_Seg[['ci번호']]
        
        for idx in listSegNull:
            df_temp[idx] = 0
        pred_Seg2 = pred_Seg.merge(df_temp, on = 'ci번호', how = 'left')
        pred_Seg2 = pred_Seg2[['기준년월', 'ci번호'] + listSelectSeg]
        PredSet  = PredSet.merge(pred_Seg2  , on = ['기준년월', 'ci번호'], how = 'left')

        
        # 아테나에 drop
        drop_athena_data(
              target = 'table'
            , bucket = bucket
            , schema = workDbNm
            , tblNm  = f"TrainSet_{idxRecCode}"
            , workGroup = workGrp  # pyathena 실행                
        )

        # 아테나에 적재    
        to_sql(
              TrainSet
            , f'TrainSet_{idxRecCode}'
            , conn
            , f's3://{bucket}/athena/TrainSet_{idxRecCode}/'
            , if_exists   = 'append'
            , schema      = workDbNm
            , chunksize   = 10000
            , max_workers = 5
        )
        
        # 아테나에 drop
        drop_athena_data(
              target = 'table'
            , bucket = bucket
            , schema = workDbNm
            , tblNm  = f"TestSet_{idxRecCode}"
            , workGroup = workGrp  # pyathena 실행                
        )

        # 아테나에 적재    
        to_sql(
              TestSet
            , f'TestSet_{idxRecCode}'
            , conn
            , f's3://{bucket}/athena/TestSet_{idxRecCode}/'
            , if_exists   = 'append'
            , schema      = workDbNm
            , chunksize   = 10000
            , max_workers = 5
        )        
        
        # 아테나에 drop
        drop_athena_data(
              target = 'table'
            , bucket = bucket
            , schema = workDbNm
            , tblNm  = f"PredSet_{idxRecCode}"
            , workGroup = workGrp  # pyathena 실행                
        )

        # 아테나에 적재    
        to_sql(
              PredSet
            , f'PredSet_{idxRecCode}'
            , conn
            , f's3://{bucket}/athena/PredSet_{idxRecCode}/'
            , if_exists   = 'append'
            , schema      = workDbNm
            , chunksize   = 10000
            , max_workers = 5
        )           
    

elif (len(listACCbaseYmd) <= 2) or (len(listACCbaseYmd2) <= 2):
    print('[LOG]' + "#" * 90)
    print("[LOG] 학습 및 평가 기준년월 부족 > 기존 모델로 예측")
    print('[LOG]' + "#" * 90)

    pred_Seg  = GenSegPvtQuery(batchdate  = baseYmd, seg_keys = seg_keys, set_cat = 'predSet', idxTgtTable = 'TCTAHMC14', idxRecCode = '10')
    for idxRecCode in [
            '10',
            '21',
            '2200',
        ]:
        
        print('[LOG]' + "#" * 90)
        print(f"[LOG] {idxRecCode} 평가월 생성")

        PredSet = MLStep4PredMartCretn(
            batchdate     = baseYmd,
            idxRecCode    = idxRecCode,
            idxRatio      = 3,
            idxVarTable   = 'TCTAHMC13',
            select_var_yn = 'y',
            )
        PredSet  = PreProcess(PredSet)    

        # 활용 변수 및 Seg Load
        TCTAHMC13 = pd.read_sql(f"""
        select * 
        from (
            select 
                  *
                , row_number() over(
                    partition by "테이블명", "활용변수명", "추천코드", "선택유무"
                    order by "기준년월" desc
                  )                                      as seq
            from {workDbNm}.TCTAHMC13
        )
        where seq = 1
        and "추천코드" = '{idxRecCode}'
        and "선택유무" = '1'
        """, conn)
        TCTAHMC13 = TCTAHMC13.drop('seq', axis = 1)

        ########################
        # 선택 변수 
        ########################

        tmp_TCTAHMC13 = TCTAHMC13.loc[(TCTAHMC13['추천코드'] == idxRecCode)
                                     &(TCTAHMC13['테이블명'].str[:7] == 'TCTAHMC')
                                     ]
        ftSelect =  ['lifestage', 'binningcat'] + tmp_TCTAHMC13['활용변수명'].tolist()
        listSelectSeg = TCTAHMC13.loc[(TCTAHMC13['추천코드'] == idxRecCode)
                                     &(TCTAHMC13['테이블명'].str[:7] == 'TCTAHMB')
                                     ]['활용변수명'].tolist()

        # 세그먼트명 변경
        listSelectSeg = ["SEG_" + x for x in listSelectSeg]
        ftSelect = ftSelect + listSelectSeg
        
        # 생성안된 세그먼트 생성
        listSegCheck = [x for x in pred_Seg.columns.tolist() if x not in ['기준년월', 'ci번호']]
        listSegNull = list(set(listSelectSeg) - set(listSegCheck))
        df_temp = pred_Seg[['ci번호']]
        for idx in listSegNull:
            df_temp[idx] = 0
        pred_Seg2 = pred_Seg.merge(df_temp, on = 'ci번호', how = 'left')
        pred_Seg2 = pred_Seg2[['기준년월', 'ci번호'] + listSelectSeg]
        PredSet = PredSet.merge(pred_Seg2, on = ['기준년월', 'ci번호'], how = 'left')

        # 아테나에 drop
        drop_athena_data(
              target = 'table'
            , bucket = bucket
            , schema = workDbNm
            , tblNm  = f"PredSet_{idxRecCode}"
            , workGroup = workGrp  # pyathena 실행                
        )

        # 아테나에 적재    
        to_sql(
              PredSet
            , f'PredSet_{idxRecCode}'
            , conn
            , f's3://{bucket}/athena/PredSet_{idxRecCode}/'
            , if_exists   = 'append'
            , schema      = workDbNm
            , chunksize   = 10000
            , max_workers = 5
        )           
