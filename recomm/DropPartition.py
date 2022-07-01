###########################################################################################
# 패키지 Import
###########################################################################################
import pandas as pd
import os, time
os.environ['TZ'] = 'Asia/Seoul'
time.tzset()
from datetime import datetime
import warnings
import boto3
import pyathena
import json
warnings.filterwarnings('ignore')

###########################################################################################
# 함수 선언
###########################################################################################
# RETURN 0 : 비정상 종료 / 1: 정상 종료
def drop_athena_data(
          target                       # drop 대상 ["TABLE" , "PARTITION"]
        , bucket                       # bucket 명
        , schema                       # athena 스키마명
        , tblNm                        # athena 테이블명
        , p_col     = None             # 파티션컬럼명(리스트)
        , p_val     = None             # 파티션값(리스트)
        , workGroup = "kl0-proj-0005"  # pyathena 실행 workgroup(default: kl0-proj-0005)      
    ):
    
    st_tm = datetime.now()
    flg_part = 0                       # 파티션여부 flag
    flg_tab_exst = 1                   # 테이블존재 여부 flag[default : 존재]
    tbl_del_Flg = 0                    # 삭제 여부 flag[0: 삭제실패 / 1: 테이블(파티션) DROP / 2: 파일까지 삭제]
    
    s3_resource = boto3.resource("s3", region_name='ap-northeast-2')   
        
    print(f'[LOG] drop_athena_data > 대상테이블 = {schema}.{tblNm}, 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')

    ###############################################################
    # 파라미터(target) 입력값 체크
    ############################################################### 
    list_target = ["TABLE" , "PARTITION"]
    if target.upper() in list_target:
        pass
    else:
        print(f'[LOG] drop_athena_data > target 값이 잘못 입력되었습니다. ["TABLE" , "PARTITION"], 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
    
    ###########################################################################################
    # Athena 및 S3 연결
    ########################################################################################### 
    conn  = pyathena.connect(
          s3_staging_dir = f"s3://{bucket}/athena/output"
        , region_name    = "ap-northeast-2"
        , work_group     = f"{workGroup}"
    )
    
    try:
        # connection 자체는 에러가 발생하지 않음
        pd.read_sql_query(f"SELECT 1", conn)
    except:
        print(f'[LOG] drop_athena_data > pyathena 연결 중 에러가 발생했습니다., 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
        return 0
        
    try:
        # 단순 객체 선언만으로 에러가 발생하지 않음
        s3Bucket = s3_resource.Bucket(name=bucket)
        for file in s3Bucket.objects.filter(Prefix="athena/").all():
            file
            break
    except:
        print(f'[LOG] drop_athena_data > s3 연결(boto3) 중 에러가 발생했습니다., 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
        return 0
    
    ###########################################################################################
    # 테이블 확인 및 경로 구해오기
    ########################################################################################### 
    str_target_loc = ''
    try:
        desc_cs = conn.cursor().execute(f"""
            desc formatted `{schema}`.{tblNm}
        """)
        desc_tab = desc_cs.fetchall()
        for desc in desc_tab:
        #     print(x[0])
            if 'Location' in desc[0]:
                str_loc = desc[0].split('\t')[1]
                str_target_loc = str_loc[str_loc.find(bucket)+len(bucket)+1:].replace("'", "")
                break
        
    except:
        1
    
    # 여태까지 path 를 가져오지 못했을 경우
    if str_target_loc == '':
        try:
            getPath = pd.read_sql(f""" select "$path" from {schema}.{tblNm} limit 1 """, conn)
            str_loc = getPath.iloc[0][0].split('=')[0].rsplit('/', 1)[0]
            str_target_loc = str_loc[str_loc.find(bucket)+len(bucket)+1:].replace("'", "")
            str_target_loc
        except:
            2
    
    # 여태까지 path 를 가져오지 못했을 경우
    if str_target_loc == '':
        try:
            glueClient = boto3.client('glue')
            str_loc = glueClient.get_table_versions(
                  DatabaseName=f'{schema}'
                , TableName=f'{tblNm}'
            )['TableVersions'][0]['Table']['StorageDescriptor']['Location']
            str_target_loc = str_loc[str_loc.find(bucket)+len(bucket)+1:].replace("'", "")
        except:
            3   
        
    # 여태까지 path 를 가져오지 못했을 경우
    if str_target_loc == '':
        try:
            df_ddl = pd.read_sql_query(f"show create table `{schema}`.`{tblNm}`", conn)

            if df_ddl.loc[(df_ddl['createtab_stmt'] == 'LOCATION')].index[0] > 0:
                str_loc = df_ddl.iloc[df_ddl.loc[(df_ddl['createtab_stmt'] == 'LOCATION')].index[0]+1][0].strip()
                str_target_loc = str_loc[str_loc.find(bucket)+len(bucket)+1:].replace("'", "")

        except:
            4

    # 경로 미확인 시 하드코딩으로 경로 생성
    if str_target_loc == '':
        str_target_loc = f'athena/{tblNm.upper()}'
        print(f'[LOG] drop_athena_data > {schema}.{tblNm} 테이블이 존재하지 않습니다., 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
    
    ###############################################################
    # DROP 문 수행
    ############################################################### 
    if target.upper() == "TABLE":
        ###############################################################
        # TABLE DROP 시
        ###############################################################
        try:
            pd.read_sql_query(f"drop table `{schema}`.`{tblNm}`", conn)
            tbl_del_Flg = 1
            delChk = s3Bucket.objects.filter(Prefix=f"{str_target_loc}").delete()
            if len(delChk) > 0:
                tbl_del_Flg = 2
        except:
            print(f'[LOG] drop_athena_data > {schema}.{tblNm} DROP 대상 테이블 존재하지 않습니다., 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')

    else: 
        ###############################################################
        # PARTITION DROP 시
        ###############################################################     
        ###########################################
        # 파티션 입력값 체크
        ###########################################
        if p_col != None:
            if len(p_col) < len(p_val):
                print(f'[LOG] drop_athena_data > p_val 값이 적게 입력되었습니다., 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
                return 0
            elif len(p_col) > len(p_val):
                print(f'[LOG] drop_athena_data > p_val 값이 적게 입력되었습니다., 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
                return 0
        
        # 실제 파티션 검증
        df_part_col = pd.read_sql_query(
            f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE 1=1
                and table_schema = '{schema.lower()}'
                and table_name = '{tblNm.lower()}'
                and extra_info = 'partition key'
            """
            , conn
        )
        list_part_col = list(df_part_col['column_name'])
        
        # 파티션 값을 줬지만 파티션이 없는 경우
        if len(list_part_col) == 0:
            print(f'[LOG] drop_athena_data > {schema}.{tblNm} 테이블의 파티션 정보가 상이합니다. input:{p_col} / 실제: 미존재, 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
            return 0

        # 파티션 컬럼값을 모두 대문자로 변환 후 비교
        c_p_col = [i.upper() for i in p_col]
        c_list_part_col = [i.upper() for i in list_part_col]
        if c_p_col != c_list_part_col:
            print(f'[LOG] drop_athena_data > {schema}.{tblNm} 테이블의 파티션 정보가 상이합니다. input:{p_col} / 실제:{list_part_col}, 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
            return 0

        ###########################################
        # DROP PARTITION
        ###########################################
        for i in range(0, len(p_col)):
            print(f'[LOG] drop_athena_data > 대상테이블 = {schema}.{tblNm} {target.upper()} 삭제조건 : {p_col[i]} = {p_val[i]}, 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
        
        # 정상 CASE 의 경우, 해당 값으로 SQL / S3 경로 생성
        str_part_spc_sql = ""
        str_part_spc_s3 = ""
        for i in range(0, len(p_col)):
            str_part_spc_sql = (str_part_spc_sql+", " if len(str_part_spc_sql) > 0 else "") + f"`{p_col[i].lower()}`='{p_val[i]}'"
            str_part_spc_s3 = (str_part_spc_s3+"/" if len(str_part_spc_s3) > 0 else "") + f"{p_col[i].lower()}={p_val[i]}"
            
        # DROP 수행
        try:
            pd.read_sql_query(f'ALTER TABLE {schema}.{tblNm} DROP IF EXISTS PARTITION ({str_part_spc_sql})', conn)
            tbl_del_Flg = 1
            delChk = s3Bucket.objects.filter(Prefix=f"{str_target_loc}/{str_part_spc_s3}").delete()
            if len(delChk) > 0:
                tbl_del_Flg = 2
        except:
            print(f'[LOG] drop_athena_data > {schema}.{tblNm} 파티션 삭제 중 에러가 발생했습니다., 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')

    # 최종 결과에 대한 검토
    if tbl_del_Flg == 1:
        print(f'[LOG] drop_athena_data > 대상테이블 = {schema}.{tblNm} {target.upper()} 삭제 완료, 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
    elif tbl_del_Flg == 2:
        print(f'[LOG] drop_athena_data > 대상테이블 = {schema}.{tblNm} {target.upper()} 및 해당경로 삭제 완료, 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}')
    else:
        return 0
    
    ed_tm = datetime.now()
    el_tm = ed_tm - st_tm
    print(f'[LOG] drop_athena_data > 대상테이블 = {schema}.{tblNm}, 실행시간 = {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")}, 소요시간 = {str(el_tm).split(".")[0]}')
    return 1
