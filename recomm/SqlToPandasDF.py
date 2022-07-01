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
warnings.filterwarnings('ignore')

# 쿼리 실행
# 반드시, conn 객체 생성 후 호출
def sqlToPandasDF(
      athena_conn   # 현재 Athena connection 객체
    , sql_trgt # sql 파일명
):
    import io
    
    g_start_time = datetime.now()
    print(f'[ {datetime.strftime(g_start_time, "%Y-%m-%d %H:%M:%S")} ] 작업 시작')
    
    # logging 시 용량을 알아보기 편하도록 변경하는 함수
    def byte_transform(s_byte, size=1024):
        if s_byte < size:
            return f'{s_byte} Bytes'
        b_arr = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']
        r = float(s_byte)
        for i in range(len(b_arr)):
            if s_byte/(size**i) < 1:
                return f'{round(s_byte/(size**(i-1)), 2)} {b_arr[i-1]}'
        return f'{round(r,2)}'

    print(f'[ {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")} ] SELECT 쿼리 실행')
    
    
    conn_cs = athena_conn.cursor()
    conn_cs.execute(sql_trgt)
    
    # excute 이후에 생기는 값(순서 주의)
    trgt_q_id   = conn_cs.query_id
    desc_tpl    = conn_cs.description
    output_path = conn_cs.output_location.replace('s3://', '').split('/', 1)
    
    
    q_end_time = datetime.now()
    q_el_time  = q_end_time - g_start_time
    print(f'[ {datetime.strftime(q_end_time, "%Y-%m-%d %H:%M:%S")} ] 쿼리 ID: {trgt_q_id}, SQL 대기 및 실행시간: {str(q_el_time).split(".")[0]}')
    
    
    s3Client  = boto3.client('s3')
    dict_cols = {}
    
    
    print(f'[ {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")} ] 쿼리 결과 METADATA 추출 / Columns = {len(desc_tpl)}')
    
    
    for col_tpl in desc_tpl:
        col_nm   = col_tpl[0]
        tmp_type = col_tpl[1]
        col_type = 'object'
        if tmp_type == 'char' or tmp_type == 'varchar':
            col_type = 'object'
        elif tmp_type == 'decimal' or tmp_type == 'double':
            col_type = 'float64'
        elif tmp_type == 'smallint' or tmp_type == 'tinyint':
            col_type = 'int64' # int32 or int16으로 변경?
        elif tmp_type == 'integer' or tmp_type == 'bigint':
            col_type = 'int64'
        elif 'timestamp' in tmp_type or 'date' in tmp_type:
            # TODO? timestamp 는 단순 문자열로 처리
            tmp_type = 'object'
        else:
            print(f'컬럼 "{col_nm}"은 object 로 생성, {tmp_type} 에 대한 대응 필요')
        
        dict_cols[col_nm] = col_type    
    
    s3_obj = s3Client.get_object(Bucket=output_path[0], Key=output_path[1])
    
        
    print(f'[ {datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")} ] 쿼리 결과 Load 수행 / Size = {byte_transform(s3_obj["ContentLength"])}')
    
#     engine = c
    df_rslt = pd.concat(pd.read_csv(io.BytesIO(s3_obj['Body'].read()), engine='c', dtype=dict_cols, chunksize=100000))
    
    
    g_end_time = datetime.now()
    l_el_time = g_end_time - q_end_time
    g_el_time = g_end_time - g_start_time
    print(f'[ {datetime.strftime(g_end_time, "%Y-%m-%d %H:%M:%S")} ] {format(df_rslt.shape[0], ",")} rows, 로딩시간: {str(l_el_time).split(".")[0]}')
    print(f'[ {datetime.strftime(g_end_time, "%Y-%m-%d %H:%M:%S")} ] 작업 종료, 총 소요시간: {str(g_el_time).split(".")[0]}')
    
    return df_rslt
