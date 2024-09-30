from datetime import datetime, timedelta
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
from sqlalchemy import create_engine

# Python 함수 정의 (ETL 작업)
def extract(**kwargs):
    # 데이터를 추출하는 로직
    print("Extracting data...")
    
    # 시간대별 승하차
    time = datetime.now(timezone('Asia/Seoul'))
    url = "url"
    res = requests.get(url)
    data = res.json()

    # DB화
    df = pd.DataFrame(data['CardSubwayTime']['row'])

    # DataFrame을 반환하여 XCom에 저장
    return df


def transform(ti, **kwargs):
    # 데이터를 변환하는 로직
    print("Transforming data...")

    # Extract에서 반환된 DataFrame을 XCom에서 가져옴
    df = ti.xcom_pull(task_ids='extract')
    
    # column명 변경
    df.columns = ["YM", "LTNM", "STNM", "GETON04", "GETOFF04", "GETON05", "GETOFF05", "GETON06", "GETOFF06", "GETON07", "GETOFF07", "GETON08", "GETOFF08", "GETON09", "GETOFF09", "GETON10", "GETOFF10", "GETON11", "GETOFF11", "GETON12", "GETOFF12", "GETON13", "GETOFF13", "GETON14", "GETOFF14", "GETON15", "GETOFF15", "GETON16", "GETOFF16", "GETON17", "GETOFF17", "GETON18", "GETOFF18", "GETON19", "GETOFF19", "GETON20", "GETOFF20", "GETON21", "GETOFF21", "GETON22", "GETOFF22", "GETON23", "GETOFF23", "GETON00", "GETOFF00", "GETON01", "GETOFF01", "GETON02", "GETOFF02", "GETON03", "GETOFF03", "REG"
]

    # colum 전처리
    df2 = df[df["LTNM"].isin(['1호선', '2호선', '3호선', '4호선', '5호선', '6호선', '7호선', '8호선'])]
    df2['LTNM'] = df2['LTNM'].str.extract(r'(\d+)')
    df2 = df2.reset_index(drop=True)
    df2["STNM"] = df2['STNM'].str.split('(', n=1, expand=True)[0]

    # '서울역'을 '서울'로 변경
    df2["STNM"] = df2["STNM"].replace("서울역", "서울")

    # 데이터 타입 변경
    df2 = df2.astype({"LTNM":"int", "GETON04":"int", "GETOFF04":"int", "GETON05":"int", "GETOFF05":"int", "GETON06":"int", "GETOFF06":"int", "GETON07":"int", "GETOFF07":"int", "GETON08":"int", "GETOFF08":"int", "GETON09":"int", "GETOFF09":"int", "GETON10":"int", "GETOFF10":"int", "GETON11":"int", "GETOFF11":"int", "GETON12":"int", "GETOFF12":"int", "GETON13":"int", "GETOFF13":"int", "GETON14":"int", "GETOFF14":"int", "GETON15":"int", "GETOFF15":"int", "GETON16":"int", "GETOFF16":"int", "GETON17":"int", "GETOFF17":"int", "GETON18":"int", "GETOFF18":"int", "GETON19":"int", "GETOFF19":"int", "GETON20":"int", "GETOFF20":"int", "GETON21":"int", "GETOFF21":"int", "GETON22":"int", "GETOFF22":"int", "GETON23":"int", "GETOFF23":"int", "GETON00":"int", "GETOFF00":"int", "GETON01":"int", "GETOFF01":"int", "GETON02":"int", "GETOFF02":"int", "GETON03":"int", "GETOFF03":"int"})

    # 데이터 값 변경
    df2['STNM'] = df2['STNM'].replace('자양', '뚝섬유원지')

    # 중복데이터 제거
    df2 = df2[~((df2['STNM'].isin(["까치울", "신중동", "부천시청", "상동", "굴포천", "삼산체육관", "춘의", "상동", "부평구청"])) & (df2['LTNM'] == 7))]
    df2 = df2[~((df2['STNM'].isin(["암사역사공원"])) & (df2['LTNM'] == 8))]
    print(len(df2))

    return df2

def load(ti, **kwargs):
    # 데이터를 PostgreSQL로 적재하는 로직
    print("Loading data...")

    # Transform에서 반환된 DataFrame을 XCom에서 가져옴
    df = ti.xcom_pull(task_ids='transform')
    
    # PostgreSQL 연결 설정
    engine = create_engine('postgresql://userid:password@host:port/dbname')

    # 데이터프레임을 PostgreSQL에 적재
    df.to_sql('rid_h', engine, if_exists='append', index=False)

    print("Data loaded into PostgreSQL successfully.")

# 기본 DAG 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 객체 생성
with DAG(
    'etl_pipeline',  # DAG의 ID
    default_args=default_args,
    description='ETL pipeline that runs monthly',
    schedule_interval='0 7 5 * *', # 매달 5일 오전 7시 (Cron 표현식)
    start_date=datetime(2024, 9, 1),
    catchup=False,
) as dag:

    # Task 정의
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    # Task 의존성 설정
    extract_task >> transform_task >> load_task
