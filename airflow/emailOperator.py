from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'email': ['email@email.co.kr'],   # DAG에서 기본 이메일 설정
    'email_on_failure': True,                 # 작업 실패 시 이메일 전송
    'email_on_retry': False,                  # 재시도 시 이메일 전송 안 함
}

with DAG(
        dag_id='emailOperator',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:

    start = DummyOperator(
            task_id='start'
    )

    email_task = EmailOperator(
            task_id='send_email',
            to='email@email.co.kr',
            subject='Airflow 성공 메일',
            html_content='Airflow 작업이 완료되었습니다.',
        )

    start >> email_task
