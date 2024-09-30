from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# DAG의 기본 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 전월 데이터를 가져오기 위한 함수
def get_previous_month():
    now = datetime.now()
    previous_month = now - relativedelta(months=1)
    return str(previous_month.year) + str(previous_month.month).zfill(2)

# DAG 정의
with DAG(
    'move_etl_data_to_dm',
    default_args=default_args,
    description='Move ETL data to Data Mart with monthly update',
    schedule_interval='0 8 5 * *',  # 매달 5일 오전 8시
    start_date=datetime(2024, 9, 1),  # DAG의 시작 날짜 설정
    catchup=False,
) as dag:

    def move_data_to_dm():
	# PostgreSQL 연결 설정
        engine = create_engine('postgresql://userid:password@host:port/dbid')
         
	# 전월 데이터를 가져옴
        previous_month = get_previous_month()
        
        # 전월 데이터만 업데이트 - 디멘젼 테이블 (d_ym)
        with engine.connect() as conn:
            conn.execute(f"""
            INSERT INTO d_ym (ymid, year, month)
            SELECT DISTINCT
                "YM" AS ymid,
                SUBSTRING("YM", 1, 4) AS year,
                SUBSTRING("YM", 5, 2) AS month
            FROM slot_rid
            WHERE "YM" = '{previous_month}'
            ON CONFLICT (ymid) DO NOTHING; -- 중복된 ymid가 존재하면 삽입하지 않음
            """)

            # 전월 데이터만 업데이트 - 팩트 테이블 (호선별)
            conn.execute(f"""
            INSERT INTO f_lt_sum (ymid, onoffid, ltid, tot, sidonm)
            SELECT
                y.ymid,
                o.onoffid,
                l.ltid,
                COALESCE(SUM(
                    CASE 
                        WHEN o.onoff = 'on' THEN 
                            COALESCE(r."GETON04", 0) + COALESCE(r."GETON05", 0) + COALESCE(r."GETON06", 0) + COALESCE(r."GETON07", 0)
                            + COALESCE(r."GETON08", 0) + COALESCE(r."GETON09", 0) + COALESCE(r."GETON10", 0) + COALESCE(r."GETON11", 0)
                            + COALESCE(r."GETON12", 0) + COALESCE(r."GETON13", 0) + COALESCE(r."GETON14", 0) + COALESCE(r."GETON15", 0)
                            + COALESCE(r."GETON16", 0) + COALESCE(r."GETON17", 0) + COALESCE(r."GETON18", 0) + COALESCE(r."GETON19", 0)
                            + COALESCE(r."GETON20", 0) + COALESCE(r."GETON21", 0) + COALESCE(r."GETON22", 0) + COALESCE(r."GETON23", 0)
                            + COALESCE(r."GETON00", 0) + COALESCE(r."GETON01", 0) + COALESCE(r."GETON02", 0) + COALESCE(r."GETON03", 0)
                        ELSE 
                            COALESCE(r."GETOFF04", 0) + COALESCE(r."GETOFF05", 0) + COALESCE(r."GETOFF06", 0) + COALESCE(r."GETOFF07", 0)
                            + COALESCE(r."GETOFF08", 0) + COALESCE(r."GETOFF09", 0) + COALESCE(r."GETOFF10", 0) + COALESCE(r."GETOFF11", 0)
                            + COALESCE(r."GETOFF12", 0) + COALESCE(r."GETOFF13", 0) + COALESCE(r."GETOFF14", 0) + COALESCE(r."GETOFF15", 0)
                            + COALESCE(r."GETOFF16", 0) + COALESCE(r."GETOFF17", 0) + COALESCE(r."GETOFF18", 0) + COALESCE(r."GETOFF19", 0)
                            + COALESCE(r."GETOFF20", 0) + COALESCE(r."GETOFF21", 0) + COALESCE(r."GETOFF22", 0) + COALESCE(r."GETOFF23", 0)
                            + COALESCE(r."GETOFF00", 0) + COALESCE(r."GETOFF01", 0) + COALESCE(r."GETOFF02", 0) + COALESCE(r."GETOFF03", 0)
                    END
                ), 0) AS tot,
                '서울특별시' AS sidonm
            FROM slot_rid r
            JOIN d_ym y ON r."YM" = concat(y.year, LPAD(y.month, 2, '0'))
            JOIN d_onoff o ON o.onoff IN ('on', 'off')
            JOIN d_lt l ON r."LTNM" = l.ltnm
            WHERE r."YM" = '{previous_month}'
            GROUP BY y.ymid, o.onoffid, l.ltid
            ON CONFLICT (ymid, onoffid, ltid, sidonm) DO UPDATE 
                SET tot = EXCLUDED.tot; -- 기존 값이 있으면 업데이트
            """)

            # 전월 데이터만 업데이트 - 팩트 테이블 (시간대별 요약)
            conn.execute(f"""
            INSERT INTO f_slot_sum (ltid, stnm, onoffid, ymid, slotid, tot, sidonm)
            SELECT
                l.ltid,
                st.stnm,
                o.onoffid,
                y.ymid,
                s.slotid,
                COALESCE(SUM(
                    CASE
                        WHEN o.onoff = 'on' THEN
                            CASE s.slot
                                WHEN '04' THEN COALESCE(r."GETON04", 0)
                                WHEN '05' THEN COALESCE(r."GETON05", 0)
                                WHEN '06' THEN COALESCE(r."GETON06", 0)
                                WHEN '07' THEN COALESCE(r."GETON07", 0)
                                WHEN '08' THEN COALESCE(r."GETON08", 0)
                                WHEN '09' THEN COALESCE(r."GETON09", 0)
                                WHEN '10' THEN COALESCE(r."GETON10", 0)
                                WHEN '11' THEN COALESCE(r."GETON11", 0)
                                WHEN '12' THEN COALESCE(r."GETON12", 0)
                                WHEN '13' THEN COALESCE(r."GETON13", 0)
                                WHEN '14' THEN COALESCE(r."GETON14", 0)
                                WHEN '15' THEN COALESCE(r."GETON15", 0)
                                WHEN '16' THEN COALESCE(r."GETON16", 0)
                                WHEN '17' THEN COALESCE(r."GETON17", 0)
                                WHEN '18' THEN COALESCE(r."GETON18", 0)
                                WHEN '19' THEN COALESCE(r."GETON19", 0)
                                WHEN '20' THEN COALESCE(r."GETON20", 0)
                                WHEN '21' THEN COALESCE(r."GETON21", 0)
                                WHEN '22' THEN COALESCE(r."GETON22", 0)
                                WHEN '23' THEN COALESCE(r."GETON23", 0)
                                WHEN '00' THEN COALESCE(r."GETON00", 0)
                                WHEN '01' THEN COALESCE(r."GETON01", 0)
                                WHEN '02' THEN COALESCE(r."GETON02", 0)
                                WHEN '03' THEN COALESCE(r."GETON03", 0)
                            END
                        ELSE
                            CASE s.slot
                                WHEN '04' THEN COALESCE(r."GETOFF04", 0)
                                WHEN '05' THEN COALESCE(r."GETOFF05", 0)
                                WHEN '06' THEN COALESCE(r."GETOFF06", 0)
                                WHEN '07' THEN COALESCE(r."GETOFF07", 0)
                                WHEN '08' THEN COALESCE(r."GETOFF08", 0)
                                WHEN '09' THEN COALESCE(r."GETOFF09", 0)
                                WHEN '10' THEN COALESCE(r."GETOFF10", 0)
                                WHEN '11' THEN COALESCE(r."GETOFF11", 0)
                                WHEN '12' THEN COALESCE(r."GETOFF12", 0)
                                WHEN '13' THEN COALESCE(r."GETOFF13", 0)
                                WHEN '14' THEN COALESCE(r."GETOFF14", 0)
                                WHEN '15' THEN COALESCE(r."GETOFF15", 0)
                                WHEN '16' THEN COALESCE(r."GETOFF16", 0)
                                WHEN '17' THEN COALESCE(r."GETOFF17", 0)
                                WHEN '18' THEN COALESCE(r."GETOFF18", 0)
                                WHEN '19' THEN COALESCE(r."GETOFF19", 0)
                                WHEN '20' THEN COALESCE(r."GETOFF20", 0)
                                WHEN '21' THEN COALESCE(r."GETOFF21", 0)
                                WHEN '22' THEN COALESCE(r."GETOFF22", 0)
                                WHEN '23' THEN COALESCE(r."GETOFF23", 0)
                                WHEN '00' THEN COALESCE(r."GETOFF00", 0)
                                WHEN '01' THEN COALESCE(r."GETOFF01", 0)
                                WHEN '02' THEN COALESCE(r."GETOFF02", 0)
                                WHEN '03' THEN COALESCE(r."GETOFF03", 0)
                            END
                    END
                ), 0) AS tot,
                '서울특별시' AS sidonm
            FROM slot_rid r
            JOIN d_ym y ON r."YM" = CONCAT(y.year, LPAD(y.month, 2, '0'))
            JOIN d_onoff o ON o.onoff IN ('on', 'off')
            JOIN d_lt l ON r."LTNM" = l.ltnm
            JOIN d_st st ON r."STNM" = st.stnm AND l.ltid = st.ltid
            JOIN d_slot s ON s.slot IN ('04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15',
                                        '16', '17', '18', '19', '20', '21', '22', '23', '00', '01', '02', '03')
            WHERE r."YM" = '{previous_month}'
            GROUP BY l.ltid, st.stnm, o.onoffid, y.ymid, s.slotid
            ON CONFLICT (ltid, stnm, onoffid, ymid, slotid, sidonm) DO UPDATE 
                SET tot = EXCLUDED.tot; -- 기존 값이 있으면 업데이트
            """)

            # 전월 데이터만 업데이트 - 팩트 테이블 (역별)
            conn.execute(f"""
            INSERT INTO f_st_sum (ymid, onoffid, ltid, stnm, tot, sidonm)
            SELECT
                y.ymid,
                o.onoffid,
                l.ltid,
                st.stnm,
                COALESCE(SUM(
		    CASE
	                    WHEN o.onoff = 'on' THEN 
        	                    COALESCE(r."GETON04", 0) + COALESCE(r."GETON05", 0) + COALESCE(r."GETON06", 0) + COALESCE(r."GETON07", 0) + COALESCE(r."GETON08", 0) + COALESCE(r."GETON09", 0) + COALESCE(r."GETON10", 0) + COALESCE(r."GETON11", 0) + COALESCE(r."GETON12", 0) + COALESCE(r."GETON13", 0) + COALESCE(r."GETON14", 0) + COALESCE(r."GETON15", 0) + COALESCE(r."GETON16", 0) + COALESCE(r."GETON17", 0) + COALESCE(r."GETON18", 0) + COALESCE(r."GETON19", 0) + COALESCE(r."GETON20", 0) + COALESCE(r."GETON21", 0) + COALESCE(r."GETON22", 0) + COALESCE(r."GETON23", 0) + COALESCE(r."GETON00", 0) + COALESCE(r."GETON01", 0) + COALESCE(r."GETON02", 0) + COALESCE(r."GETON03", 0)
                            ELSE 
                                COALESCE(r."GETOFF04", 0) + COALESCE(r."GETOFF05", 0) + COALESCE(r."GETOFF06", 0) + COALESCE(r."GETOFF07", 0) + COALESCE(r."GETOFF08", 0) + COALESCE(r."GETOFF09", 0) + COALESCE(r."GETOFF10", 0) + COALESCE(r."GETOFF11", 0) + COALESCE(r."GETOFF12", 0) + COALESCE(r."GETOFF13", 0) + COALESCE(r."GETOFF14", 0) + COALESCE(r."GETOFF15", 0) + COALESCE(r."GETOFF16", 0) + COALESCE(r."GETOFF17", 0) + COALESCE(r."GETOFF18", 0) + COALESCE(r."GETOFF19", 0) + COALESCE(r."GETOFF20", 0) + COALESCE(r."GETOFF21", 0) + COALESCE(r."GETOFF22", 0) + COALESCE(r."GETOFF23", 0) + COALESCE(r."GETOFF00", 0) + COALESCE(r."GETOFF01", 0) + COALESCE(r."GETOFF02", 0) + COALESCE(r."GETOFF03", 0)
                    END
                ), 0) AS tot,
                '서울특별시' AS sidonm
            FROM slot_rid r
            JOIN d_ym y ON r."YM" = CONCAT(y.year, LPAD(y.month, 2, '0'))
            JOIN d_onoff o ON o.onoff IN ('on', 'off')
            JOIN d_lt l ON r."LTNM" = l.ltnm
            JOIN d_st st ON r."STNM" = st.stnm AND l.ltid = st.ltid
            WHERE r."YM" = '{previous_month}'
            GROUP BY y.ymid, o.onoffid, l.ltid, st.stnm
            ON CONFLICT (ymid, onoffid, ltid, stnm, sidonm) DO UPDATE 
                SET tot = EXCLUDED.tot;
            """)

        
    # DAG에 PythonOperator 추가
    update_data = PythonOperator(
        task_id='update_data_to_dm',
        python_callable=move_data_to_dm,
    )

    update_data
