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

# 데이터를 재처리하는 함수
def update_data_to_dm():
    # PostgreSQL 연결 설정
    engine = create_engine('postgresql://userid:password@host:port/dbid')
    

    # 모든 팩트 테이블을 다시 생성하여 업데이트
    with engine.connect() as conn:
        # 팩트 테이블 (호선별) 재생성
        conn.execute("""
            DELETE FROM f_lt_sum;  -- 기존 데이터 삭제
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
            JOIN d_ym y ON r."YM" = CONCAT(y.year, LPAD(y.month, 2, '0'))
            JOIN d_onoff o ON o.onoff IN ('on', 'off')
            JOIN d_lt l ON r."LTNM" = l.ltnm
            GROUP BY y.ymid, o.onoffid, l.ltid
            ON CONFLICT (ymid, onoffid, ltid, sidonm) DO UPDATE 
                SET tot = EXCLUDED.tot;
        """)

        # 팩트 테이블 (시간대별 요약) 재생성
        conn.execute("""
            DELETE FROM f_slot_sum;  -- 기존 데이터 삭제
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
            JOIN d_slot s ON s.slot IN ('04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '00', '01', '02', '03')
            GROUP BY l.ltid, st.stnm, o.onoffid, y.ymid, s.slotid
            ON CONFLICT (ltid, stnm, onoffid, ymid, slotid, sidonm) DO UPDATE 
                SET tot = EXCLUDED.tot;
        """)

    	# 팩트 테이블 (역별) 재생성
        conn.execute("""
            DELETE FROM f_st_sum;  -- 기존 데이터 삭제
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
            GROUP BY y.ymid, o.onoffid, l.ltid, st.stnm
            ON CONFLICT (ymid, onoffid, ltid, stnm, sidonm) DO UPDATE 
                SET tot = EXCLUDED.tot;
        """)

# DAG 정의
with DAG(
    dag_id='update_data_to_dm',
    default_args=default_args,
    description='Update data mart from ODS',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    update_task = PythonOperator(
        task_id='update_data_task',
        python_callable=update_data_to_dm,
    )

    update_task
