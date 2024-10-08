# 지하철 이용량 분석 프로젝트

## 👉 Develop Motive
- 출퇴근 시간대 인력 부족으로 인한 승객 안전 관리 문제
- 응급 상황 대처 미흡
- 역, 호선, 시간대별 승객 수 차이로 인한 인력 배치 어려움
- 효율적인 인력 배치 도구 개발 필요

## 👉 Data Pipeline
데이터 파이프라인을 직접 구축하는 프로젝트입니다.
![image](https://github.com/user-attachments/assets/81f17520-3a24-4d12-9e46-778f0c784219)

### 원천 데이터
[시간대별 이용량](https://data.seoul.go.kr/dataList/OA-12252/S/1/datasetView.do)

[위경도](https://www.data.go.kr/data/15099316/fileData.do?recommendDataYn=Y)

[시도코드](https://sgis.kostat.go.kr/developer/html/openApi/api/dataCode/SidoCode.html)

### 스케줄러
데이터가 매 달 한번 입력되어 처리되어야하기에, 배치 작업에(batch processing)에 적합한 Airflow를 선택했습니다.

### ETL & DW
ETL 과정의 코드와 DW 설계는 [airflow](airflow) 폴더를 참조하시길 바랍니다. DB는 postgreSQL을 사용했습니다.



## 👉 전체 시연
https://github.com/user-attachments/assets/26497e33-b912-42db-9f9d-fb60fe1aa7cf

## 👉 프로젝트 성과 및 반성
### 성과
- ETL 기법과 BI 도구 활용 역량 향상
- 사용자 맞춤형 대시보드 구축 및 데이터 시각화 역량 확보
- DW 설계 역량 향상
- 동적 필터링과 다중 선택 옵션 쿼리 구조 이해 및 구현

### 반성
- 역 이름 표기 통합 및 지리적 데이터 연결 등 데이터 클렌징 작업에서 많은 시간 소요
- 초기 단계에서의 요구사항 설정 강화 피요
