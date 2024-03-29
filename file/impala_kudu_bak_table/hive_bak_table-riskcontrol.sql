﻿-- # 【创建数据库】
create database if not exists bak_riskcontrol;

-- # 【删除表】
drop table if exists bak_riskcontrol.inf_customer;
drop table if exists bak_riskcontrol.inf_customer_credit;
drop table if exists bak_riskcontrol.his_customer_credit;
drop table if exists bak_riskcontrol.customer_credit_apply_log;
drop table if exists bak_riskcontrol.inf_credit_frozen;
drop table if exists bak_riskcontrol.inf_credit_unfrozen;
drop table if exists bak_riskcontrol.customer_blacklist;
drop table if exists bak_riskcontrol.call_ack_task;
drop table if exists bak_riskcontrol.inf_baiqishi;
drop table if exists bak_riskcontrol.inf_tongdun;
drop table if exists bak_riskcontrol.inf_tongdun_education;
drop table if exists bak_riskcontrol.inf_cs_insurance;
drop table if exists bak_riskcontrol.inf_cs_accum_found;
drop table if exists bak_riskcontrol.inf_zmxy;
-- drop table if exists bak_riskcontrol.interface_log;

drop table if exists bak_riskcontrol.inf_channel;
drop table if exists bak_riskcontrol.inf_loan_product;

-- # 创建表
CREATE TABLE if not exists bak_riskcontrol.inf_baiqishi (
	ID 					STRING,
	CERT_ID 			STRING,
	CREATE_DATE 		STRING,
	CUST_NAME 			STRING,
	FINAL_DECISION 		STRING,
	FINAL_SCORE 		INT,
	FLOW_NO 			STRING,
	INTERFACE_LOG_ID 	STRING,
	LOAN_PROD_CODE 		STRING,
	MOBILE 				STRING,
	RESULT_CODE 		STRING,
	RESULT_DESC 		STRING,
	HIT_BLACKLIST_CNT 	INT,
	HIT_GREYLIST_CNT 	INT,
	BIGDATA_DEL_STATUS	INT,
	BIGDATA_SYNC_TIME	BIGINT
)
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_tongdun (
	ID 					STRING,
	CERT_ID 			STRING,
	CREATE_DATE 		STRING,
	CUST_NAME 			STRING,
	DECISION_LOG_ID 	STRING,
	DECISION_DATA 		STRING,
	FINAL_SCORE 		BIGINT,
	HIT_BLACK_LIST 		STRING,
	MOBILE 				STRING,
	RESULT_CODE 		STRING,
	RESULT_DESC 		STRING,
	DETAIL_LOG_ID 		STRING,
	SEQ_ID 				STRING,
	EVENT_ID 			STRING,
	BIGDATA_DEL_STATUS	INT,
	BIGDATA_SYNC_TIME	BIGINT
)
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_tongdun_education (
	ID 					STRING,
	EDUCATION_DEGREE 	STRING,
	ENTRANCE_DATE 		STRING,
	SCHOOL_NAME 		STRING,
	GRADUATE_TIME 		STRING,
	CERT_ID 			STRING,
	PHOTE 				STRING,
	MAJOR 				STRING,
	STUDY_RESULT 		STRING,
	STUDY_STYLE 		STRING,
	NAME 				STRING,
	CREATE_DATE 		STRING,
	MODIFY_DATE 		STRING,
	BIGDATA_DEL_STATUS	INT,
	BIGDATA_SYNC_TIME	BIGINT
)
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_cs_accum_found (
	ID					STRING,
	ACC_BAL				STRING,
	ACC_CODE			STRING,
	ACC_STATE			STRING,
	BMNY				STRING,
	BMNY_TIMES			STRING,
	CERT_ID				STRING,
	CORP_CODE			STRING,
	CORP_DEPMNY			STRING,
	CORP_NAME			STRING,
	CREATE_DATE			STRING,
	CUR_BAL				STRING,
	CUR_LIFT_CUM		STRING,
	CUR_NOPAY			STRING,
	CUR_RATE_CUM		STRING,
	DEP_ACC				STRING,
	DEP_CODE			STRING,
	DEP_NAME			STRING,
	DEP_TYPE			STRING,
	FIN_CODE			STRING,
	INTERFACE_LOG_ID	STRING,
	LLIFT_CUM			STRING,
	LRATE_CUM			STRING,
	PAY_END_MNH			STRING,
	PER_CODE			STRING,
	PER_CORP_SCALE		STRING,
	PER_DEPMNY			STRING,
	PER_PER_SCALE		STRING,
	REG_TIME			STRING,
	REMIT_MNH			STRING,
	SCALE_TIMES			STRING,
	YD_ACC_BAL			STRING,
	CLOSE_TIME			STRING,
	FETCH_TIME			STRING,
	BIGDATA_DEL_STATUS	INT,
	BIGDATA_SYNC_TIME	BIGINT
)
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_cs_insurance (
	ID					STRING,
	CERT_ID				STRING,
	CORP_NAME			STRING,
	CREATE_DATE			STRING,
	CUST_NAME			STRING,
	FIRST_IN_DATE		STRING,
	GENDER				STRING,
	INTERFACE_LOG_ID	STRING,
	PAY_SALARY			DOUBLE,
	PAY_STATUS			STRING,
	RATE				STRING,
	BIGDATA_DEL_STATUS	INT,
	BIGDATA_SYNC_TIME	BIGINT
)
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_zmxy (
	ID			 					STRING,
	CERT_ID							STRING,
	ANTI_FRAUD_INTEGRATE_QRY_LOG_ID	STRING,
	ANTI_FRAUD_RISK_LOG_ID			STRING,
	ANTI_FRAUD_SCORE_LOG_ID			STRING,
	ANTI_FRAUD_VERIFY_LOG_ID		STRING,
	CREDIT_SCORE_LOG_ID				STRING,
	WATCH_LIST_LOG_ID				STRING,
	CREATE_DATE						STRING,
	MODIFY_DATE						STRING,
	CERTIFICATION_QRY_LOG_ID		STRING,
	ZMXY_SCORE						STRING,
	BIGDATA_DEL_STATUS				INT,
	BIGDATA_SYNC_TIME				BIGINT
)
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.call_ack_task (
	TASK_ID				STRING,
	ACCOUNT_NO			STRING,
	CERT_ID				STRING,
	CERT_TYPE			STRING,
	CHECK_TYPE			STRING,
	CREATE_DATE			STRING,
	CUST_NAME			STRING,
	FILE_NAME			STRING,
	MOBILE_NO			STRING,
	MODIFY_DATE			STRING,
	OPERATOR_ID			STRING,
	REFER_ID			STRING,
	REMARK				STRING,
	SERVER_PATH			STRING,
	STATUS				STRING,
	CREDIT_TYPE			STRING,
	LOAN_AMOUNT			DOUBLE,
	BIGDATA_DEL_STATUS	INT,
	BIGDATA_SYNC_TIME	BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.customer_blacklist (
	ID					STRING,
	BLACKLIST_TYPE		BIGINT,
	CERT_ID				STRING,
	CERT_TYPE			STRING,
	CREATE_DATE			STRING,
	CUST_NAME			STRING,
	MODIFY_DATE			STRING,
	OPERATE_ID			STRING,
	REMARK				STRING,
	SOURCE_FROM			BIGINT,
	ENABLED				INT,
	MOBILE_NO			STRING,
	ACCOUNT_NO			STRING,
	BIGDATA_DEL_STATUS	INT,
	BIGDATA_SYNC_TIME	BIGINT
)
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_credit_frozen (
	ID					STRING,
	CERT_ID				STRING, 
	CREATE_DATE			STRING,
	CUST_NAME			STRING,
	FROZEN_AMOUNT		DOUBLE, 
	ORIGINAL_STATUS		STRING, 
	REMARK				STRING, 
	SOURCE_TYPE			STRING, 
	CREDIT_INFO			STRING, 
	OPERATOR_ID			STRING, 
	BUSI_SEQ			STRING, 
	BIGDATA_DEL_STATUS	INT,
	BIGDATA_SYNC_TIME	BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_credit_unfrozen (
	ID					STRING,
	CERT_ID				STRING,
	CREATE_DATE			STRING,
	CUST_NAME			STRING,
	REMARK				STRING,
	SOURCE_TYPE			STRING,
	UNFROZEN_AMOUNT		DOUBLE,
	CREDIT_INFO			STRING,
	OPERATOR_ID			STRING, 
	FROZEN_OPERATOR		STRING, 
	BUSI_SEQ			STRING, 
	FROZEN_BUSI_SEQ		STRING, 
	BIGDATA_DEL_STATUS	INT,
	BIGDATA_SYNC_TIME	BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_customer(
	CUST_ID					STRING
	, CERT_ID				STRING
	, CUST_NAME				STRING
	, MOBILE_NO				STRING
	, REMARK				STRING
	, ACCOUNT_NO			STRING
	, APPLY_TYPE			INT
	, INTRODUCER			STRING
	, INTRODUCER_RELATION	STRING
	, CORP_NAME				STRING
	, INTRODUCER_CODE		STRING
	, CREATE_DATE			STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_customer_credit(
	ID						STRING
	, CERT_ID				STRING
	, CUST_NAME 			STRING
	, CREATE_DATE			STRING
	, CHANNEL_CODE 			STRING
	, LOAN_PROD 			STRING
	, CREDIT_TYPE 			STRING
	, REPAYMENT_TYPE 		STRING
	, TERM 					INT
	, RATE 					DOUBLE
	, CREDIT_AMOUNT 		DOUBLE
	, FROZEN_AMOUNT 		DOUBLE
	, SOURCE_TYPE 			STRING
	, MANUAL_VERIFIED 		STRING
	, EXPIRE_DATE 			STRING
	, START_DATE 			STRING
	, LATEST_EXPIRE_DATE 	STRING
	, ACTIVATED 			STRING
	, ACTIVED_DATE 			STRING
	, VALIDATE_DATE 		STRING
	, HOUSINGINFO 			STRING
	, MODIFY_DATE 			STRING
	, OPERATOR_ID 			STRING
	, RECHECK_OPERATOR 		STRING
	, REMARK				STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.his_customer_credit(
	ID						STRING
	, CERT_ID				STRING
	, CUST_NAME 			STRING
	, CREATE_DATE			STRING
	, CHANNEL_CODE 			STRING
	, LOAN_PROD 			STRING
	, CREDIT_TYPE 			STRING
	, REPAYMENT_TYPE 		STRING
	, TERM 					INT
	, RATE 					DOUBLE
	, CREDIT_AMOUNT 		DOUBLE
	, FROZEN_AMOUNT 		DOUBLE
	, SOURCE_TYPE 			STRING
	, MANUAL_VERIFIED 		STRING
	, VERIFIED_STATUS 		STRING
	, EXPIRE_DATE 			STRING
	, ACTIVATED 			STRING
	, ACTIVED_DATE 			STRING
	, VALIDATE_DATE 		STRING
	, OPERATOR_ID 			STRING
	, REASON 				STRING
	, REMARK				STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.customer_credit_apply_log(
	ID						STRING
	, CERT_ID 				STRING
	, CHANNEL_CODE 			STRING
	, CREATE_DATE 			STRING
	, CREDIT_TYPE 			STRING
	, CUST_NAME 			STRING
	, DEVICE_ID 			STRING
	, MOBILE_NO 			STRING
	, LOAN_PROD_CODE 		STRING
	, REMOTE_IP 			STRING
	, LBS_ADRESS 			STRING
	, OPENID 				STRING
	, CERT_OCR_MODIFY 		STRING
	, DEVICE_TYPE 			STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_channel(
  ID 				STRING,
  CHANNEL_CODE 		STRING,
  CHANNEL_NAME 		STRING,
  CHANNEL_TYPE 		INT,
  ENABLED 			INT,
  REMARK 			STRING,
  PRIORITY 			INT 	COMMENT '渠道优先级,数字越大优先级越高',
  NOTIFY_TYPE 		STRING  COMMENT '1-短信通知;2-微信通知;3-不限制; 9-不通知'
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_riskcontrol.inf_loan_product(
  ID 				STRING,
  LOAN_PROD_CODE 	STRING,
  PRODUCT_NAME 		STRING,
  REMARK 			STRING,
  REPAYMENT_TYPE 	STRING,
  RATE 				DOUBLE,
  CREDIT_TYPE 		STRING 	COMMENT '授信类型 C-现金类型 S-场景类型',
  ENABLED 			INT 	COMMENT '渠道优先级,数字越大优先级越高',
  AUTO_ACTIVED 		STRING 	COMMENT '产品是否自动激活,0-手动激活, 1-自动激活'
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;


-- 覆写数据
insert overwrite table bak_riskcontrol.inf_customer partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.inf_customer t
;
insert overwrite table bak_riskcontrol.inf_customer_credit partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.inf_customer_credit t
;
insert overwrite table bak_riskcontrol.his_customer_credit partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.his_customer_credit t
;
insert overwrite table bak_riskcontrol.customer_credit_apply_log partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.customer_credit_apply_log t
;
insert overwrite table bak_riskcontrol.inf_credit_frozen partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.inf_credit_frozen t
;
insert overwrite table bak_riskcontrol.inf_credit_unfrozen partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.inf_credit_unfrozen t
;
insert overwrite table bak_riskcontrol.customer_blacklist partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.customer_blacklist t
;
insert overwrite table bak_riskcontrol.call_ack_task partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.call_ack_task t
;
insert overwrite table bak_riskcontrol.inf_baiqishi partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.inf_baiqishi t
;
insert overwrite table bak_riskcontrol.inf_tongdun partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.inf_tongdun t
;
insert overwrite table bak_riskcontrol.inf_tongdun_education partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.inf_tongdun_education t
;
insert overwrite table bak_riskcontrol.inf_cs_insurance partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.inf_cs_insurance t
;
insert overwrite table bak_riskcontrol.inf_cs_accum_found partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.inf_cs_accum_found t
;
insert overwrite table bak_riskcontrol.inf_zmxy partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_riskcontrol.inf_zmxy t
;

insert overwrite table bak_riskcontrol.inf_channel select t.* from kudu_riskcontrol.inf_channel t
;
insert overwrite table bak_riskcontrol.inf_loan_product select t.* from kudu_riskcontrol.inf_loan_product t
;

