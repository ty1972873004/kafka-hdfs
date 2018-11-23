-- # 【创建数据库】
create database if not exists bak_isop;

-- # 【删除表】
drop table if exists bak_isop.isop_employee;
drop table if exists bak_isop.isop_department;
drop table if exists bak_isop.customer;
drop table if exists bak_isop.customer_account;
drop table if exists bak_isop.inf_customer_contact;
drop table if exists bak_isop.inf_customer_base;
drop table if exists bak_isop.inf_cust_address;
drop table if exists bak_isop.inf_customer_occupation;

-- # 【创建表】
CREATE TABLE if not exists bak_isop.isop_employee(
	ID		 				STRING
	, EMPLOYEENO 			STRING
	, NAME					STRING
	, DEPARTMENT_ID			INT
	, CREATEDATE			STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_isop.isop_department(
	ID 						INT
	, NAME					STRING
	, CREATEDATE			STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_isop.customer(
	CUST_ID					BIGINT
	, CERT_ID				STRING
	, CUST_NAME				STRING
	, MOBILE_NO				STRING
	, CUSTOMER_SOURCE		INT
	, CREATEDATE			STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_isop.customer_account(
	ACCT_ID 				BIGINT
	, ACCT_DIRECT 			INT
	, ACCT_NO 				STRING
	, ACCT_TYPE 			INT
	, CREATEDATE 			STRING
	, CREATEUSER 			STRING
	, DEFAULT_ACCT_FLAG 	INT
	, MODIFYDATE 			STRING
	, MODIFYUSER 			STRING
	, CUST_ID 				BIGINT
	, BANK_MOBILE_NO 		STRING
	, DELETE_FLAG 			STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_isop.inf_customer_contact(
	ID			 			STRING
	, CONTACT_NAME 			STRING
	, CONTACT_RELATION 		STRING
	, CREATEDATE 			STRING
	, CREATEUSER 			STRING
	, CONTACT_MOBILE_NO 	STRING
	, MODIFYDATE 			STRING
	, MODIFYUSER 			STRING
	, CUST_ID 				BIGINT
	, bigdata_del_status	INT
	, bigdata_sync_time		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_isop.inf_customer_base(
	CUST_ID 				BIGINT
	, ADDRESS 				STRING
	, BIRTH_DATE 			STRING
	, CERT_ID 				STRING
	, CERT_TYPE 			STRING
	, COMPANY_TEL 			STRING
	, EMAIL 				STRING
	, HIGHEST_DEGREE 		INT
	, HIGHEST_EDUCATION 	INT
	, HOUSEHOLD_ADDRESS 	STRING
	, HOUSE_TEL 			STRING
	, MARITAL_STATUS 		INT
	, MOBILE_NO 			STRING
	, OPER_ID 				STRING
	, POSTAL_CODE 			STRING
	, SEX 					INT
	, SPOUSE_COMPANY 		STRING
	, SPOUSE_IDNO 			STRING
	, SPOUSE_IDTYPE 		STRING
	, SPOUSE_NAME 			STRING
	, SPOUSE_TEL 			STRING
	, CUST_NAME 			STRING
	, CREATE_DATE 			STRING
	, MOD_DATE 				STRING
	, NEED_SUPPLEMENT 		INT
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_isop.inf_cust_address(
	CUST_ID 			BIGINT
	, ADRESS_STATUS 	INT
	, CERT_ID 			STRING
	, CERT_TYPE 		STRING
	, HOME_ADRESS 		STRING
	, OPER_ID 			STRING
	, POASTAL_CODE 		STRING
	, CREATE_DATE 		STRING
	, MOD_DATE 			STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_isop.inf_customer_occupation(
	CUST_ID 				BIGINT
	, ANNUAL_INCOME 		DOUBLE
	, CERT_ID 				STRING
	, CERT_TYPE 			STRING
	, COMPANY 				STRING
	, COMPANY_ADRESS 		STRING
	, COMPANY_POATAL_CODE 	STRING
	, DUTY 					INT
	, INDUSTRY 				STRING
	, OCCUPTATION_NAME 		STRING
	, OPER_ID 				STRING
	, POSITIONAL_TITLE 		INT
	, SALARY_ACCOUNT 		STRING
	, SALARY_BANK_CODE 		STRING
	, START_DATE 			STRING
	, CREATE_DATE 			STRING
	, MOD_DATE 				STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;


-- 覆写数据
insert overwrite table bak_isop.isop_employee partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATEDATE, 'yyyyMM') as BIGINT) as CREATE_DT_ID
from kudu_isop.isop_employee t
;
insert overwrite table bak_isop.isop_department partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATEDATE, 'yyyyMM') as BIGINT) as CREATE_DT_ID
from kudu_isop.isop_department t
;
insert overwrite table bak_isop.customer partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATEDATE, 'yyyyMM') as BIGINT) as CREATE_DT_ID
from kudu_isop.customer t
;
insert overwrite table bak_isop.customer_account partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATEDATE, 'yyyyMM') as BIGINT) as CREATE_DT_ID
from kudu_isop.customer_account t
;
insert overwrite table bak_isop.inf_customer_contact partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATEDATE, 'yyyyMM') as BIGINT) as CREATE_DT_ID
from kudu_isop.inf_customer_contact t
;
insert overwrite table bak_isop.inf_customer_base partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as CREATE_DT_ID
from kudu_isop.inf_customer_base t
;
insert overwrite table bak_isop.inf_cust_address partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as CREATE_DT_ID
from kudu_isop.inf_cust_address t
;
insert overwrite table bak_isop.inf_customer_occupation partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATE_DATE, 'yyyyMM') as BIGINT) as CREATE_DT_ID
from kudu_isop.inf_customer_occupation t
;
