﻿-- # 【创建数据库】
create database if not exists kudu_sit_ods_isop;

-- # 【删除表】
drop table if exists kudu_sit_ods_isop.isop_employee;
drop table if exists kudu_sit_ods_isop.isop_department;
drop table if exists kudu_sit_ods_isop.customer;
drop table if exists kudu_sit_ods_isop.customer_account;
drop table if exists kudu_sit_ods_isop.inf_customer_contact;
drop table if exists kudu_sit_ods_isop.inf_customer_base;
drop table if exists kudu_sit_ods_isop.inf_cust_address;
drop table if exists kudu_sit_ods_isop.inf_customer_occupation;

-- # 【创建表】
CREATE TABLE if not exists kudu_sit_ods_isop.isop_employee(
	ID		 				STRING
	, EMPLOYEENO 			STRING
	, NAME					STRING
	, DEPARTMENT_ID			INT
	, CREATEDATE			STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT,
	PRIMARY KEY(ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_sit_ods_isop.isop_department(
	ID 						INT
	, NAME					STRING
	, CREATEDATE			STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT,
	PRIMARY KEY(ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_sit_ods_isop.customer(
	CUST_ID					BIGINT
	, CERT_ID				STRING
	, CUST_NAME				STRING
	, MOBILE_NO				STRING
	, CUSTOMER_SOURCE		INT
	, CREATEDATE			STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT,
	PRIMARY KEY(CUST_ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_sit_ods_isop.customer_account(
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
	, BIGDATA_SYNC_TIME		BIGINT,
	PRIMARY KEY(ACCT_ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_sit_ods_isop.inf_customer_contact(
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
	, bigdata_sync_time		BIGINT,
	PRIMARY KEY(ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_sit_ods_isop.inf_customer_base(
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
	, BIGDATA_SYNC_TIME		BIGINT,
	PRIMARY KEY(CUST_ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_sit_ods_isop.inf_cust_address(
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
	, BIGDATA_SYNC_TIME		BIGINT,
	PRIMARY KEY(CUST_ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_sit_ods_isop.inf_customer_occupation(
	CUST_ID 				BIGINT
	, ANNUAL_INCOME 		DECIMAL(20, 6)
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
	, BIGDATA_SYNC_TIME		BIGINT,
	PRIMARY KEY(CUST_ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;
