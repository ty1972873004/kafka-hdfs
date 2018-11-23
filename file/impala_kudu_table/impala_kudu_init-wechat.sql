-- # 【创建数据库】
create database if not exists kudu_wechat;

-- # 【删除表】
drop table if exists kudu_wechat.wechatmember;
drop table if exists kudu_wechat.loan_user_info;
drop table if exists kudu_wechat.his_loan_user_info;
drop table if exists kudu_wechat.customer_login_record;
drop table if exists kudu_wechat.customer_verify_record;
drop table if exists kudu_wechat.feedback_record;

-- # 【创建表】
CREATE TABLE if not exists kudu_wechat.wechatmember (
  ID 					STRING,
  CITY 					STRING,
  COUNTRY 				STRING,
  HEADIMGURL 			STRING,
  LANGUAGE 				STRING,
  LASTUPDATE 			STRING,
  NICKNAME 				STRING,
  OPENID 				STRING,
  PROVINCE 				STRING,
  SEX 					STRING,
  SUBSCRIBE 			INT,
  SUBSCRIBE_TIME		BIGINT,
  UNIONID 				STRING,
  BIGDATA_DEL_STATUS	INT,
  BIGDATA_SYNC_TIME		BIGINT,
  PRIMARY KEY(ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;


CREATE TABLE if not exists kudu_wechat.customer_login_record (
  ID 					STRING,
  CREATEDATE 			STRING,
  MOBILE 				STRING,
  OPENID 				STRING,
  BIGDATA_DEL_STATUS	INT,
  BIGDATA_SYNC_TIME		BIGINT,
  PRIMARY KEY(ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_wechat.his_loan_user_info (
  ID 					STRING,
  CREATEDATE 			STRING,
  CREATEUSER 			STRING,
  MODIFYDATE 			STRING,
  MODIFYUSER 			STRING,
  VERSION 				BIGINT,
  CERTID 				STRING,
  CUSTNAME 				STRING,
  MOBILE 				STRING,
  OPENID 				STRING,
  OPERTIME 				STRING,
  BIGDATA_DEL_STATUS	INT,
  BIGDATA_SYNC_TIME		BIGINT,
  PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_wechat.loan_user_info (
  ID 					STRING,
  CERTID 				STRING,
  CERTTYPE 				STRING,
  CUSTNAME 				STRING,
  LENDACCOUNT 			STRING,
  LOANCODE 				STRING,
  MOBILE 				STRING,
  OPENID 				STRING,
  OPERTIME 				STRING,
  USERFLAG 				STRING,
  USERSTATE 			STRING,
  FLOWSTATE 			INT,
  CREATEDATE 			STRING,
  VERSION 				INT,
  BIGDATA_DEL_STATUS	INT,
  BIGDATA_SYNC_TIME		BIGINT,
  PRIMARY KEY(ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_wechat.customer_verify_record (
  ID 					STRING,
  CREATEDATE 			STRING,
  CREATEUSER 			STRING,
  MODIFYDATE 			STRING,
  MODIFYUSER 			STRING,
  VERSION 				INT,
  CERTID 				STRING,
  CUSTNAME 				STRING,
  DESCRIPTION 			STRING,
  LENDACCOUNT 			STRING,
  OPENID 				STRING,
  REGISTERMOBILE 		STRING,
  RESERVEDMOBILE 		STRING,
  BIGDATA_DEL_STATUS	INT,
  BIGDATA_SYNC_TIME		BIGINT,
  PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;

CREATE TABLE if not exists kudu_wechat.feedback_record (
  ID 					STRING,
  BROWSERENTITY 		STRING,
  BROWSERMAJOR 			STRING,
  BROWSERNAME 			STRING,
  BROWSERVERSION 		STRING,
  CREATETIME 			STRING,
  DEVICEENTITY 			STRING,
  DEVICEMODEL 			STRING,
  DEVICEVENDOR 			STRING,
  EXCEPTDESCRIPTION 	STRING,
  OPENID 				STRING,
  OSENTITY 				STRING,
  OSNAME 				STRING,
  OSVERSION 			STRING,
  USERAGENT 			STRING,
  TRANSTYPE 			STRING,
  EVENTTYPE 			STRING,
  BIGDATA_DEL_STATUS	INT,
  BIGDATA_SYNC_TIME		BIGINT,
  PRIMARY KEY(ID)
) 
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;
