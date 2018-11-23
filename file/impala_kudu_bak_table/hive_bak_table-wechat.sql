-- # 【创建数据库】
create database if not exists bak_wechat;

-- # 【删除表】
drop table if exists bak_wechat.wechatmember;
drop table if exists bak_wechat.loan_user_info;
drop table if exists bak_wechat.his_loan_user_info;
drop table if exists bak_wechat.customer_login_record;
drop table if exists bak_wechat.customer_verify_record;
drop table if exists bak_wechat.feedback_record;

-- # 【创建表】
CREATE TABLE if not exists bak_wechat.wechatmember (
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
  BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(SUBSCRIBE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;


CREATE TABLE if not exists bak_wechat.customer_login_record (
  ID 					STRING,
  CREATEDATE 			STRING,
  MOBILE 				STRING,
  OPENID 				STRING,
  BIGDATA_DEL_STATUS	INT,
  BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_wechat.his_loan_user_info (
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
  BIGDATA_SYNC_TIME		BIGINT
)
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_wechat.loan_user_info (
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
  BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_wechat.customer_verify_record (
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
  BIGDATA_SYNC_TIME		BIGINT
)
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

CREATE TABLE if not exists bak_wechat.feedback_record (
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
  BIGDATA_SYNC_TIME		BIGINT
) 
partitioned by(CREATE_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;


-- 覆写数据
insert overwrite table bak_wechat.wechatmember partition(SUBSCRIBE_DT_ID)
select t.*, cast(from_timestamp(from_unixtime(cast(t.subscribe_time/1000 as bigint), 'yyyy-MM-dd HH:mm:ss'), 'yyyyMM') as BIGINT) as DT_ID
from kudu_wechat.wechatmember t
;
insert overwrite table bak_wechat.loan_user_info partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATEDATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_wechat.loan_user_info t
;
insert overwrite table bak_wechat.his_loan_user_info partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATEDATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_wechat.his_loan_user_info t
;
insert overwrite table bak_wechat.customer_login_record partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATEDATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_wechat.customer_login_record t
;
insert overwrite table bak_wechat.customer_verify_record partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATEDATE, 'yyyyMM') as BIGINT) as DT_ID
from kudu_wechat.customer_verify_record t
;
insert overwrite table bak_wechat.feedback_record partition(CREATE_DT_ID)
select t.*, cast(from_timestamp(t.CREATETIME, 'yyyyMM') as BIGINT) as DT_ID
from kudu_wechat.feedback_record t
;