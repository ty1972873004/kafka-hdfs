-- # 【创建数据库】
create database if not exists bak_payment;

-- # 【删除表】
drop table if exists bak_payment.dict_card_bin;

---------------【支付表】-------------------------
CREATE TABLE if not exists bak_payment.dict_card_bin(
	ID						STRING
	, BANK_CODE				STRING
	, BANK_NAME_SHORT		STRING
	, BANK_TYPE				STRING
	, BIN_LEN				INT
	, CARD_BIN_CODE			STRING
	, CARD_LEN				INT
	, CARD_NAME				STRING
	, CARD_TYPE				STRING
	, BIGDATA_DEL_STATUS	INT
	, BIGDATA_SYNC_TIME		BIGINT
)
partitioned by(SYNC_DT_ID BIGINT)
stored as parquet
tblproperties('parquet.compression'='SNAPPY')
;

-- 覆写数据
insert overwrite table bak_payment.dict_card_bin partition(SYNC_DT_ID)
select t.*, cast(from_timestamp(from_unixtime(cast(t.bigdata_sync_time/1000 as bigint), 'yyyy-MM-dd HH:mm:ss'), 'yyyyMM') as BIGINT) as CREATE_DT_ID
from kudu_payment.dict_card_bin t
;


