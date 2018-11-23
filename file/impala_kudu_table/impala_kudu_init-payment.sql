-- # 【创建数据库】
create database if not exists kudu_payment;

-- # 【删除表】
drop table if exists kudu_payment.dict_card_bin;


---------------【支付表】-------------------------
CREATE TABLE if not exists kudu_payment.dict_card_bin(
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
	, BIGDATA_SYNC_TIME		BIGINT,
	PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
;