
-- 增量更新昨日分区所有数据
-- 更新【dict_card_bin】，拉链表
insert overwrite table bak_payment.dict_card_bin partition(sync_dt_id)
select t.*, cast(from_timestamp(from_unixtime(cast(t.bigdata_sync_time/1000 as bigint), 'yyyy-MM-dd HH:mm:ss'), 'yyyyMM') as BIGINT) as DT_ID
from kudu_payment.dict_card_bin t
where t.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), 0)) * 1000
    and unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), 1)) * 1000 
;

