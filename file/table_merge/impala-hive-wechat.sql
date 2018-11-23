
-- 【增量更新所有有数据变更的分区的所有数据】

-- 更新【wechatmember】
insert overwrite table bak_wechat.wechatmember partition(subscribe_dt_id)
select 
t.*, cast(from_timestamp(from_unixtime(cast(t.subscribe_time/1000 as bigint), 'yyyy-MM-dd HH:mm:ss'), 'yyyyMM') as BIGINT) as DT_ID
from kudu_wechat.wechatmember t
where
from_timestamp(from_unixtime(cast(t.subscribe_time/1000 as bigint), 'yyyy-MM-dd HH:mm:ss'), 'yyyyMM') -- 只更新有数据更新的分区，这样可可以减少很大一部分重复数据覆写情况 
in
(
    select distinct from_timestamp(from_unixtime(cast(t1.subscribe_time/1000 as bigint), 'yyyy-MM-dd HH:mm:ss'), 'yyyyMM') 
    from kudu_wechat.wechatmember t1
    where t1.bigdata_sync_time >= unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【loan_user_info】
insert overwrite table bak_wechat.loan_user_info partition(create_dt_id)
select t.*, cast(replace(substr(t.createdate, 1, 7), '-', '') as bigint) as DT_ID
from kudu_wechat.loan_user_info t
where substr(t.createdate, 1, 7) in
(
    select distinct substr(t1.createdate, 1, 10)
    from kudu_wechat.loan_user_info t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【his_loan_user_info】
insert overwrite table bak_wechat.his_loan_user_info partition(create_dt_id)
select t.*, cast(replace(substr(t.createdate, 1, 7), '-', '') as bigint) as DT_ID
from kudu_wechat.his_loan_user_info t
where substr(t.createdate, 1, 7) in
(
    select distinct substr(t1.createdate, 1, 10)
    from kudu_wechat.his_loan_user_info t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【customer_login_record】，拉链表
insert overwrite table bak_wechat.customer_login_record partition(create_dt_id)
select t.*, cast(replace(substr(t.createdate, 1, 7), '-', '') as bigint) as DT_ID
from kudu_wechat.customer_login_record t
where substr(t.createdate, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【customer_verify_record】
insert overwrite table bak_wechat.customer_verify_record partition(create_dt_id)
select t.*, cast(replace(substr(t.createdate, 1, 7), '-', '') as bigint) as DT_ID
from kudu_wechat.customer_verify_record t
where substr(t.createdate, 1, 7) in
(
    select distinct substr(t1.createdate, 1, 10)
    from kudu_wechat.customer_verify_record t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【feedback_record】，拉链表
insert overwrite table bak_wechat.feedback_record partition(create_dt_id)
select t.*, cast(replace(substr(t.createtime, 1, 7), '-', '') as bigint) as DT_ID
from kudu_wechat.feedback_record t
where substr(t.createtime, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;
