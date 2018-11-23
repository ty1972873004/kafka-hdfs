
-- 【增量更新所有有数据变更的分区的所有数据】

-- 更新【inf_customer】
insert overwrite table bak_riskcontrol.inf_customer partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.inf_customer t
where substr(t.CREATE_DATE, 1, 7) in
(
    select distinct substr(t1.CREATE_DATE, 1, 10)
    from kudu_riskcontrol.inf_customer t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【inf_customer_credit】
insert overwrite table bak_riskcontrol.inf_customer_credit partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.inf_customer_credit t
where substr(t.CREATE_DATE, 1, 7) in
(
    select distinct substr(t1.CREATE_DATE, 1, 10)
    from kudu_riskcontrol.inf_customer_credit t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【his_customer_credit】，拉链表
insert overwrite table bak_riskcontrol.his_customer_credit partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.his_customer_credit t
where substr(t.CREATE_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【customer_credit_apply_log】，拉链表
insert overwrite table bak_riskcontrol.customer_credit_apply_log partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.customer_credit_apply_log t
where substr(t.CREATE_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【inf_credit_frozen】，拉链表
insert overwrite table bak_riskcontrol.inf_credit_frozen partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.inf_credit_frozen t
where substr(t.CREATE_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【inf_credit_unfrozen】，拉链表
insert overwrite table bak_riskcontrol.inf_credit_unfrozen partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.inf_credit_unfrozen t
where substr(t.CREATE_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【inf_zmxy】
insert overwrite table bak_riskcontrol.inf_zmxy partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.inf_zmxy t
where substr(t.CREATE_DATE, 1, 7) in
(
    select distinct substr(t1.CREATE_DATE, 1, 10)
    from kudu_riskcontrol.inf_zmxy t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【customer_blacklist】，拉链表
insert overwrite table bak_riskcontrol.customer_blacklist partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.customer_blacklist t
where substr(t.CREATE_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【call_ack_task】
insert overwrite table bak_riskcontrol.call_ack_task partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.call_ack_task t
where substr(t.CREATE_DATE, 1, 7) in
(
    select distinct substr(t1.CREATE_DATE, 1, 10)
    from kudu_riskcontrol.call_ack_task t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【inf_baiqishi】，拉链表
insert overwrite table bak_riskcontrol.inf_baiqishi partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.inf_baiqishi t
where substr(t.CREATE_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【inf_tongdun】，拉链表
insert overwrite table bak_riskcontrol.inf_tongdun partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.inf_tongdun t
where substr(t.CREATE_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【inf_tongdun_education】
insert overwrite table bak_riskcontrol.inf_tongdun_education partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.inf_tongdun_education t
where substr(t.CREATE_DATE, 1, 7) in
(
    select distinct substr(t1.CREATE_DATE, 1, 10)
    from kudu_riskcontrol.inf_tongdun_education t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【inf_cs_insurance】，拉链表
insert overwrite table bak_riskcontrol.inf_cs_insurance partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.inf_cs_insurance t
where substr(t.CREATE_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【inf_cs_accum_found】，拉链表
insert overwrite table bak_riskcontrol.inf_cs_accum_found partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_riskcontrol.inf_cs_accum_found t
where substr(t.CREATE_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;