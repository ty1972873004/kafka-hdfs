
-- 【增量更新所有有数据变更的分区的所有数据】

-- 更新【isop_employee】
insert overwrite table bak_isop.isop_employee partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATEDATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_isop.isop_employee t
where substr(t.CREATEDATE, 1, 7) in
(
    select distinct substr(t1.CREATEDATE, 1, 10)
    from kudu_isop.isop_employee t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【isop_department】
insert overwrite table bak_isop.isop_department partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATEDATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_isop.isop_department t
where substr(t.CREATEDATE, 1, 7) in
(
    select distinct substr(t1.CREATEDATE, 1, 10)
    from kudu_isop.isop_department t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【customer】
insert overwrite table bak_isop.customer partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATEDATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_isop.customer t
where substr(t.CREATEDATE, 1, 7) in
(
    select distinct substr(t1.CREATEDATE, 1, 10)
    from kudu_isop.customer t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【customer_account】
insert overwrite table bak_isop.customer_account partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATEDATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_isop.customer_account t
where substr(t.CREATEDATE, 1, 7) in
(
    select distinct substr(t1.CREATEDATE, 1, 10)
    from kudu_isop.customer_account t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【inf_customer_contact】
insert overwrite table bak_isop.inf_customer_contact partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATEDATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_isop.inf_customer_contact t
where substr(t.CREATEDATE, 1, 7) in
(
    select distinct substr(t1.CREATEDATE, 1, 10)
    from kudu_isop.inf_customer_contact t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【inf_customer_base】
insert overwrite table bak_isop.inf_customer_base partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_isop.inf_customer_base t
where substr(t.CREATE_DATE, 1, 7) in
(
    select distinct substr(t1.CREATE_DATE, 1, 10)
    from kudu_isop.inf_customer_base t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【inf_cust_address】
insert overwrite table bak_isop.inf_cust_address partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_isop.inf_cust_address t
where substr(t.CREATE_DATE, 1, 7) in
(
    select distinct substr(t1.CREATE_DATE, 1, 10)
    from kudu_isop.inf_cust_address t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【inf_customer_occupation】
insert overwrite table bak_isop.inf_customer_occupation partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_isop.inf_customer_occupation t
where substr(t.CREATE_DATE, 1, 7) in
(
    select distinct substr(t1.CREATE_DATE, 1, 10)
    from kudu_isop.inf_customer_occupation t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

