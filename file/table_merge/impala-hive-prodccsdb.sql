
-- 【增量更新所有有数据变更的分区的所有数据】

-- 更新【ccs_channel_info】
insert overwrite table bak_prodccsdb.ccs_channel_info partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_channel_info t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.ccs_channel_info t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【ccs_loan】
insert overwrite table bak_prodccsdb.ccs_loan partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_loan t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.ccs_loan t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【ccs_loan_reg】
insert overwrite table bak_prodccsdb.ccs_loan_reg partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_loan_reg t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.ccs_loan_reg t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【ccs_loan_reg_hst】
insert overwrite table bak_prodccsdb.ccs_loan_reg_hst partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_loan_reg_hst t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.ccs_loan_reg_hst t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【ccs_acct】
insert overwrite table bak_prodccsdb.ccs_acct partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_acct t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.ccs_acct t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【ccs_plan】
insert overwrite table bak_prodccsdb.ccs_plan partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_plan t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.ccs_plan t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【ccs_customer】
insert overwrite table bak_prodccsdb.ccs_customer partition(create_dt_id)
select t.*, cast(replace(substr(t.birthday, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_customer t
where substr(t.birthday, 1, 7) in
(
    select distinct substr(t1.birthday, 1, 10)
    from kudu_prodccsdb.ccs_customer t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【ccs_order】
insert overwrite table bak_prodccsdb.ccs_order partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_order t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.ccs_order t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【ccs_order_hst】
insert overwrite table bak_prodccsdb.ccs_order_hst partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_order_hst t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.ccs_order_hst t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【ccs_loan_bal_rpt】，拉链表
insert overwrite table bak_prodccsdb.ccs_loan_bal_rpt partition(BATCH_DT_ID)
select t.*, cast(replace(substr(t.BATCH_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_loan_bal_rpt t
where substr(t.BATCH_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【ccs_overdue_hst_rpt】
insert overwrite table bak_prodccsdb.ccs_overdue_hst_rpt partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.ccs_overdue_hst_rpt t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.ccs_overdue_hst_rpt t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【tpl_customer】
insert overwrite table bak_prodccsdb.tpl_customer partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.tpl_customer t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.tpl_customer t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【tpl_loan_info】
insert overwrite table bak_prodccsdb.tpl_loan_info partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.tpl_loan_info t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.tpl_loan_info t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【tpl_repay_info】，拉链表
insert overwrite table bak_prodccsdb.tpl_repay_info partition(BIZ_DT_ID)
select t.*, cast(replace(substr(t.BIZ_DATE, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.tpl_repay_info t
where substr(t.BIZ_DATE, 1, 7) between from_timestamp(date_add(now(), -1), 'yyyy-MM-dd') and from_timestamp(now(), 'yyyy-MM-dd')
;

-- 更新【tpl_trade_register】
insert overwrite table bak_prodccsdb.tpl_trade_register partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.tpl_trade_register t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.tpl_trade_register t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

-- 更新【tpl_txn_register】
insert overwrite table bak_prodccsdb.tpl_txn_register partition(create_dt_id)
select t.*, cast(replace(substr(t.CREATE_TIME, 1, 7), '-', '') as bigint) as DT_ID
from kudu_prodccsdb.tpl_txn_register t
where substr(t.CREATE_TIME, 1, 7) in
(
    select distinct substr(t1.CREATE_TIME, 1, 10)
    from kudu_prodccsdb.tpl_txn_register t1 
    where t1.bigdata_sync_time between unix_timestamp(date_add(from_timestamp(now(), 'yyyy-MM-dd'), -1)) * 1000
    and unix_timestamp(from_timestamp(now(), 'yyyy-MM-dd')) * 1000
)
;

