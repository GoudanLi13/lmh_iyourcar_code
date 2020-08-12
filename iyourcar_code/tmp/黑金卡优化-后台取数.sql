-- 第一次被赠送黑金的用户

CREATE TABLE IF NOT EXISTS `tmp.new_user_free_vcard_first_privilege` (
  `uid` STRING COMMENT '被赠送的用户uid'
,`activity_id` INT COMMENT '活动ID'
)
COMMENT '通过赠送黑金卡首次获得黑金卡的用户'
PARTITIONED BY (`d` STRING COMMENT '赠送日期 eg 2020-06-05')
CLUSTERED BY (`uid`) INTO 32 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '`'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS ORCFILE
TBLPROPERTIES ('creator'='liminghao', 'create_time'='{{ ds }}');

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
insert into table tmp.new_user_free_vcard_first_privilege
PARTITION (d)
select vcard.uid as uid,vcard.source as source,substr(vcard.starttime,0,10) as d
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard as vcard
join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user as users
on vcard.uid=users.uid and substr(vcard.starttime,0,10)=substr(users.member_time,0,10)
distribute by d;

select *
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard
order by starttime limit 10;

---------
drop table iyourcar_dw.rpt_ycyh_service_day_privilege_user_vcard_statistics;
CREATE TABLE IF NOT EXISTS `iyourcar_dw.rpt_ycyh_service_day_privilege_user_vcard_statistics`
(
    `id`                       BIGINT COMMENT 'id',
    `exposures_num`            BIGINT COMMENT '曝光人数',
    `receive_num`              BIGINT COMMENT '点击马上领取人数',
    `receive_success_num`      BIGINT COMMENT '成功领取人数',
    `use_oil_num`              BIGINT COMMENT '使用加油特权人数',
    `consumption_mall_num`     BIGINT COMMENT '商城消费人数',
    `source_id`                BIGINT COMMENT '来源主键',
    `createtime`               TIMESTAMP COMMENT '创建时间',
    `updatetime`               TIMESTAMP COMMENT '更新時間',
    `receive_success_new_num`  BIGINT COMMENT '第一次领卡的用户数',
    `use_oil_new_num`          BIGINT COMMENT '第一次领卡且使用加油服务的用户数',
    `consumption_mall_new_num` BIGINT COMMENT '第一次领卡且有在商城消费的用户数',
    `forever_renewal_num`      BIGINT COMMENT '领卡以后续费永久会员的用户数',
    `forever_renewal_new_num`  BIGINT COMMENT '第一次领卡且领卡以后续费成永久会员的用户数'
)
    COMMENT '电子卡来源数据统计表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    LOCATION '/data/bigdata/hive/warehouse/iyourcar_dw.db/rpt_ycyh_service_day_privilege_user_vcard_statistics'
    TBLPROPERTIES ('creator' = 'liminghao', 'create_time' = '{{ ds }}');


with exposure as (
       select activity_id,count(distinct case when exposures_num is not null then cid end) as `exposures_num` from tmp.new_user_free_vcard_cid_exposure_click
       group by activity_id
    ),
     click as (
        select activity_id, count(distinct case when click_num !='0' then cid end) as `receive_num` from tmp.new_user_free_vcard_cid_exposure_click
         group by activity_id
    ),
     privilege_user_vcard as (
         select source,count(distinct uid) as `receive_success_num`
         from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard
         group by source
     ),
     privilege_oil_order as (
         select vcard.source,count(distinct vcard.uid) as `use_oil_num`
         from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_oil_order as oil
         inner join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user as pri_user
         on pri_user.phone = oil.phone
         inner join
         iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard as vcard
         on pri_user.uid = vcard.uid
         where oil.pay_order_time > vcard.createtime
         group by vcard.source
     ),
     consume as (
         select vcard.source,count(distinct orders.uid ) as `consumption_mall_num`
         from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
         inner join
             (select * from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard) as vcard
         on orders.uid = vcard.uid
         where orders.order_status in (1,2,3,5) and orders.ordertime > vcard.createtime
         group by vcard.source
     ),
     new_user_privilege_user_vcard as (
         select source,count(distinct vcard.uid) as `receive_success_num`
         from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard as vcard
         join tmp.new_user_free_vcard_first_privilege as new_user
         on vcard.uid=new_user.uid and vcard.source=new_user.activity_id
         group by source
     ),
     new_user_privilege_oil_order as (
         select vcard.source,count(distinct vcard.uid) as `use_oil_num`
         from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_oil_order as oil
         inner join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user as pri_user
         on pri_user.phone = oil.phone
         inner join
             (select * from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard )as vcard
         on pri_user.uid = vcard.uid
         join tmp.new_user_free_vcard_first_privilege as new_user
         on vcard.uid=new_user.uid and vcard.source=new_user.activity_id
         where oil.pay_order_time > vcard.createtime
         group by vcard.source
     ),
     new_user_consume as (
         select vcard.source,count(distinct orders.uid ) as `consumption_mall_num`
         from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
         inner join
             (select * from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard) as vcard
         on orders.uid = vcard.uid
         join tmp.new_user_free_vcard_first_privilege as new_user
         on vcard.uid=new_user.uid and vcard.source=new_user.activity_id
         where orders.order_status in (1,2,3,5) and orders.ordertime > vcard.createtime
         group by vcard.source
     ),
     forever_renewal as (
         select source,count(distinct pri_user.uid) as `forever_renewal_user`
         from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard as vcard
         join (select * from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user where member_type=1) as pri_user
         on pri_user.uid=vcard.uid
         group by source
     ),
     new_user_forever_renewal as (
         select source,count(distinct pri_user.uid) as `new_forever_renewal_user`
         from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard as vcard
         join tmp.new_user_free_vcard_first_privilege as new_user
         on vcard.uid=new_user.uid and vcard.source=new_user.activity_id
         join (select * from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user where member_type=1) as pri_user
         on pri_user.uid=vcard.uid
         group by source
     )

insert overwrite table iyourcar_dw.rpt_ycyh_service_day_privilege_user_vcard_statistics
select exposure.activity_id as id,
   exposure.exposures_num,
   click.receive_num,
   privilege_user_vcard.receive_success_num,
   privilege_oil_order.use_oil_num,
   consume.consumption_mall_num,
   exposure.activity_id,
   CURRENT_TIMESTAMP as createtime,
   CURRENT_TIMESTAMP as updatetime,
   new_user_privilege_user_vcard.receive_success_num,
    new_user_privilege_oil_order.use_oil_num,
    new_user_consume.consumption_mall_num,
    forever_renewal.forever_renewal_user,
    new_user_forever_renewal.new_forever_renewal_user
from exposure
         left join click
    on exposure.activity_id=click.activity_id
    left join privilege_user_vcard
    on exposure.activity_id=privilege_user_vcard.source
    left join privilege_oil_order
    on privilege_oil_order.source=exposure.activity_id
    left join consume
    on consume.source=exposure.activity_id
    left join new_user_privilege_user_vcard
    on new_user_privilege_user_vcard.source=exposure.activity_id
    left join new_user_privilege_oil_order
    on new_user_privilege_oil_order.source=exposure.activity_id
    left join new_user_consume
    on new_user_consume.source=exposure.activity_id
    left join forever_renewal
    on forever_renewal.source=exposure.activity_id
    left join new_user_forever_renewal
    on new_user_forever_renewal.source=exposure.activity_id;

------
dag_id: privilege_user_vcard_statistics
description: 电子卡来源数据统计表 # 调度周期改为不自动调度
schedule_interval: 30 6 * * *
default_args:
  owner: liurishen
  # 重试时间改为10分支后重试
  retry_delay: {minutes: 10}
tasks:
  privilege_user_vcard_statistics_sqoop:
    operator: SqoopOperator
    sources: iyourcar_dw.rpt_ycyh_service_day_privilege_user_vcard_statistics
    conn_id: sqoop-iyourcar_activity
    table: privilege_user_vcard_statistics
    cmd_type: export
    export_dir: /data/bigdata/hive/warehouse/iyourcar_dw.db/rpt_ycyh_service_day_privilege_user_vcard_statistics
    properties: {'mapreduce.job.queuename': 'dailyDay'}
    extra_export_options: {'num-mappers': '1','update-key': 'id','columns':'id,exposure,receive_num,receive_success_num,use_oil_num,consumption_mall_num,source_id,createtime,updatetime,recevice_success_new_num,use_oil_new_num,consumption_mall_new_num,forever_renewal_num,forever_renewal_new_num', 'update-mode': 'allowinsert', 'fields-terminated-by': '\001','null-string' : '"\\\\N"','input-null-string' : '"\\\\N"','input-null-non-string' : '"\\\\N"'}

-----
-- 赠送黑金卡弹窗点击曝光
CREATE TABLE IF NOT EXISTS `tmp.new_user_free_vcard_cid_exposure_click` (
  `cid` STRING COMMENT '用户cid'
, `exposures_num` INT COMMENT '弹窗曝光次数'
, `click_num` INT COMMENT '弹窗点击次数'
,`activity_id` INT COMMENT '活动ID'
)
COMMENT '赠送黑金卡弹窗点击曝光'
PARTITIONED BY (`d` STRING COMMENT '日期 eg 2019-10-28')
CLUSTERED BY (`cid`) INTO 32 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '`'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS ORCFILE
TBLPROPERTIES ('creator'='liminghao', 'create_time'='{{ ds }}');

insert overwrite table `tmp.new_user_free_vcard_cid_exposure_click` partition (d = '{{ ds }}')
select
    cid,
    count( case when a = 's' then cid else null end) as `exposures_num`,
    count( case when a = 'e' then cid else null end) as `click_num`,
    get_json_object(args,"$.activity_freecard") as activity_id
        from iyourcar_dw.dwd_all_action_hour_log where d = '{{ ds }}' and id in ('12008','12009','12010','12011',
        '12012','12013','12014','12015')
        group by cid,get_json_object(args,"$.activity_freecard") ;

-- 第一次被赠送黑金的用户
CREATE TABLE IF NOT EXISTS `tmp.new_user_free_vcard_first_privilege` (
  `uid` STRING COMMENT '被赠送的用户uid'
,`activity_id` INT COMMENT '活动ID'
)
COMMENT '通过赠送黑金卡首次获得黑金卡的用户'
PARTITIONED BY (`d` STRING COMMENT '赠送日期 eg 2020-06-05')
CLUSTERED BY (`uid`) INTO 32 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '`'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS ORCFILE
TBLPROPERTIES ('creator'='liminghao', 'create_time'='{{ ds }}');

--每日更新
insert overwrite table `tmp.new_user_free_vcard_first_privilege` partition (d = '{{ ds }}')
select vcard.uid as uid,vcard.source as source
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard as vcard
join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user as users
on vcard.uid=users.uid and substr(vcard.starttime,0,10)=substr(users.member_time,0,10)
where substr(vcard.starttime,0,10)='{{ ds }}';

---------------------------------------------------------
--黑金卡赠送优化分析
--1.用户抽离
--1.1.8月6日-8月12日没有被赠送黑金卡的非黑金卡用户
drop table tmp.lmh_unpri_users_0812;
create table if not exists tmp.lmh_unpri_users_0812 as
    select log.cid,maps.uid
    from
    (select distinct cid
    from iyourcar_dw.dws_behavior_day_device_active
    where d between '2020-08-06' and '2020-08-11'
    and ctype in(1,2,4)
    and cname in('APP_SUV','WXAPP_YCYH_PLUS')) as log
    left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
    on maps.cid=log.cid
    left join
    (
        select uid
        from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user
        where is_member in(1,2)
        and member_type=2
        ) as forever_pri
    on maps.uid=forever_pri.uid
    left join
    (
        select distinct uid
        from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard
        where substr(starttime,0,10) between '2020-08-06' and '2020-08-11'
        or substr(endtime,0,10) >= '2020-08-06'
        ) as vcard
    on maps.uid=vcard.uid
where forever_pri.uid is null and vcard.uid is null;

--1.2.抽离6.11-6.17有曝光的赠卡用户
drop table tmp.lmh_old_pri_users_0812;
create table if not exists tmp.lmh_old_pri_users_0812 as
select log.*,maps.uid
from
(select distinct cid,d
from iyourcar_dw.dwd_all_action_hour_log
    where d between '2020-06-21' and '2020-06-27'
    and id in(12008,12009,12010)) as log
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
    on maps.cid=log.cid
 join
(select uid,substr(starttime,0,10) as d
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard
where substr(starttime,0,10) between '2020-06-21' and '2020-06-27') as vcard
on vcard.uid=maps.uid and vcard.d=log.d
;

select count(*) from tmp.lmh_old_pri_users_0812;

--1.3把8月6日-8月12日的用户抽离
drop table tmp.lmh_new_pri_users_0812;
create table if not exists tmp.lmh_new_pri_users_0812 as
    select log.*,vcard.uid,
           case when exposure.cid is null then 1
                when exposure.cid is not null and click.cid is null then 2
                when click.cid is not null then 3 end as level,
            exposure.source_type,
           vcard.source
    from
    (select distinct cid,d
    from iyourcar_dw.dws_behavior_day_device_active
    where d between '2020-08-06' and '2020-08-11'
    and ctype in(1,2,4)
    and cname in('APP_SUV','WXAPP_YCYH_PLUS')) as log
    join iyourcar_dw.dws_extend_day_cid_map_uid as maps
    on maps.cid=log.cid
    join
    (
        select distinct uid,substr(starttime,0,10) as d,source
        from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard
        where substr(starttime,0,10) between '2020-08-06' and '2020-08-11'
        ) as vcard
    on maps.uid=vcard.uid and vcard.d=log.d
    left join
    (
        select cid,d,source_type
        from
        (select cid,d,get_json_object(args,'$.blackcard_source_type') as source_type,row_number() over (partition by cid,d order by st) as rank
        from iyourcar_dw.dwd_all_action_hour_log
        where d between '2020-08-06' and '2020-08-11'
        and id in(12902,12903,12904)) as a
        where rank=1
        ) as exposure
    on exposure.cid=log.cid and exposure.d=vcard.d
    left join
    (

        select distinct cid,d
        from iyourcar_dw.dwd_all_action_hour_log
        where d between '2020-08-06' and '2020-08-11'
        and id in(12906,12907,12908)
        ) as click
    on click.cid=log.cid;

--2.各类型用户7日转化率
--2.1. 上一次赠卡的用户
select
count(distinct a.uid),
count(distinct case when b.uid is not null and a.d<=b.d then b.uid end)
from tmp.lmh_old_pri_users_0812 as a
left join
(
    select uid,substr(ordertime,0,10) as d
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-06-21' and '2020-06-27'
    and all_price>0
    and biz_type in(1,3)
    and order_status in(1,2,3)
    and mall_type=1
    ) as b
on a.uid=b.uid;

--2.2.普通用户
select
count(distinct a.cid),
count(distinct b.uid)
from tmp.lmh_unpri_users_0812 as a
left join
(
    select uid,substr(ordertime,0,10) as d
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-08-06' and '2020-08-11'
    and all_price>0
    and biz_type in(1,3)
    and order_status in(1,2,3)
    and mall_type=1
    ) as b
on a.uid=b.uid;

--2.3.优化赠送后用户
select
a.source,a.source_type,a.level,
count(distinct a.cid),
count(distinct case when b.uid is not null and a.d<=b.d then b.uid end)
from tmp.lmh_new_pri_users_0812 as a
left join
(
    select uid,substr(ordertime,0,10) as d
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-08-06' and '2020-08-11'
    and all_price>0
    and biz_type in(1,3)
    and order_status in(1,2,3)
    and mall_type=1
    ) as b
on a.uid=b.uid
group by a.source,a.source_type,a.level;

--3.用户获得卡后进入用户占比

