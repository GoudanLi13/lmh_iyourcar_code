--安卓
--下拉用户数(新版的新老用户)
select
pull_user.d,
count(case when new_user.cid is null then pull_user.cid end) as `下拉老用户数`,
count(case when new_user.cid is not null then new_user.cid end) as `下拉新用户数`
from (select * from iyourcar_recommendation.rpt_recommend_stat_service_day_active_new_user_retention where ctype=2) as new_user
 right join
        (select distinct d,cid, uid
        from (
                 select d,
                        cid,
                        session,
                        uid,
                        count(distinct case
                                           when get_json_object(args, "$.rank") = 1
                                               then get_json_object(args, "$.gid") end) as num_rank_1,
                        count(case
                                  when get_json_object(args, "$.rank") >= 5
                                      then get_json_object(args, "$.gid") end) as num_rank_5
                 from iyourcar_dw.dwd_all_action_hour_log
                 where get_json_object(args, "$.gid") != '\N'
                   and get_json_object(args, "$.page_type") = 48
                   and args not like '%"is_from_notice_bar":1%'
                   and page_evt_id=141
                   and ctype = 2
                   and cname = 'APP_SUV'
                   and a = 's'
                   and c_ver >= 424000
                   and d between '2020-06-09' and '2020-06-22'
                   and (get_json_object(args, "$.rank") = 1 or get_json_object(args, "$.rank") >= 5)
                 group by d,cid,session,uid
             ) as t1
             where num_rank_1>1 or num_rank_5>0) as pull_user
        on pull_user.cid=new_user.cid and pull_user.d=new_user.d
group by pull_user.d
;


--下拉用户数(老版新老用户）
select
pull_user.d,
count(case when new_user.cid is null then pull_user.cid end) as `下拉老用户数`,
count(case when new_user.cid is not null then new_user.cid end) as `下拉新用户数`
from (select * from iyourcar_recommendation.rpt_recommend_stat_service_day_active_new_user_retention where ctype=2) as new_user
 right join
        (select distinct d,cid, uid
        from (
                 select d,
                        cid,
                        session,
                        uid,
                        count(distinct case
                                           when get_json_object(args, "$.rank") = 1
                                               then get_json_object(args, "$.gid") end) as num_rank_1,
                        count(case
                                  when get_json_object(args, "$.rank") >= 3
                                      then get_json_object(args, "$.gid") end) as num_rank_3
                 from iyourcar_dw.dwd_all_action_hour_log
                 where d between '2020-03-04' and '2020-06-03'
                   and get_json_object(args, "$.gid") != '\N'
                   and get_json_object(args, "$.page_type") = 48
                   and args not like '%"is_from_notice_bar":1%'
                   and page_evt_id=141
                   and ctype = 2
                   and cname = 'APP_SUV'
                   and a = 's'
                   and c_ver < 424000
                   and c_ver>=405000
                   and (get_json_object(args, "$.rank") = 1 or get_json_object(args, "$.rank") >= 3)
                 group by d,cid,session,uid
             ) as t1
             where num_rank_1>1 or num_rank_3>0) as pull_user
        on pull_user.cid=new_user.cid and pull_user.d=new_user.d
group by pull_user.d
;


--新版新用户DAU

select log.d                                                 as d,
        sum(case when new_user.cid is null then 1 else 0 end) as old_num,
        sum(case when new_user.cid is null then 0 else 1 end) as new_num
 from (select * from iyourcar_recommendation.rpt_recommend_stat_service_day_active_new_user_retention where ctype=2)as new_user
          right join
      (select distinct d, ctype, cid
       from iyourcar_dw.dwd_all_action_hour_log
       where
        d between '2020-06-09' and '2020-06-22'
        and ctype=2
        and c_ver >= 424000
        and page_evt_id=141
      ) as log
      on new_user.cid = log.cid and new_user.d = log.d
 group by log.d;

--老版首页DAU
select log.d                                                 as d,
        sum(case when new_user.cid is null then 1 else 0 end) as old_num,
        sum(case when new_user.cid is null then 0 else 1 end) as new_num
 from (select * from iyourcar_recommendation.rpt_recommend_stat_service_day_active_new_user_retention where ctype=2)as new_user
          right join
      (select distinct d, ctype, cid
       from iyourcar_dw.dwd_all_action_hour_log
       where
        d between '2020-03-04' and '2020-06-03'
        and ctype=2
        and c_ver < 424000
        and c_ver>=405000
        and page_evt_id=141
      ) as log
      on new_user.cid = log.cid and new_user.d = log.d
 group by log.d;

--7月6日-7月12日的搜索关键词和搜索的商品的spu
--每个搜索所曝光的名字
select distinct key,spu,item.name
from
(select distinct get_json_object(args,'$.search_key') as key,get_json_object(args,'$.spu') as spu
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-06' and '2020-7-12'
and id in(1378,1448,1454)
and length(get_json_object(args,'$.search_key'))!=0) as search
join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info as item
on search.spu=item.id
where item.name like concat('%',key,'%');


--每个关键词的曝光个数
select get_json_object(args,'$.search_key') as key,count(distinct case when then get_json_object(args,'$.spu') end) as spu
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-06' and '2020-7-12'
and id in(1378,1448,1454)
and length(get_json_object(args,'$.search_key'))!=0
group by get_json_object(args,'$.search_key');

select key,count(spu)
from
(select distinct get_json_object(args,'$.search_key') as key,get_json_object(args,'$.spu') as spu
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-06' and '2020-7-12'
and id in(1378,1448,1454)
and length(get_json_object(args,'$.search_key'))!=0) as search
join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info as item
on search.spu=item.id
where item.name like concat('%',key,'%')
group by key;

select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info where id =530;

select search.*,item.name
from
(select d,ctype,cname,cid,get_json_object(args,'$.spu') as spu,get_json_object(args,'$.search_key') as key
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-06' and '2020-7-12'
and id in(1378,1448,1454,1446)
) as search
join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info as item
on search.spu=item.id
where item.name not like concat('%',key,'%')
;

---黑金卡赠送优化
select *
from iyourcar_dw.rpt_ycyh_privilege_day_mall where d='2020-07-15';

select count(a.uid),count(distinct a.uid) from
iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user as a
join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard as b
on a.uid=b.uid;

--每日访问的cid中，离最近1次的访问间隔是多久
select d,ctype,cname,min_dif,count(cid)
from
(select d,cname,ctype,cid,min(dif) as min_dif
from
(select a.cid,datediff(a.d,b.d) as dif,a.d,a.cname,a.ctype
from tmp.lmh_tmp_pri_user as a
join (
        select *
     from iyourcar_dw.dws_behavior_day_device_active
     where d between '2019-12-16' and '2020-07-15'
    and ctype in(1,2,4)
    and cname in('WXAPP_YCYH_PLUS','WXAPP_YCQKJ','APP_SUV')
    ) as b
on a.cid=b.cid and a.ctype=b.ctype and a.cname=b.cname
where a.d>b.d) as c
group by d,cname,ctype,cid) as d
group by d,ctype,cname,min_dif;

--创建用户在访问当天是否黑金卡用户的表
create table tmp.lmh_tmp_pri_user as
    select distinct visit.*
    from
    (select *
     from iyourcar_dw.dws_behavior_day_device_active
     where d between '2020-06-16' and '2020-07-15'
    and ctype in(1,2,4)
    and cname in('WXAPP_YCYH_PLUS','WXAPP_YCQKJ','APP_SUV')) as visit
    left join
    iyourcar_dw.dws_extend_day_cid_map_uid as maps
    on visit.cid=maps.cid
    left join
    (select uid,substr(createtime,0,10) as create_d
    from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user
    where is_member in(1,2)
    and member_type=1) as real
    on real.uid=maps.uid
    left join
    (
        select *
        from
        (select uid,substr(starttime,0,10) as start_d,substr(endtime,0,10) as end_d,row_number() over (partition by uid order by starttime) as rank
        from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard
        where substr(starttime,0,10)>='2020-06-04') as a
        where rank=1
        ) as elec
    on maps.uid=elec.uid
    where maps.uid is null
    or (create_d is null and start_d is null)
    or (create_d is null and visit.d not between date_add(elec.start_d,1) and elec.end_d)
    or (start_d is null and visit.d<real.create_d)
    ;

select count(cid) from tmp.lmh_tmp_pri_user;

select count(cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-11' and '2020-07-14'
and id in(12561,12562,12563,12564,12565,12566,12567);

select count(cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-11' and '2020-07-14'
and id in(12575,12576,12577,12578,12579,12580,12581);


--查询黑金卡相关表
select *
from tmp.new_user_free_vcard_cid_exposure_click;

select num,count(distinct uid)
from
(select uid,count(distinct member_type) as num
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user
group by uid) as a
group by num
;

select *
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user limit 10;

select *
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user_vcard limit 10;

--ios和安卓7月14日到7月20日的各个tab访问人数
select ctype,d,name,count(distinct cid)
from
(select get_json_object(args,'$.gid') as gid,ctype,cid,d
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-14' and '2020-07-20'
and ctype in(1,2)
and cname='APP_SUV'
and id in(11338,11829,11341,11828,11339,11340)) as log
join (select name,special_no from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info where is_show=1 and mall_type=1) as groups
on groups.special_no=split(gid,'#')[1]
group by ctype,d,name;

select ctype,d,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-14' and '2020-07-20'
and get_json_object(args,'$.group_id')=27
and id in(544,365)
group by ctype,d;

select *
from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info;

select args,ctype,cid,d
from iyourcar_dw.dwd_all_action_hour_log
where d ='2020-07-20'
and ctype in(1,2)
and cname='APP_SUV'
and id in(11338,11829,11341,11828,11339,11340)
and get_json_object(args,'$.redirect_target')=27;

select *
from iyourcar_dw.dwd_all_action_hour_log
where d='2020-07-21' and id=11502;

select *
from tmp.new_user_free_vcard_first_privilege;



select * from tmp.rpt_mall_global_data;

select trunc('2020-06-07','MM');

---Q2的退货GMV
select sum(all_price)/100
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-04-01' and '2020-06-30'
and all_price>0
and order_status in(4,5)
and paytime is not null;


--利润
select (sum(case when order_status in(1,2,3) then all_price end) - sum(case when order_status in(1,2,3) then cost end) + sum(case when order_status in(1,2,3) then postage_price end) + count(case when is_open_privilege_card = 1 and order_status in(1,2,3,5) and paytime is not null then 1 end) * 5) / 100,
       sum(case when order_status in(1,2,3) then cost end)/100,
       sum(case when order_status in(1,2,3) then postage_price end)/100,
       count(case when is_open_privilege_card = 1 and order_status in(1,2,3,5) and paytime is not null then 1 end) * 5
from (
         select orders.order_no,
                orders.all_price,
                orders.postage_price,
                is_open_privilege_card,
                order_status,
                paytime,
                sum(item.cost_price * item.item_num) as cost
         from (select uid, order_no, all_price, postage_price, is_open_privilege_card, order_status,paytime
               from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
               where substr(ordertime, 0, 10) between '2020-07-01' and '2020-07-28'
                 and all_price > 0
                 and biz_type in (1, 3)) as orders
                  join
              iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
              on item.order_no = orders.order_no
                  join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku as info
                       on item.item_sku_id = info.id
         group by orders.order_no, orders.all_price, orders.postage_price, is_open_privilege_card, order_status,paytime) as a;


select sum(all_price)
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-07-01' and '2020-07-23'
and all_price>0
and order_status in(1,2,3)
and biz_type in(1,3);





select min(createtime) from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info
where cost_price>0;

--每日push点击用户
select d,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-10' and '2020-07-23'
and id in(11762,12486)
group by d;


---日数据利润
select d,ctype,mall_type,
 (sum(case when order_status in(1,2,3) then all_price end) - sum(case when order_status in(1,2,3) then cost end) + sum(case when order_status in(1,2,3) then postage_price end) + count(case when is_open_privilege_card = 1 and order_status in(1,2,3,5) and paytime is not null then 1 end) * 5) / 100 as `利润`
from (
         select orders.order_no,
                orders.all_price,
                orders.postage_price,
                is_open_privilege_card,
                order_status,
                paytime,
                ctype,
                mall_type,
                d,
                sum(item.cost_price * item.item_num) as cost
         from (select uid, order_no, all_price, postage_price, is_open_privilege_card, order_status,paytime,ctype,mall_type,substr(ordertime, 0, 10) as d
               from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
               where substr(ordertime, 0, 10) between date_sub('2020-07-27',14) and  '2020-07-27'
                 and all_price > 0
                 and biz_type in (1, 3)
                and mall_type=1) as orders
                  join
              iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
              on item.order_no = orders.order_no
         group by orders.order_no, orders.all_price, orders.postage_price, is_open_privilege_card, order_status,paytime,ctype,mall_type,d ) as a
    group by d,ctype,mall_type;


-----修复日监控数据bug
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
    TBLPROPERTIES ('creator' = 'liminghao', 'create_time' = '2020-07-28');

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

select * from iyourcar_dw.rpt_ycyh_service_day_privilege_user_vcard_statistics;

--车主秀历史数据
select log.d as `日期`,spu as `商品id`,
        count(distinct log.cid) as `看到车主秀的人数`,
       count(distinct case when order_item.order_no is not null and log.st<order_item.st then order_no end) as `下单人数`
from
(select cid,st,d,get_json_object(args,'$.spu') as spu
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-11' and '2020-07-28'
and id in(12576,12562)) as log
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on log.cid=maps.cid
left join
(
    select orders.*,item_id
    from
    (select uid,substr(ordertime,0,10) as d,order_no,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10)  between '2020-07-11' and '2020-07-28'
        and biz_type in(1,3)
        and ctype in(2,4)
        and order_status in(1,2,3)
        and all_price>0
        and mall_type=1)
    as orders
    join
    (
    select order_no,item_id
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item) as item
    on orders.order_no=item.order_no
) as order_item
on order_item.uid=maps.uid and log.d=order_item.d and order_item.item_id=log.spu
group by log.d,spu;

select * from tmp.rpt_official_comparative_analysis;
select * from tmp.rpt_official_comparative_analysis_card;

select * from tmp.rpt_mall_operation_wx_week_and_month;

--各月成为黑金卡实体卡用户的人数
select month(member_time),count(distinct uid)
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user
where member_type=1 and is_member=2 and substr(member_time,0,10)>='2020-01-01'
.+
group by month(member_time);

--各月成为黑金卡的用户到目前为止的加油花费
select month(member_time),sum(order_sum)
         from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_oil_order as oil
         inner join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user as pri_user
         on pri_user.phone = oil.phone
         where oil.pay_order_time > member_time and member_type=1 and is_member=2 and substr(member_time,0,10)>='2020-01-01' and status in(1,3)
group by month(member_time);

select month(member_time),count(distinct pri_user.uid)
         from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_oil_order as oil
         inner join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user as pri_user
         on pri_user.phone = oil.phone
         where oil.pay_order_time > member_time and member_type=1 and is_member=2 and substr(member_time,0,10)>='2020-01-01' and status in(1,3)
group by month(member_time);

select count(*)
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_oil_order where substr(createtime,0,10)>='2020-01-01' and status in(1,3);

--商城消费
select month(member_time),sum(all_price)/100
         from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
         inner join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user as pri_user
         on pri_user.uid = orders.uid
         where substr(orders.ordertime,0,10) > substr(member_time,0,10)
           and member_type=1
           and is_member=2
           and substr(member_time,0,10)>='2020-01-01'
            and order_status in(1,2,3)
            and all_price>0
            and biz_type in(1,3)
group by month(member_time);

select month(member_time),count(distinct pri_user.uid)
         from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
         inner join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user as pri_user
         on pri_user.uid = orders.uid
         where  member_type=1
           and is_member=2
           and substr(member_time,0,10)>='2020-01-01'
            and order_status in(1,2,3)
            and all_price>0
group by month(member_time);

select substr(ordertime,1,10) from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order;

---日燊代码
with ods as
    (
                select x.*,substr(x.ordertime,0,10) as d ,y.cost from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as x
                left join
                (
                        select a.order_no,sum(b.cost_price*a.item_num) as cost
                        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item a
                        left join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku b
                        on a.item_sku_id = b.id
                        group by a.order_no
                )y
                on x.order_no=y.order_no
                where biz_type in (1,3)  and order_status in (1,2,3) and substr(ordertime,1,10)<='2020-07-29' and substr(ordertime,1,10)>='2020-07-01'

),
      new_users as(
select count(*) from tmp.mall_new_user_cid_all_ctype_cname as t1
inner join
iyourcar_dw.dwd_all_user_day_uid_ralevant_cid as t2
on t1.cid = t2.cid_ralevant
where t1.d <='2020-07-29' and t1.d >='2020-07-01'
)
select concat('2020-07-29','~','2020-07-01') as `时间周期`,
case when ods.ctype = '1' then 'iOS'
when ods.ctype = '2' then 'And'
when ods.ctype = '4' then '小程序' end as ctype,
ods.mall_type,
count(distinct case when new_users.uid is not null then ods.uid end) as `新用户下单人数`,
count(distinct case when new_users.uid is  null then ods.uid end) as `老用户下单人数`
from
ods
left join
new_users
on ods.uid = new_users.uid
group by ods.ctype,ods.mall_type;

--
select *
from iyourcar_dw.rpt_ycyh_service_day_privilege_user_vcard_statistics;

--查询用户回访
drop table tmp.lmh_call_visit_user_0731;
CREATE TABLE IF NOT EXISTS tmp.lmh_call_visit_user_0731
(
    order_no                   string COMMENT 'id',
    d                          string COMMENT '回访日期',
    spu                        string comment '商品id'
)
    COMMENT '电召用临时表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

insert into tmp.lmh_call_visit_user_0731
values
('2557009093474649089','2020-07-09','1974'),
('2557107808474498051','2020-07-09','1555'),
('2557049307689649154','2020-07-09','530'),
('2557312858593428486','2020-07-09','1298'),
('2557648121509184518','2020-07-09','446'),
('2557726030798259200','2020-07-09','409'),
('2557831718937560068','2020-07-09','446'),
('2558917515199644677','2020-07-09','1953'),
('2559349363675497476','2020-07-09','475'),
('2559854802188108802','2020-07-09','446'),
('2562789238164685831','2020-07-16','446'),
('2563154517919007745','2020-07-16','698'),
('2563596201131770881','2020-07-16','446'),
('2564183219092063238','2020-07-16','446'),
('2564631950799143944','2020-07-16','776'),
('2565680991838405637','2020-07-19','446'),
('2565665398481486849','2020-07-19','1974'),
('2566038791697466376','2020-07-19','408'),
('2566504315594212359','2020-07-19','1555'),
('2566750289797317637','2020-07-19','446'),
('2566766937652593673','2020-07-19','1397'),
('2566819478566339593','2020-07-19','409'),
('2567512939825202176','2020-07-23','2276'),
('2567623268819272708','2020-07-23','1949'),
('2567924755072025602','2020-07-23','446'),
('2568664984355603459','2020-07-23','509'),
('2569247939511714817','2020-07-23','582'),
('2570344729438848000','2020-07-23','558'),
('2557049307689649154','2020-07-09','1555'),
('2558917515199644677','2020-07-09','2180'),
('2558917515199644677','2020-07-09','1237'),
('2558917515199644677','2020-07-09','2124'),
('2563154517919007745','2020-07-16','1208'),
('2563154517919007745','2020-07-16','2180');


delete from tmp.lmh_call_visit_user_0731 where order_no='2557107808474498051';

select *
from tmp.lmh_call_visit_user_0731;

---有多少用户在电访后访问过商城

select count(distinct log.uid)
from
(select uid,d,st
from iyourcar_dw.dwd_all_action_hour_log as visit
join iyourcar_dw.dwd_all_action_day_event_group as groups
on groups.event_id=visit.id
where d between '2020-07-09' and '2020-07-30'
    and groups.event_group_id=20) as log
join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on orders.uid=log.uid
join tmp.lmh_call_visit_user_0731 as call
on call.order_no=orders.order_no
where log.d>=call.d;

select * from iyourcar_dw.rpt_ycyh_service_day_privilege_mall_page_statistics;

--有多少用户在电访后访问过相应详情页

select log.id,log.spu,count(distinct maps.uid)
from
(select cid,d,get_json_object(args,'$.spu') as spu,id
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-07-30'
    and id in(302,1024,379,267)
    ) as log
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on log.cid=maps.cid
join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on orders.uid=maps.uid
join tmp.lmh_call_visit_user_0731 as call
on call.order_no=orders.order_no and call.spu=log.spu
where log.d>=call.d
group by log.id,log.spu;

select call.order_no
from
(select cid,d,get_json_object(args,'$.spu') as spu,id
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-07-30'
    and id in(302,1024,379,267)
    ) as log
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on log.cid=maps.cid
join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on orders.uid=maps.uid
join tmp.lmh_call_visit_user_0731 as call
on call.order_no=orders.order_no and call.spu=log.spu
where log.d>=call.d
;

select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_coupon_info limit 100;

select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_special_info;

select name from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info where id=1298;

select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info;

select ctype,args
from iyourcar_dw.dwd_all_action_hour_log
where d='2020-08-03'
and id=544
and get_json_object(args,'$.group_id')=27;

--7月14日-20日首页的分组点击
select d,groups.name,count(distinct cid)
from
(select d,get_json_object(args,'$.redirect_target') as group_id,cid
    from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-14' and '2020-07-20'
and ctype in(1,2)
and cname='APP_SUV'
and id in(11340,11339)
and split(get_json_object(args,'$.gid'),'#')[1]='416674') as log
join
    (
        select name,special_no,id
               from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info
                where special_no in(392716,392599,549391,390556,549392,349076,648452,648453)
        ) groups
on log.group_id=groups.id
group by d,groups.name;


select * from iyourcar_dw.dwd_all_action_hour_log
where d='2020-07-14'
and id =11340
and get_json_object(args,'$.redirect_target') like '%89%';

select id,args
from iyourcar_dw.dwd_all_action_hour_log
where d='2020-08-04'
and cname='WXAPP_YCYH_PLUS'
and get_json_object(args,'$.group_id')=83;

--导出每月首单+关闭订单+开通黑金卡的人数
select count(*)
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where order_status=5 and is_first_order=1;

select month(ordertime),count(distinct uid)
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where to_date(ordertime) between '2020-01-01' and '2020-07-31'
and order_status=5
and is_first_order=1
and is_open_privilege_card=1
and order_no in
    (
        '20200731193803142212992',
'2020073116364591328672',
'2020073022240617128672',
'2020073019461983128672',
'20200726202107569978944',
'20200726190003484212992',
'20200725215624484196608',
'20200719102402831978944',
'2568628472998003719',
'2020070308205465328672',
'20200625094350858212992',
'2551487758774305796',
'2551483073057260549',
'2550697239836099589',
'20200622164559594196608',
'2549497955597943813',
'20200620193029844212992',
'2548310905507021831',
'2020061913014031928672',
'2547005540198777862',
'2546983818955326472',
'2546914770066342920',
'2546892594202280963',
'2546856847508243461',
'2546831668212139013',
'2546419271399900163',
'2546279440023815169',
'20200616155358185196608',
'2545536757986755587',
'20200612121147718978944',
'2020061211211282228672',
'2541194458741867528',
'20200609144056956196608',
'20200608201421521212992',
'2020060720324077328672',
'20200607124053976978944',
'2538899964763833348',
'20200529170601190978944',
'2529842365846258697',
'2529659553222493192',
'2529395430836405249',
'2526687922896241671',
'2020052112413754628672',
'2020052011115694528672',
'2020051600282919728672',
'2020051420542720728672',
'2519315868685435908',
'20200509141431656978944',
'20200508155626299196608',
'20200508155236669212992',
'20200508154637353978944',
'20200508150630120196608',
'20200508141757826196608',
'2515882118584730625',
'2511658232997479429',
'20200429173435014196608',
'2020042917294156128672',
'20200429172509606196608',
'20200429172136558196608',
'20200428062857205196608',
'2508703950295794690',
'20200426112710910978944',
'20200425223919394196608',
'2507874624935560198',
'2507165641354511368',
'20200423180310358212992',
'20200423175554001212992',
'20200423175243812212992',
'2020042208583798528672',
'20200420183640984978944',
'2504183745251116041',
'2504109113298912263',
'2503451777253245959',
'2498396019641811971',
'20200409140525709196608',
'20200407203028018196608',
'2494312939574526980',
'2493986025253110793',
'20200406094608246196608',
'2020040509472149328672',
'20200404212835009196608',
'20200402172812581196608',
'20200402172513631978944',
'20200402164455486212992',
'2020040216312515428672',
'2490751053822166024',
'2020040117111992828672',
'20200401094846153978944',
'2489899534738523144',
'2489681786171294722',
'20200327145531894196608',
'2483119572127843333',
'20200319162658248978944',
'20200319131549064196608',
'20200318194016420978944',
'20200313091412653212992',
'2020031220163995828672',
'2020031220101631728672',
'2020031219051871428672',
'20200305182903892978944',
'20200305181919395212992',
'20200305181037588978944',
'2020030421075937928672',
'20200304101140590212992',
'20200227191153284212992',
'2020022719042938928672',
'20200227185232528196608',
'20200224082859895978944',
'20200223163819247196608',
'20200220223420825978944',
'20200220222717829196608',
'20200216001906612212992',
'20200215203406713978944',
'20200215202510214978944',
'20200215190441742212992',
'20200206151022467978944',
'2020020217325660328672',
'20200127132004379196608',
'20200121110020597212992',
'20200119094338503978944',
'20200118095152557978944',
'20200115164115163978944',
'2020011501082345828672',
'20200114215601503196608',
'20200114200702951212992',
'20200113033914345978944',
'20200111174136028196608',
'2020011110353505028672',
'2020010920381779628672',
'20200109143109814196608',
'2020010911362619128672',
'20200107135650948212992',
'20200104094824354196608',
'2020010313290315428672'        )
group by month(ordertime);

select count(*)
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where is_first_order=1
and to_date(ordertime) between '2020-01-01' and '2020-01-31';

select orders.*,item.*
from
(select order_no,ordertime,order_status
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where uid='2_56d96b5a0cf28b4edcf17a26') as orders
join
(select *
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item where order_no='2568628472998003719') as item
on orders.order_no=item.order_no
;