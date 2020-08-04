drop table if exists tmp.rpt_mall_operation_wx_month;
create table tmp.rpt_mall_operation_wx_month_01 as
with order_tmp_30 as
         (
             select x.*,y.cost from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as x
                                        left join
                                    (
                                        select a.order_no,sum(b.cost_price*a.item_num) as cost
                                        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item a
                                                 left join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku b
                                                           on a.item_sku_id = b.id
                                        group by a.order_no
                                    )y
                                    on x.order_no=y.order_no
             where biz_type in (1,3) and substr(ordertime,1,10)>='2020-06-01' and substr(ordertime,1,10)<='2020-06-29'
         )
select action_data.`时间周期` as time_period
     ,order_data.`商城类型` as mall_type
     ,order_data.`平台` as platform
     ,case when order_data.`商城类型`='有车币商城' and order_data.`平台`='iOS' then `有车币商城ios访问人数`
           when order_data.`商城类型`='有车币商城' and order_data.`平台`='And' then `有车币商城And访问人数`
           when order_data.`商城类型`='有车币商城' and order_data.`平台`='App' then `有车币商城App访问人数`
           when order_data.`商城类型`='有车币商城' and order_data.`平台`='小程序' then `有车币商城小程序访问人数`
           when order_data.`商城类型`='有车币商城' and order_data.`平台`='总计' then `有车币商城总计访问人数`
           when order_data.`商城类型`='声望商城' and order_data.`平台`='总计' then `声望商城访问人数`
           when order_data.`商城类型`='总计' and order_data.`平台`='总计' then `访问合计` end as mall_visit_count
     ,order_data.`下单人数` as order_user_count
     ,order_data.`下单人数`/(case when order_data.`商城类型`='有车币商城' and order_data.`平台`='iOS' then `有车币商城iOS访问人数`
                              when order_data.`商城类型`='有车币商城' and order_data.`平台`='And' then `有车币商城And访问人数`
                              when order_data.`商城类型`='有车币商城' and order_data.`平台`='App' then `有车币商城App访问人数`
                              when order_data.`商城类型`='有车币商城' and order_data.`平台`='小程序' then `有车币商城小程序访问人数`
                              when order_data.`商城类型`='有车币商城' and order_data.`平台`='总计' then `有车币商城总计访问人数`
                              when order_data.`商城类型`='声望商城' and order_data.`平台`='总计' then `声望商城访问人数`
                              when order_data.`商城类型`='总计' and order_data.`平台`='总计' then `访问合计` end) as `conversion_rate`
     ,order_data.`订单数` as order_count
     ,order_data.`GMV`
     ,order_data.`GMV`/order_data.`下单人数` as `customer_price`
     ,order_data.`首单用户数` as first_order_user_count
     ,order_data.`首单用户数`/order_data.`下单人数` as `first_order_user_portion`
     ,order_data.`开卡用户数` as open_card_user_count
     ,(order_data.`GMV`-order_data.`成本` +order_data.`开卡用户数`*5 +order_data.`邮费`) as `all_profit`
     ,order_data.`GMV`-order_data.`成本` as `goods_profit`
     ,order_data.`开卡用户数`*5 as `open_card_profit`
     ,order_data.`邮费` as mail_profit
from
    (
--月数据
        select concat('2020-06-01','~','2020-06-29') as `时间周期`
             ,count(distinct case when ctype in (1)  and cname = 'APP_SUV' then cid end ) as `有车币商城iOS访问人数`
             ,count(distinct case when ctype in (2)  and cname = 'APP_SUV' then cid end ) as `有车币商城And访问人数`
             ,count(distinct case when ctype in (1,2)  and cname = 'APP_SUV' then cid end ) as `有车币商城App访问人数`
             ,count(distinct case when ctype = 4 and cname = 'WXAPP_YCYH_PLUS' then cid end ) as `有车币商城小程序访问人数`
             ,count(distinct case when ((cname='APP_SUV' and ctype in (1,2)) or (cname='WXAPP_YCYH_PLUS' and ctype=4 )) then cid end ) as `有车币商城总计访问人数`
             ,count(distinct case when ctype = 4 and cname = 'WXAPP_YCQKJ' then cid end ) as `声望商城访问人数`
             ,count(distinct cid) as `访问合计`
        from iyourcar_dw.dwd_all_action_hour_log
                 inner join (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id = 20) as visit_mall
                            on dwd_all_action_hour_log.id = visit_mall.event_id
        where d>='2020-06-01'  and d <= '2020-06-29'
    )action_data
        left join
    (
--月数据
-- 有车币商城分平台
        select concat('2020-06-01','~','2020-06-29') as `时间周期`
             ,'有车币商城' as `商城类型`
             ,case when ctype =1  then 'iOS' when ctype = 2 then 'And' when ctype=4 then '小程序' end as `平台`
             ,count(distinct case when order_status in (1,2,3)  then uid end) as `下单人数`
             ,count(distinct case when order_status in (1,2,3)  then order_no end) as `订单数`
             ,sum(case when order_status in (1,2,3)  then all_price end)/100 as `GMV`
             ,count(distinct case when is_first_order  =1 and order_status in (1,2,3) then uid else null end) as `首单用户数`
             ,count(distinct case when is_open_privilege_card  =1 and order_status in (1,2,3,5)  and paytime is not null then uid else null end) as `开卡用户数`
             ,sum(case when order_status in (1,2,3) then postage_price end)/100 as `邮费`
             ,sum(case when order_status in (1,2,3) then cost end)/100 as `成本`
        from order_tmp_30
        where  mall_type=1
        group by case when ctype =1  then 'iOS' when ctype = 2 then 'And' when ctype=4 then '小程序' end
        having `平台` is not null
        union all
--有车币商城的App单独拿出来
        select concat('2020-06-01','~','2020-06-29') as `时间周期`
             ,'有车币商城' as `商城类型`
             ,'App' as `平台`
             ,count(distinct case when order_status in (1,2,3)  then uid end) as `下单人数`
             ,count(distinct case when order_status in (1,2,3)  then order_no end) as `订单数`
             ,sum(case when order_status in (1,2,3)  then all_price end)/100 as `GMV`
             ,count(distinct case when is_first_order  =1 and order_status in (1,2,3) then uid else null end) as `首单用户数`
             ,count(distinct case when is_open_privilege_card  =1 and order_status in (1,2,3,5)  and paytime is not null then uid else null end) as `开卡用户数`
             ,sum(case when order_status in (1,2,3) then postage_price end)/100 as `邮费`
             ,sum(case when order_status in (1,2,3) then cost end)/100 as `成本`
        from order_tmp_30
        where  mall_type=1 and ctype in (1,2)
        union all
-- 两个商城分平台
        select concat('2020-06-01','~','2020-06-29') as `时间周期`
             ,case mall_type when 1 then '有车币商城' when 2 then '声望商城' end as `商城类型`
             ,'总计' as `平台`
             ,count(distinct case when order_status in (1,2,3)  then uid end) as `下单人数`
             ,count(distinct case when order_status in (1,2,3)  then order_no end) as `订单数`
             ,sum(case when order_status in (1,2,3)  then all_price end)/100 as `GMV`
             ,count(distinct case when is_first_order  =1 and order_status in (1,2,3) then uid else null end) as `首单用户数`
             ,count(distinct case when is_open_privilege_card  =1 and order_status in (1,2,3,5)  and paytime is not null then uid else null end) as `开卡用户数`
             ,sum(case when order_status in (1,2,3) then postage_price end)/100 as `邮费`
             ,sum(case when order_status in (1,2,3) then cost end)/100 as `成本`
        from order_tmp_30
        group by case mall_type when 1 then '有车币商城' when 2 then '声望商城' end
        union all
-- 全局总计
        select concat('2020-06-01','~','2020-06-29') as `时间周期`
             ,'总计' as `商城类型`
             ,'总计' as `平台`
             ,count(distinct case when order_status in (1,2,3)  then uid end) as `下单人数`
             ,count(distinct case when order_status in (1,2,3)  then order_no end) as `订单数`
             ,sum(case when order_status in (1,2,3)  then all_price end)/100 as `GMV`
             ,count(distinct case when is_first_order  =1 and order_status in (1,2,3) then uid else null end) as `首单用户数`
             ,count(distinct case when is_open_privilege_card  =1 and order_status in (1,2,3,5)  and paytime is not null then uid else null end) as `开卡用户数`
             ,sum(case when order_status in (1,2,3) then postage_price end)/100 as `邮费`
             ,sum(case when order_status in (1,2,3) then cost end)/100 as `成本`
        from order_tmp_30
    )order_data
    on action_data.`时间周期`= order_data.`时间周期`;


select * from tmp.rpt_mall_operation_wx_month_01;

--
create table tmp.rpt_mall_global_data_month_01 as
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
                where biz_type in (1,3)  and order_status in (1,2,3) and substr(ordertime,1,10)>='2020-06-01' and substr(ordertime,1,10)<='2020-06-29'
),
visit_mall as
         (
select distinct ctype,cname,cid,d from iyourcar_dw.dwd_all_action_hour_log log
join iyourcar_dw.dwd_all_action_day_event_group event_group
on log.id=event_group.event_id
where log.d >='2020-06-01' and log.d <='2020-06-29'  and event_group.event_group_id=20
),
 new_users as(
select * from tmp.mall_new_user_cid_all_ctype_cname as t1
inner join
iyourcar_dw.dwd_all_user_day_uid_ralevant_cid as t2
on t1.cid = t2.cid_ralevant
where t1.d >='2020-06-01' and t1.d <='2020-06-29'
),
 operation as  (select case when mall_type = '声望商城' then '2'
    when mall_type = '有车币商城' then '1'
    when mall_type = '总计' then '总计' end as malltype,
      case when platform = 'And' then 'And'
    when platform = 'iOS' then 'iOS'
    when platform = '小程序' or mall_type = '声望商城' then '小程序'
    when platform = 'App' then 'App'
    when platform = '总计' then '总计' end as ctype,*
       from  tmp.rpt_mall_operation_wx_month_01)
,rep_buy_users as (
    select concat('2020-06-01','~','2020-06-29') as `时间周期`,
     case when mall_type = '2' then '2'
    when mall_type = '1' then '1'
    when mall_type = '总计' then '总计' end as malltype,
      case when ctype = '2' then 'And'
    when ctype = '1'  then 'iOS'
    when ctype = '4' or mall_type = '2' then '小程序'
    when ctype = 'APP'  then 'App'
    when ctype = '总计' then '总计' end as ctype
    ,`复购人数`
    from
(
    select `时间周期`,mall_type,ctype, count(distinct uid) as `复购人数` from (
        select concat('2020-06-01','~','2020-06-29') as `时间周期`
        ,uid
        ,mall_type
        ,ctype
        ,count(uid)      as `下单次数`
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order  as orders
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-29'
 and biz_type in (1,3) and order_status in (1,2,3)
group by concat('2020-06-01','~','2020-06-29')
        ,uid
        ,mall_type
        ,ctype
        having count(uid) > 1
        ) as a
group by `时间周期`,mall_type,ctype
union all
    select `时间周期`,'1' as mall_type,'总计' as ctype, count(distinct uid) as `复购人数` from (
        select concat('2020-06-01','~','2020-06-29' as `时间周期`
        ,uid
        ,count(uid)      as `下单次数`
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order  as orders
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-29' and mall_type = 1
group by concat('2020-06-01','~','2020-06-29')
        ,uid
        having count(uid) > 1
        ) as a
group by `时间周期`
union all
    select `时间周期`,'1' as mall_type,'APP' as ctype, count(distinct uid) as `复购人数` from (
        select concat('2020-06-01','~','2020-06-29') as `时间周期`
        ,uid
        ,count(uid)      as `下单次数`
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order  as orders
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-29' and mall_type = 1 and ctype in (1,2)
group by concat('2020-06-01','~','2020-06-29')
        ,uid
        having count(uid) > 1
        ) as a
group by `时间周期`
union all
    select `时间周期`,'总计' as mall_type,'总计' as ctype, count(distinct uid) as `复购人数` from (
        select concat('2020-06-01','~','2020-06-29') as `时间周期`
        ,uid
        ,count(uid)      as `下单次数`
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order  as orders
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-29'
group by concat('2020-06-01','~','2020-06-29')
        ,uid
        having count(uid) > 1
        ) as a
group by `时间周期`) as a
)
select
       consume.`时间周期` as `时间周期`,
       case when consume.mall_type = '1' then '有车币商城'
           when consume.mall_type = '2' then '声望商城'
           when consume.mall_type = '总计' then '总计'
               end as `商城类型`,
       case when consume.ctype = 'And' then '安卓' else consume.ctype end  as `平台`,
       visit.`新用户访问商城人数`  as `新用户访问商城人数`,
       visit.`老用户访问商城人数` as `老用户访问商城人数`,
       operation.mall_visit_count as `访问商城人数`,
       consume.`新用户下单人数` as`新用户下单人数` ,
       consume.`老用户下单人数` as `老用户下单人数`,
       operation.order_user_count as `下单人数`,
       operation.order_count as `下单量`,
       operation.customer_price as `客单价`,
       operation.gmv as `GMV`,
       operation.all_profit as `利润`,
       operation.all_profit / operation.gmv as `利润率`,
       operation.order_user_count / operation.mall_visit_count as `转化率`,
        rep_buy_users.`复购人数` as `复购人数`,
        rep_buy_users.`复购人数`/operation.order_user_count as `复购率`
       from
(
select concat('2020-06-01','~','2020-06-29') as `时间周期`,
case when visit_mall.ctype = '1' then 'iOS'
     when visit_mall.ctype = '2' then 'And'
     when visit_mall.ctype = '4' then '小程序' end as ctype,
case when visit_mall.cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when visit_mall.cname  = 'WXAPP_YCQKJ' then '2' end as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d >='2020-06-01' and d <='2020-06-29') as new_users
on visit_mall.cid = new_users.cid
group by visit_mall.ctype,case when visit_mall.cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when visit_mall.cname  = 'WXAPP_YCQKJ' then '2' end
union all
select concat('2020-06-01','~','2020-06-29') as `时间周期`,'App' as ctype, '1' as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d >='2020-06-01' and d <='2020-06-29') as new_users
on visit_mall.cid = new_users.cid
where visit_mall.cname in ('APP_SUV')
union all
select concat('2020-06-01','~','2020-06-29') as `时间周期`,'总计' as ctype, '1' as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d >='2020-06-01' and d <='2020-06-29')as new_users
on visit_mall.cid = new_users.cid
where visit_mall.cname in ('APP_SUV','WXAPP_YCYH_PLUS')
union all
select concat('2020-06-01','~','2020-06-29') as `时间周期`,'总计' as ctype,'总计' as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d >='2020-06-01' and d <='2020-06-29')as new_users
on visit_mall.cid = new_users.cid
) as visit
inner join
(
select concat('2020-06-01','~','2020-06-29') as `时间周期`,
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
group by ods.ctype,ods.mall_type
union all
select concat('2020-06-01','~','2020-06-29') as `时间周期`,'App' as ctype,'1' as mall_type,
count(distinct case when new_users.uid is not null then ods.uid end) as `新用户下单人数`,
count(distinct case when new_users.uid is  null then ods.uid end) as `老用户下单人数`
from
(select * from ods where ctype in (1,2)) as ods
left join
new_users
on ods.uid = new_users.uid
union all
select concat('2020-06-01','~','2020-06-29') as `时间周期`,'总计' as ctype,ods.mall_type,
count(distinct case when new_users.uid is not null then ods.uid end) as `新用户下单人数`,
count(distinct case when new_users.uid is  null then ods.uid end) as `老用户下单人数`
from
ods
left join
new_users
on ods.uid = new_users.uid
group by ods.mall_type
union all
select concat('2020-06-01','~','2020-06-29') as `时间周期`,'总计' as ctype,'总计' as mall_type,
count(distinct case when new_users.uid is not null then ods.uid end) as `新用户下单人数`,
count(distinct case when new_users.uid is  null then ods.uid end) as `老用户下单人数`
from
ods
left join
new_users
on ods.uid = new_users.uid
) as consume
on visit.ctype = consume.ctype and visit.mall_type = consume.mall_type
inner join
 operation
on consume.`时间周期` = operation.time_period and  consume.ctype = operation.ctype and consume.mall_type = operation.malltype
inner join
     rep_buy_users
on consume.`时间周期` = rep_buy_users.`时间周期` and consume.ctype = rep_buy_users.ctype and consume.mall_type = rep_buy_users.malltype
;