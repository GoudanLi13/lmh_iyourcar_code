drop table if exists tmp.rpt_mall_global_data;

set hive.groupby.skewindata = false;
set mapreduce.job.queuename=dailyHour;

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
                where biz_type in (1,3)  and order_status in (1,2,3) and substr(ordertime,1,10)<='2020-01-31' and substr(ordertime,1,10)>='2020-01-01'

),

 new_users as(
select * from tmp.mall_new_user_cid_all_ctype_cname as t1
inner join
iyourcar_dw.dwd_all_user_day_uid_ralevant_cid as t2
on t1.cid = t2.cid_ralevant
where t1.d <='2020-01-31' and t1.d >='2020-01-01'
),
 operation as  (select case when mall_type = '声望商城' then '2'
    when mall_type = '有车币商城' then '1'
    when mall_type = '总计' then '总计' end as malltype,
      case when platform = 'And' then 'And'
    when platform = 'iOS' then 'iOS'
    when platform = '小程序' or mall_type = '声望商城' then '小程序'
    when platform = 'App' then 'App'
    when platform = '总计' then '总计' end as ctype,*
       from  tmp.rpt_mall_operation_wx_week_and_month)


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
       operation.order_user_count / operation.mall_visit_count as `转化率`
       from
(
select concat('2020-01-31','~','2020-01-01') as `时间周期`,
case when visit_mall.ctype = '1' then 'iOS'
     when visit_mall.ctype = '2' then 'And'
     when visit_mall.ctype = '4' then '小程序' end as ctype,
case when visit_mall.cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when visit_mall.cname  = 'WXAPP_YCQKJ' then '2' end as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from

(select * from tmp.mall_new_user_cid_all_ctype_cname where d <='2020-01-31' and d >='2020-01-01') as new_users
on visit_mall.cid = new_users.cid
group by visit_mall.ctype,case when visit_mall.cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when visit_mall.cname  = 'WXAPP_YCQKJ' then '2' end
union all
select concat('2020-01-31','~','2020-01-01') as `时间周期`,'App' as ctype, '1' as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d <='2020-01-31' and d >='2020-01-01') as new_users
on visit_mall.cid = new_users.cid
where visit_mall.cname in ('APP_SUV')
union all
select concat('2020-01-31','~','2020-01-01') as `时间周期`,'总计' as ctype, '1' as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d <='2020-01-31' and d >='2020-01-01') as new_users
on visit_mall.cid = new_users.cid
where visit_mall.cname in ('APP_SUV','WXAPP_YCYH_PLUS')
union all
select concat('2020-01-31','~','2020-01-01') as `时间周期`,'总计' as ctype,'总计' as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d <='2020-01-31' and d >='2020-01-01') as new_users
on visit_mall.cid = new_users.cid
) as visit
inner join
(
select concat('2020-01-31','~','2020-01-01') as `时间周期`,
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
select concat('2020-01-31','~','2020-01-01') as `时间周期`,'App' as ctype,'1' as mall_type,
count(distinct case when new_users.uid is not null then ods.uid end) as `新用户下单人数`,
count(distinct case when new_users.uid is  null then ods.uid end) as `老用户下单人数`
from
(select * from ods where ctype in (1,2)) as ods
left join
new_users
on ods.uid = new_users.uid
union all
select concat('2020-01-31','~','2020-01-01') as `时间周期`,'总计' as ctype,ods.mall_type,
count(distinct case when new_users.uid is not null then ods.uid end) as `新用户下单人数`,
count(distinct case when new_users.uid is  null then ods.uid end) as `老用户下单人数`
from
ods
left join
new_users
on ods.uid = new_users.uid
group by ods.mall_type
union all
select concat('2020-01-31','~','2020-01-01') as `时间周期`,'总计' as ctype,'总计' as mall_type,
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
;

--新用户访问数，新用户下单数
select visit.ctype,visit.cname,count(distinct case when maps.uid is not null then orders.uid end)
from
(select *
from tmp.mall_new_user_cid_all_ctype_cname where d bet '2020-07-01' and '2020-07-29') as visit
join iyourcar_dw.dwd_all_user_day_uid_ralevant_cid as maps
on maps.cid_ralevant=visit.cid
right join
(
    select *
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,1,10) between '2020-07-01' and '2020-07-29'
    and biz_type in(1,3)
    and order_status in(1,2,3)
    and all_price>0
    ) as orders
on maps.uid=orders.uid and orders.ctype=visit.ctype
group by visit.ctype,visit.cname
order by visit.ctype,visit.cname;

select count(distinct uid)
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-01' and '2020-07-29'
    and biz_type in(1,3)
    and order_status in(1,2,3)
    and all_price>0
    and ctype=1;

select min(d)
from tmp.mall_new_user_cid_all_ctype_cname;

with t_user as
(select distinct ctype,cid,cname from iyourcar_dw.dwd_all_action_hour_log log
join iyourcar_dw.dwd_all_action_day_event_group event_group
on log.id=event_group.event_id
where log.d = '2020-01-02' and event_group.event_group_id=20
)
-- 历史用户
, t_user_his as
(select distinct ctype,cid,cname from iyourcar_dw.dwd_all_action_hour_log log
join iyourcar_dw.dwd_all_action_day_event_group event_group
on log.id=event_group.event_id
where log.d>=date_sub('2020-01-02',180) and log.d<'2020-01-02' and event_group.event_group_id=20
)
insert overwrite table tmp.mall_new_user_cid_all_ctype_cname partition (d = '2020-01-02')
select t_user.cid,t_user.ctype,t_user.cname from t_user left join t_user_his
on t_user.ctype=t_user_his.ctype and t_user.cid=t_user_his.cid and t_user.cname=t_user_his.cname
where t_user_his.cid is null ;

select count(*)
from
(select *
from tmp.mall_new_user_cid_all_ctype_cname where d between '2020-07-01' and '2020-07-29') as visit
join iyourcar_dw.dwd_all_user_day_uid_ralevant_cid as maps
on maps.cid_ralevant=visit.cid;

select * from iyourcar_dw.rpt_ycyh_service_day_privilege_user_vcard_statistics;