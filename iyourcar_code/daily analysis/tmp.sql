create table tmp.rpt_mall_global_data_month as
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
                where biz_type in (1,3)  and order_status in (1,2,3) and substr(ordertime,1,10)>='{{ ds }}' and substr(ordertime,1,10)<=date_add('{{ ds }}',31)
),
visit_mall as
         (
select distinct ctype,cname,cid,d from iyourcar_dw.dwd_all_action_hour_log log
join iyourcar_dw.dwd_all_action_day_event_group event_group
on log.id=event_group.event_id
where log.d >='{{ ds }}' and log.d <=date_add('{{ ds }}',31)  and event_group.event_group_id=20
),
 new_users as(
select * from tmp.mall_new_user_cid_all_ctype_cname as t1
inner join
iyourcar_dw.dwd_all_user_day_uid_ralevant_cid as t2
on t1.cid = t2.cid_ralevant
where t1.d >='{{ ds }}' and t1.d <=date_add('{{ ds }}',31)
),
 operation as  (select case when mall_type = '声望商城' then '2'
    when mall_type = '有车币商城' then '1'
    when mall_type = '总计' then '总计' end as malltype,
      case when platform = 'And' then 'And'
    when platform = 'iOS' then 'iOS'
    when platform = '小程序' or mall_type = '声望商城' then '小程序'
    when platform = 'App' then 'App'
    when platform = '总计' then '总计' end as ctype,*
       from  tmp.rpt_mall_operation_wx_month)
,rep_buy_users as (
    select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`,
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
        select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`
        ,uid
        ,mall_type
        ,ctype
        ,count(uid)      as `下单次数`
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order  as orders
where substr(ordertime,0,10) between '{{ ds }}' and date_add('{{ ds }}',31)
 and biz_type in (1,3) and order_status in (1,2,3)
group by concat('{{ ds }}','~',date_add('{{ ds }}',31))
        ,uid
        ,mall_type
        ,ctype
        having count(uid) > 1
        ) as a
group by `时间周期`,mall_type,ctype
union all
    select `时间周期`,'1' as mall_type,'总计' as ctype, count(distinct uid) as `复购人数` from (
        select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`
        ,uid
        ,count(uid)      as `下单次数`
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order  as orders
where substr(ordertime,0,10) between '{{ ds }}' and date_add('{{ ds }}',31) and mall_type = 1
group by concat('{{ ds }}','~',date_add('{{ ds }}',31))
        ,uid
        having count(uid) > 1
        ) as a
group by `时间周期`
union all
    select `时间周期`,'1' as mall_type,'APP' as ctype, count(distinct uid) as `复购人数` from (
        select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`
        ,uid
        ,count(uid)      as `下单次数`
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order  as orders
where substr(ordertime,0,10) between '{{ ds }}' and date_add('{{ ds }}',31) and mall_type = 1 and ctype in (1,2)
group by concat('{{ ds }}','~',date_add('{{ ds }}',31))
        ,uid
        having count(uid) > 1
        ) as a
group by `时间周期`
union all
    select `时间周期`,'总计' as mall_type,'总计' as ctype, count(distinct uid) as `复购人数` from (
        select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`
        ,uid
        ,count(uid)      as `下单次数`
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order  as orders
where substr(ordertime,0,10) between '{{ ds }}' and date_add('{{ ds }}',31)
group by concat('{{ ds }}','~',date_add('{{ ds }}',31))
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
select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`,
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
(select * from tmp.mall_new_user_cid_all_ctype_cname where d >='{{ ds }}' and d <=date_add('{{ ds }}',31)) as new_users
on visit_mall.cid = new_users.cid
group by visit_mall.ctype,case when visit_mall.cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when visit_mall.cname  = 'WXAPP_YCQKJ' then '2' end
union all
select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`,'App' as ctype, '1' as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d >='{{ ds }}' and d <=date_add('{{ ds }}',31)) as new_users
on visit_mall.cid = new_users.cid
where visit_mall.cname in ('APP_SUV')
union all
select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`,'总计' as ctype, '1' as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d >='{{ ds }}' and d <=date_add('{{ ds }}',31))as new_users
on visit_mall.cid = new_users.cid
where visit_mall.cname in ('APP_SUV','WXAPP_YCYH_PLUS')
union all
select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`,'总计' as ctype,'总计' as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d >='{{ ds }}' and d <=date_add('{{ ds }}',31))as new_users
on visit_mall.cid = new_users.cid
) as visit
inner join
(
select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`,
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
select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`,'App' as ctype,'1' as mall_type,
count(distinct case when new_users.uid is not null then ods.uid end) as `新用户下单人数`,
count(distinct case when new_users.uid is  null then ods.uid end) as `老用户下单人数`
from
(select * from ods where ctype in (1,2)) as ods
left join
new_users
on ods.uid = new_users.uid
union all
select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`,'总计' as ctype,ods.mall_type,
count(distinct case when new_users.uid is not null then ods.uid end) as `新用户下单人数`,
count(distinct case when new_users.uid is  null then ods.uid end) as `老用户下单人数`
from
ods
left join
new_users
on ods.uid = new_users.uid
group by ods.mall_type
union all
select concat('{{ ds }}','~',date_add('{{ ds }}',31)) as `时间周期`,'总计' as ctype,'总计' as mall_type,
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

--看各端的黑金卡用户访问占比
select ctype,cname,d,count(card.uid)
from (
        select uid
        from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user
        where is_member=2
          and member_type=1
        and member_time is not null
         ) as card
join
    (
        select distinct d,ctype,uid,cname
        from iyourcar_dw.dwd_all_action_hour_log
        where d between '2020-06-06' and '2020-07-05'
        and ctype in(1,2,4)
        and cname in('APP_SUV','WXAPP_YCYH_PLUS','WXAPP_YCQKJ')
        ) as log_a
on  card.uid=log_a.uid
group by ctype,cname,d;

select ctype,cname, d, count(card.uid)
from (
         select uid
         from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user
         where is_member = 2
        and member_type=1
        and member_time is not null
     ) as card
         join
     (
         select distinct d, ctype, uid,cname
         from iyourcar_dw.dwd_all_action_hour_log as log
                  join (select event_id
                        from iyourcar_dw.dwd_all_action_day_event_group
                        where event_group_id = 20
         ) as visit_mall
                       on log.id = visit_mall.event_id
         where d between '2020-06-06' and '2020-07-05'
           and ctype in (1, 2, 4)
           and cname in ('APP_SUV', 'WXAPP_YCYH_PLUS', 'WXAPP_YCQKJ')
     )
         as log_a
     on card.uid = log_a.uid
group by ctype, cname,d;


select ctype,cname,count(card.uid),count(log_a.uid)
from (
        select uid
        from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user
        where is_member=2
        and member_time is not null
         ) as card
right join
    (
        select distinct ctype,uid,cname
        from iyourcar_dw.dwd_all_action_hour_log
        where d between '2020-06-06' and '2020-07-05'
        and ctype in(1,2,4)
        and cname in('APP_SUV','WXAPP_YCYH_PLUS','WXAPP_YCQKJ')
        ) as log_a
on  card.uid=log_a.uid
group by ctype,cname;

select min(createtime)
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where cost_price!=0;

select `时间周期`,
`商城类型`,
`平台`,
`新用户访问商城人数`,
`老用户访问商城人数`,
`访问商城人数`,
`新用户下单人数`,
`老用户下单人数`,
`新用户下单人数`/`新用户访问商城人数` as `新用户下单转化率`,
`老用户下单人数`/`老用户访问商城人数` as `老用户下单转化率`,
`下单人数`/`访问商城人数` as `所有用户下单转化率`,
`下单量`,
`客单价`,
`gmv`,
`利润`,
`利润率`,
`转化率`,
`复购人数`,
`复购率` from tmp.rpt_mall_global_data order by
(case `商城类型` when '有车币商城' then 1
    when '声望商城' then 2
    when '总计'then 3 end )
asc
,
(case `平台` when '安卓' then 1
    when 'iOS' then 2
    when 'App'then 3
    when '小程序' then 4
    when '总计' then 5
    end );

select *
from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_car_show;

select ordertime
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where order_status=4
limit 50;



select
     action.`推文id`
     , action.`商品id`
     , action.`小程序商品详情页`
     , action.`商城下单人数`
from (
         select              as `时间周期`,
                wx_app.article_id    as `推文id`,
                '$spu'               as `商品id`,
                wx_app.count         as `小程序商品详情页`,
                app_mall_order.count as `商城下单人数`
         from (
                  select "$article_list" as article_id, count(distinct a.cid) as count
                  from (select cid, session
                        from iyourcar_dw.dwd_all_action_hour_log
                        where d >= '$first'
                          and d <= date_add('$first', 6)
                          and id = 316
                          and get_json_object(args, '$.wx_page') like concat($article_list,'%')) a
                           join
                       (select cid, session
                        from iyourcar_dw.dwd_all_action_hour_log
                        where d >= '$first'
                          and d <= date_add('$first', 6)
                          and id = 302
                          and get_json_object(args, '$.spu') = '$spu') b
                       on a.cid = b.cid and a.session = b.session
                  group by "$article_list"
              ) as wx_app
left join
(select "$article_list" as article_id,count(distinct xxx.uid) as count from
(select distinct xx.uid,xx.d from
(select distinct a.uid,a.d from
(select uid,cid,session,d from iyourcar_dw.dwd_all_action_hour_log
where d>='$first' and d<=date_add('$first',6) and id=316 and get_json_object(args,'$.wx_page') like concat($article_list,'%')) a
join
(select uid,cid,session from iyourcar_dw.dwd_all_action_hour_log
where d>='$first' and d<=date_add('$first',6) and id=302 and get_json_object(args,'$.spu')='$spu' ) b
on a.cid =b.cid and a.session =b.session
)xx
)xxx
join
(select distinct uid,d from
(select uid,order_no,substr(ordertime,1,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where biz_type=1 and substr(ordertime,1,10)>='$first' and substr(ordertime,1,10)<=date_add('$first',6) and order_status in (1,2,3)) e
join
(select distinct order_no from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item where item_id='$spu')f
on e.order_no=f.order_no )yyy
on xxx.uid=yyy.uid and xxx.d = yyy.d
group by "$article_list"
) as app_mall_order
on wx_app.article_id = app_mall_order.article_id
) as action;