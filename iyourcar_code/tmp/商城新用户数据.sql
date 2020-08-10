--1.路过的用户

with new_user as(
select *
from tmp.mall_new_user_cid_all_ctype_cname
where ctype in(1,2)
and d='2020-07-30')


select visit.ctype,count(case when mall.cid is null then visit.cid end)
from
(select distinct cid,ctype
from iyourcar_dw.dwd_all_action_hour_log
where ctype in(1,2)
and d='2020-07-30'
and cname='APP_SUV'
and id=11341
and split(get_json_object(args,'$.gid'),'#')[1]='416674') as visit
left join
(select distinct ctype,cid
from
(select id,ctype,cid
from iyourcar_dw.dwd_all_action_hour_log
where ctype in(1,2)
and d='2020-07-30'
and cname='APP_SUV') as log
join
(select event_id
from iyourcar_dw.dwd_all_action_day_event_group
    where event_group_id=20
    ) as groups
on groups.event_id=log.id) as mall
on mall.cid=visit.cid and mall.ctype=visit.ctype
join new_user
on new_user.ctype=visit.ctype and new_user.cid=visit.cid
group by visit.ctype;


--2.付款占比
select new_user.d,count(distinct new_user.cid)
from
(select *
from tmp.mall_new_user_cid_all_ctype_cname
where d between '2020-08-01' and '2020-08-09'
and ctype in(1,2)
and cname='APP_SUV') as new_user
join
iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.cid=new_user.cid
join (
    select uid,all_price,to_date(ordertime) as d,ctype
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where mall_type=1
    and ctype in(1,2)
    and all_price>0
    and order_status in(1,2,3)
    and biz_type in(1,3)) as orders
on maps.uid=orders.uid and orders.d=new_user.d and orders.ctype=new_user.ctype
group by new_user.d;

select d,count(distinct cid)
from tmp.mall_new_user_cid_all_ctype_cname
where d between '2020-08-01' and '2020-08-09'
and ctype in(1,2)
and cname='APP_SUV'
group by d;

--2.2 3天内付款占比
select new_user.d,count(distinct new_user.cid)
from
(select *
from tmp.mall_new_user_cid_all_ctype_cname
where d between '2020-08-01' and '2020-08-09'
and ctype in(1,2)
and cname='APP_SUV') as new_user
join
iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.cid=new_user.cid
join (
    select uid,all_price,to_date(ordertime) as d,ctype
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where mall_type=1
    and ctype in(1,2)
    and all_price>0
    and order_status in(1,2,3)
    and biz_type in(1,3)) as orders
on maps.uid=orders.uid and orders.ctype=new_user.ctype
where orders.d between new_user.d and date_add(new_user.d,2)
group by new_user.d;



--3.详情页占比
select visit.d,count(visit.cid)
from
(select distinct d,cid,get_json_object(args,'$.spu')
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-01' and '2020-08-09'
and id in(379,267)
    ) as visit
join
    (select *
from tmp.mall_new_user_cid_all_ctype_cname
where d between '2020-08-01' and '2020-08-09'
and ctype in(1,2)
and cname='APP_SUV') as new_user
on visit.d=new_user.d and new_user.cid=visit.cid
group by visit.d;



--4.人均详情页数
