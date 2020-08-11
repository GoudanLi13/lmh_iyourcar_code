--运营改版
--
select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_special_info;
select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info;
select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info;
select args
from iyourcar_dw.dwd_all_action_hour_log
where d = '2020-07-30'
  and id in (11339, 11338, 11340, 11341)
  and split(get_json_object(args, '$.gid'), '#')[1] = '416674'
  and get_json_object(args, '$.redirect_target')=27;



--0.改版热力图

--旧版本
select type,page,avg(ro)
from
(select d,get_json_object(args,'$.redirect_type') as type,get_json_object(args,'$.redirect_target') as page,
       count(distinct case when id=11339 then cid end)/count(distinct cid) as ro
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and id in(11339,11338)
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
group by d,get_json_object(args,'$.redirect_type'),get_json_object(args,'$.redirect_target'))
as a group by type,page;

--新版本
select d,get_json_object(args,'$.redirect_type'),get_json_object(args,'$.redirect_target'),
       count(distinct cid),
       count(distinct case when id=11339 then cid end)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-09'
and id in(11339,11338)
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
group by d,get_json_object(args,'$.redirect_type'),get_json_object(args,'$.redirect_target');

--1.首页部分
--1.1.跳失率（大型专题曝光或点击事件的人）
--旧版本
select
       d,
       count(distinct cid),
       count(distinct case when (get_json_object(args,'$.redirect_type')=520 and get_json_object(args,'$.redirect_target')=1) or id=11339 then cid end)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id in(11338,11339)
group by d;

--新版本
select
       d,
       count(distinct cid),
       count(distinct case when (get_json_object(args,'$.redirect_type')=206 and get_json_object(args,'$.redirect_target')=734915) or id=11339 then cid end)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-09'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id in(11338,11339)
group by d;

--1.2.人均首页item曝光百分比（大型专题曝光或点击事件的人）

--旧版本（47个item）
select d,avg(page_num)
from
(select d,cid,count(page) page_num
from
(select distinct
       d,
       cid,
       get_json_object(args,'$.redirect_type'),
        get_json_object(args,'$.redirect_target') as page
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id =11338) as log
group by d,cid) as a
group by d
;


--新版本（25个item）
select d,avg(page_num)
from
(select d,cid,count(page) page_num
from
(select distinct
       d,
       cid,
       get_json_object(args,'$.redirect_type'),
        get_json_object(args,'$.redirect_target') as page
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-09'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id =11338) as log
group by d,cid) as a
group by d
;

--1.3 点击人数占比
--旧版本
select
       d,
       count(distinct cid),
       count(distinct case when id=11339 then cid end)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id in(11338,11339)
group by d;

--新版本
select
       d,
       count(distinct cid),
       count(distinct case when id=11339 then cid end)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-09'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id in(11338,11339)
group by d;

--1.4.人均第一次点击的时间的间隔（有点击行为的人）每日在首页的第一次曝光-第一次点击

--旧版本

select d,avg(dif)
from
(select log_s.d,log_s.session,log_s.cid,(log_e.st-log_s.st)/1000 as dif,row_number() over (partition by log_s.d,log_s.cid order by log_s.st) as rank
from
(select
       d,
       cid,
        session,
       min(st) as st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id =11338
group by d,cid,session
 ) as log_s
join
(select
       d,
       cid,
        session,
       min(st) as st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id =11339
group by d,cid,session
 ) as log_e
on log_e.cid=log_s.cid and log_s.d=log_e.d and log_e.session=log_s.session
where log_s.st<log_e.st
) as a
where rank=1
group by d
;

--新版本

select d,avg(dif)
from
(select log_s.d,log_s.session,log_s.cid,(log_e.st-log_s.st)/1000 as dif,row_number() over (partition by log_s.d,log_s.cid order by log_s.st) as rank
from
(select
       d,
       cid,
        session,
       min(st) as st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-09'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id =11338
group by d,cid,session
 ) as log_s
join
(select
       d,
       cid,
        session,
       min(st) as st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-09'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id =11339
group by d,cid,session
 ) as log_e
on log_e.cid=log_s.cid and log_s.d=log_e.d and log_e.session=log_s.session
where log_s.st<log_e.st
) as a
where rank=1
group by d
;

--1.5.人均点击(有点击行为的人)

--旧版本
select d,count(distinct cid),count(cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id=11339
group by d;

--新版本
select d,count(distinct cid),count(cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-09'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id=11339
group by d;

--1.6.人均点击(在首页有曝光或点击的人)

--旧版本
select d,count(distinct cid),count(case when id=11339 then cid end)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id in(11338,11339)
group by d;

--新版本
select d,count(distinct cid),count(case when id=11339 then cid end)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-09'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id in(11338,11339)
group by d;

--产品改版
--2.1 人均详情页访问（有访问过首页的人,1人访问1个详情页就算1次）

--旧版本
select visit.d,count(distinct visit.cid),count(detail.spu)
from
(select distinct d,cid
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id in(11338,11339)) as visit
left join
(
    select distinct d,cid,get_json_object(args,'$.spu') as spu
    from iyourcar_dw.dwd_all_action_hour_log
    where d between '2020-07-21' and '2020-08-03'
    and cname='WXAPP_YCYH_PLUS'
    and id=302
    ) as detail
on visit.cid=detail.cid and visit.d=detail.d
group by visit.d
;

--新版本
select visit.d,count(distinct visit.cid),count(detail.spu)
from
(select distinct d,cid
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-09'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id in(11338,11339)) as visit
left join
(
    select distinct d,cid,get_json_object(args,'$.spu') as spu
    from iyourcar_dw.dwd_all_action_hour_log
    where d between '2020-08-05' and '2020-08-09'
    and cname='WXAPP_YCYH_PLUS'
    and id=302
    ) as detail
on visit.cid=detail.cid and visit.d=detail.d
group by visit.d
;


--2.2 人均GMV

--旧版本
select visit.d,count(distinct visit.cid),sum(orders.all_price)/100
from
    (select distinct d,cid
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id in(11338,11339)) as visit
left join
iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.cid=visit.cid
left join
(
    select uid,all_price,to_date(ordertime) as d
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where mall_type=1
    and ctype=4
    and all_price>0
    and order_status in(1,2,3)
    and biz_type in(1,3)
    ) as orders
on maps.uid=orders.uid and visit.d=orders.d
group by visit.d
;

--新版本

select visit.d,count(distinct visit.cid),sum(orders.all_price)/100
from
    (select distinct d,cid
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-09'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id in(11338,11339)) as visit
left join
iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.cid=visit.cid
left join
(
    select uid,all_price,to_date(ordertime) as d
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where mall_type=1
    and ctype=4
    and all_price>0
    and order_status in(1,2,3)
    and biz_type in(1,3)
    ) as orders
on maps.uid=orders.uid and visit.d=orders.d
group by visit.d
;

------新用户
--1.首页部分
--1.1.跳失率（大型专题曝光或点击事件的人）
--旧版本
select
       log.d,
       count(distinct log.cid),
       count(distinct case when (get_json_object(args,'$.redirect_type')=520 and get_json_object(args,'$.redirect_target')=1) or id=11339 then log.cid end)
from
(select *
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id in(11338,11339)) as log
join tmp.mall_new_user_cid_all_ctype_cname as new
on new.cid=log.cid and new.d=log.d and log.ctype=new.ctype and log.cname=new.cname
group by log.d;

--新版本


select
       log.d,
       count(distinct log.cid),
       count(distinct case when (get_json_object(args,'$.redirect_type')=206 and get_json_object(args,'$.redirect_target')=734915) or id=11339 then log.cid end)
from
(select *
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-10'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id in(11338,11339)) as log
join tmp.mall_new_user_cid_all_ctype_cname as new
on new.cid=log.cid and new.d=log.d and log.ctype=new.ctype and log.cname=new.cname
group by log.d;



--1.3 点击人数占比
--旧版本
select
       log.d,
       count(distinct log.cid),
       count(distinct case when id=11339 then log.cid end)
from
(select *
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id in(11338,11339)) as log
join tmp.mall_new_user_cid_all_ctype_cname as new
on new.cid=log.cid and new.d=log.d and log.ctype=new.ctype and log.cname=new.cname
group by log.d;

--新版本
select
       log.d,
       count(distinct log.cid),
       count(distinct case when id=11339 then log.cid end)
from
(select *
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-10'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id in(11338,11339)) as log
join tmp.mall_new_user_cid_all_ctype_cname as new
on new.cid=log.cid and new.d=log.d and log.ctype=new.ctype and log.cname=new.cname
group by log.d;

--1.5.人均点击(有点击行为的人)

--旧版本
select
       log.d,
       count(distinct log.cid),
       count(log.cid)
from
(select *
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id=11339) as log
join tmp.mall_new_user_cid_all_ctype_cname as new
on new.cid=log.cid and new.d=log.d and log.ctype=new.ctype and log.cname=new.cname
group by log.d;

--新版本

select
       log.d,
       count(distinct log.cid),
       count(log.cid)
from
(select *
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-10'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id=11339) as log
join tmp.mall_new_user_cid_all_ctype_cname as new
on new.cid=log.cid and new.d=log.d and log.ctype=new.ctype and log.cname=new.cname
group by log.d;

--1.6.人均点击(在首页有曝光或点击的人)

--旧版本

select
       log.d,
       count(distinct log.cid),
       count(case when id=11339 then log.cid end)
from
(select *
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='416674'
and id in(11338,11339)) as log
join tmp.mall_new_user_cid_all_ctype_cname as new
on new.cid=log.cid and new.d=log.d and log.ctype=new.ctype and log.cname=new.cname
group by log.d;

--新版本

select
       log.d,
       count(distinct log.cid),
       count(case when id=11339 then log.cid end)
from
(select *
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-10'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id in(11338,11339)) as log
join tmp.mall_new_user_cid_all_ctype_cname as new
on new.cid=log.cid and new.d=log.d and log.ctype=new.ctype and log.cname=new.cname
group by log.d;