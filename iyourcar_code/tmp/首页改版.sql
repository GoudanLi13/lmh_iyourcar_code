--运营改版
--
select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_special_info;
select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info;
select args
from iyourcar_dw.dwd_all_action_hour_log
where d = '2020-07-30'
  and id in (11339, 11338, 11340, 11341)
  and split(get_json_object(args, '$.gid'), '#')[1] = '416674'
  and get_json_object(args, '$.redirect_target')=27;



--0.改版热力图

--新版本
select d,get_json_object(args,'$.redirect_type'),get_json_object(args,'$.redirect_target'),count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-05' and '2020-08-05'
and id in(11339,11338,11340,11341)
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
where d between '2020-08-05' and '2020-08-05'
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
where d between '2020-07-21' and '2020-08-03'
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
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id in(11338,11339)
group by d;

--1.4.人均第一次点击的时间的间隔（有点击行为的人）每日在首页的第一次曝光-第一次点击

--旧版本

select d,avg(dif)
from
(select log_s.d,log_s.session,log_s.cid,log_e.st-log_s.st as dif
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
where log_s.st<log_e.st) as a
group by d
;

--新版本


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
where d between '2020-07-21' and '2020-08-03'
and cname='WXAPP_YCYH_PLUS'
and split(get_json_object(args,'$.gid'),'#')[1]='734867'
and id=11339
group by d;

--1.6

--产品改版