--1.查看近半个月的黑金卡特权点击率
--1.1.商城页面


select b.name,sum(user)/14
from
(select d,get_json_object(args,'$.welfare_id') as welfare_id,count(distinct cid) as user
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-22' and '2020-08-04'
and id in(542,370)
group by d,get_json_object(args,'$.welfare_id')) as a
left join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_category_item as b
on a.welfare_id=b.id
group by b.name
;

--1.2.权益页面

select b.name,sum(user)/14
from
(select d,get_json_object(args,'$.welfare_id') as welfare_id,count(distinct cid) as user
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-22' and '2020-08-04'
and id in(11190,11189)
group by d,get_json_object(args,'$.welfare_id')) as a
left join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_category_item as b
on a.welfare_id=b.id
group by b.name
;


--2.查看近半个月点击权益人数/权益曝光人数

--2.1.商城页面
select sum(user)/14
from
(select d,count(distinct cid) as user
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-22' and '2020-08-04'
and id in(542,370)
group by d) as a
;

--2.2 权益页面
select sum(user)/14
from
(select d,count(distinct cid) as user
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-22' and '2020-08-04'
and id in(11190,11189)
group by d) as a
;