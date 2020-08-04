-- 小程序开屏
-- id =  11511 11512 gid = "501#83"
-- 曝光
select d,count(distinct cid) as `小程序弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11511,11512)  and d between '2020-06-01' and '2020-06-18' and ctype = 4
and get_json_object(args,"$.gid") in("501#83","501#84")
group by d;


select d,count(distinct cid) as `小程序机油专场弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11511,11512)  and d between '2020-06-01' and '2020-06-18' and ctype = 4
and get_json_object(args,"$.gid") in("501#27","501#32")
group by d;

select d,count(distinct cid) as `小程序爱车专场弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11511,11512)  and d between '2020-06-01' and '2020-06-18' and ctype = 4
and get_json_object(args,"$.gid") in("501#28","501#33")
group by d;

select d,count(distinct cid) as `小程序舒适用车专场弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11511,11512)  and d between '2020-06-01' and '2020-06-18' and ctype = 4
and get_json_object(args,"$.gid") in("501#112","501#67")
group by d;

-- 点击
select d,count(distinct cid) as `小程序弹窗点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11507,11508)  and d between '2020-06-01' and '2020-06-18' and ctype = 4
and get_json_object(args,"$.gid") in("501#83","501#84")
group by d;
select d,count(distinct cid) as `小程序机油专场弹窗点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11507,11508)  and d between '2020-06-01' and '2020-06-18' and ctype = 4
and get_json_object(args,"$.gid") in("501#27","501#32")
group by d;
select d,count(distinct cid) as `小程序爱车专场弹窗点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11507,11508)  and d between '2020-06-01' and '2020-06-18' and ctype = 4
and get_json_object(args,"$.gid") in("501#28","501#33")
group by d;
select d,count(distinct cid) as `小程序舒适用车专场弹窗点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11507,11508)  and d between '2020-06-01' and '2020-06-18' and ctype = 4
and get_json_object(args,"$.gid") in("501#112","501#67")
group by d;


select action.d,count(distinct action.uid ) as `下单人数`,sum(all_price/100) as `GMV` from
(select distinct uid,d from
iyourcar_dw.dwd_all_action_hour_log
where id in (11507,11508)  and d between '2020-06-01' and '2020-06-18' and ctype =4
and get_json_object(args,"$.gid") in("501#27","501#32")) as action
inner join
(select substr(ordertime,0,10) as d,uid,all_price from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-18' and ctype = 4 and order_status  in (1,2,3))
as orders
on action.uid = orders.uid and action.d = orders.d
group by action.d;


select action.d,count(distinct action.uid ) as `小程序爱车弹窗下单人数`,sum(all_price/100) as `GMV` from
(select distinct uid,d from
iyourcar_dw.dwd_all_action_hour_log
where id in (11507,11508)  and d between '2020-06-01' and '2020-06-18' and ctype =4
and get_json_object(args,"$.gid") in("501#28","501#33")) as action
inner join
(select substr(ordertime,0,10) as d,uid,all_price from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-18' and ctype = 4 and order_status  in (1,2,3))
as orders
on action.uid = orders.uid and action.d = orders.d
group by action.d;


select action.d,count(distinct action.uid ) as `小程序舒适用车弹窗下单人数`,sum(all_price/100) as `GMV` from
(select distinct uid,d from
iyourcar_dw.dwd_all_action_hour_log
where id in (11507,11508)  and d between '2020-06-01' and '2020-06-18' and ctype =4
and get_json_object(args,"$.gid") in("501#112","501#67")) as action
inner join
(select substr(ordertime,0,10) as d,uid,all_price from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-18' and ctype = 4 and order_status  in (1,2,3))
as orders
on action.uid = orders.uid and action.d = orders.d
group by action.d;

--APP开屏
-- id = 11509 11510 gid = "501#83"
-- 曝光人数
select d,count(distinct cid) as `APP弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11509,11510)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#83"
group by d;
select d,count(distinct cid) as `APP专场弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11509,11510)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#27"
group by d;
select d,count(distinct cid) as `APP爱车专场弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11509,11510)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#28"
group by d;
select d,count(distinct cid) as `APP舒适用车专场弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11509,11510)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#112"
group by d;
--点击人数
select d,count(distinct cid) as `APP弹窗点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11505,11506)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#83"
group by d;
select d,count(distinct cid) as `APP专场弹窗点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11505,11506)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#27"
group by d;
select d,count(distinct cid) as `APP专场弹窗点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11505,11506)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#28"
group by d;
select d,count(distinct cid) as `APP舒适用车专场弹窗点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11505,11506)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#112"
group by d;


--GMV.下单人数
select action.d,count(distinct action.uid ) as `下单人数`,sum(all_price/100) as `GMV` from
(select distinct uid,d from
iyourcar_dw.dwd_all_action_hour_log
where id in (11505,11506)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#27" ) as action
inner join
(select substr(ordertime,0,10) as d,uid,all_price from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-18' and ctype in (1,2) and order_status  in (1,2,3))
as orders
on action.uid = orders.uid and action.d = orders.d
group by action.d;


select action.d,count(distinct action.uid ) as `app爱车专场下单人数`,sum(all_price/100) as `GMV` from
(select distinct uid,d from
iyourcar_dw.dwd_all_action_hour_log
where id in (11505,11506)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#28" ) as action
inner join
(select substr(ordertime,0,10) as d,uid,all_price from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-18' and ctype in (1,2) and order_status  in (1,2,3))
as orders
on action.uid = orders.uid and action.d = orders.d
group by action.d;


select action.d,count(distinct action.uid ) as `app舒适用车专场下单人数`,sum(all_price/100) as `GMV` from
(select distinct uid,d from
iyourcar_dw.dwd_all_action_hour_log
where id in (11505,11506)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#112" ) as action
inner join
(select substr(ordertime,0,10) as d,uid,all_price from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-18' and ctype in (1,2) and order_status  in (1,2,3))
as orders
on action.uid = orders.uid and action.d = orders.d
group by action.d;

--求各端商城首页访问人数
select d,count(distinct cid) as `APP商城首页` from iyourcar_dw.dwd_all_action_hour_log
where id in (242,363)  and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
group by d
select d,count(distinct cid) as `小程序商城首页` from iyourcar_dw.dwd_all_action_hour_log
where id in (293,1016)  and d between '2020-06-01' and '2020-06-18'
group by d;


select open.d,count(distinct open.cid) as `小程序广告图扫码人数` from
(select * from iyourcar_dw.dwd_all_action_hour_log
where id in (316)  and d between '2020-06-01' and '2020-06-18' and ctype = 4
and get_json_object(args,"$.scene_id") in (1047,1048,1049)
    )
    as open
inner join
(select * from iyourcar_dw.dwd_all_action_hour_log where id = 293 and d between '2020-06-01' and '2020-06-18' and ctype = 4 ) as mall
on open.cid = mall.cid and open.d = mall.d
group by open.d;

--GMV.下单人数
select action.d,count(distinct action.uid ) as `下单人数`,sum(all_price/100) as `GMV` from
(
    select open.uid,open.d from
    (select cid,uid,d from iyourcar_dw.dwd_all_action_hour_log
where id in (316)  and d between '2020-06-01' and '2020-06-18' and ctype = 4
and get_json_object(args,"$.scene_id") in (1047,1048,1049)
    )
    as open
inner join
(select cid,uid,d from iyourcar_dw.dwd_all_action_hour_log where id = 293 and d between '2020-06-01' and '2020-06-18' and ctype = 4 ) as mall
on open.cid = mall.cid and open.d = mall.d) as action
inner join
(select substr(ordertime,0,10) as d,uid,all_price from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-18' and ctype in (1,2) and order_status  in (1,2,3))
as orders
on action.uid = orders.uid and action.d = orders.d
group by action.d;

select *
from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info;