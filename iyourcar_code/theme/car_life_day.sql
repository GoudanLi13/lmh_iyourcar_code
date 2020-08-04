--活动前
set hive.execution.engine=mr;
--商品售卖情况历史数据(60d)
select t2.item_id as `商品id`, t2.item_sku_id as `商品sku_id`,count(*) as `售卖数量`,
       count(distinct substr(t2.createtime,0,10)) as `售卖天数`,sum(t2.all_price)/100 as `总GMV`,
       sum(t2.all_price)/100/sum(t2.item_num) as `均价`,sum(t2.all_deduction_score) as `实际支付的有车币/声望`,
       avg(t2.all_deduction_score)/sum(t2.item_num) as `平均实际支付的有车币/声望` from
(
	select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
	where ordertime between "2019-11-11 00:00:00" and "2020-05-15 00:00:00" and order_status in (1,2,3) and biz_type  in( 1,3)
)
as t1
inner join
(
	select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
	where createtime between "2019-11-11 00:00:00" and "2020-05-15 00:00:00"
	and item_id  in
              (908,1709,1760,848,964,480,1774,1727,1744,1660,1674,1762,472,904,973,707,397,1758,852,1619,889)
	)
as t2
on t1.order_no = t2.order_no
group by t2.item_id , t2.item_sku_id ;
-- 过去90天销售情况
select t2.item_id as `商品id`, t2.item_sku_id as `商品sku_id`,count(*) as `售卖数量`,
       count(distinct substr(t2.createtime,0,10)) as `售卖天数`,sum(t2.all_price)/100 as `总GMV`,
       sum(t2.all_price)/100/sum(t2.item_num) as `均价` ,sum(t2.all_deduction_score) as `实际支付的有车币/声望`,
       avg(t2.all_deduction_score)/sum(t2.item_num) as `平均实际支付的有车币/声望` from
    (
        select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where ordertime between "2020-02-11 00:00:00" and "2020-05-15 00:00:00" and order_status in (1,2,3) and biz_type  in( 1,3)
    )
        as t1
        inner join
    (
        select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
        where createtime between "2020-02-11 00:00:00" and "2020-05-11 00:00:00"
          and item_id  in
              (908,1709,1760,848,964,480,1774,1727,1744,1660,1674,1762,472,904,973,707,397,1758,852,1619,889)
    )
        as t2
    on t1.order_no = t2.order_no
group by t2.item_id, t2.item_sku_id;



-- 探究价格波动对下单量的影响
select t2.item_id as `商品id`, t2.item_sku_id as `商品sku_id`, substr(t2.createtime,0,10) as `下单日期`,
       sum(t2.item_num) as `售卖数量`, count(distinct t2.uid) as `下单人数`, count(distinct substr(t2.createtime,0,10)) as `售卖天数`,
       sum(t2.all_price)/100 as `总GMV`, sum(t2.all_price)/100/sum(t2.item_num) as `当日售价`, avg(t2.all_deduction_score)/sum(t2.item_num) as `平均实际支付的有车币/声望`
from
(
	select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
	where ordertime between "2019-11-13 00:00:00" and "2020-05-14 00:00:00" and order_status in (1,2,3) and biz_type  in( 1,3)
)
as t1
inner join
(
	select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
	where createtime between "2020-05-13 00:00:00" and "2020-05-14 00:00:00"
-- 	and item_id  in
-- 	(397,883,796,445,812,829,831,737,758,472,655,657,725,845,
-- 	848,741,709,639,980,707,564,1092,460,465,652,760,921,790,939,
-- 	918,914,916,764,753,729,711,375,1774,1016,989,973,971,966,962,
-- 	1403,429,427,964,649,637,913,907,910,959,904,735,908,644,1149,1127,482,1702,721,
-- 	713,890,825,821,816,842,817,889,884,887,850,856,852)
	)
as t2
on t1.order_no = t2.order_no
inner join (select distinct item_id,group_id from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_group where group_id = 39) as cp
on t2.item_id = cp.item_id
group by t2.item_id as `商品id`, t2.item_sku_id as `商品sku_id`substr(t2.createtime,0,10);

set mapreduce.job.queuename = dailyDay;
desc iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item;


select * from tmp.rpt_official_comparative_analysis;
select distinct t2.item_id,item_sku_id,item_name,item_sku_info from
    (
	select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
	where createtime between "2019-11-13 00:00:00" and "2020-05-14 00:00:00"
	)
as t2
inner join (select distinct item_id,group_id from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_group where group_id = 39) as cp
on t2.item_id = cp.item_id;


--活动中
-- set mapreduce.job.queuename = dailyDay;
select d,count(distinct cid) as `APP弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11509,11510)  and d between '2020-05-16' and '2020-05-21' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#109"
group by d;
select d,count(distinct cid) as `小程序弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11511)  and d between '2020-05-16' and '2020-05-21' and ctype = 4
and get_json_object(args,"$.gid") = "501#109"
group by d;
set mapreduce.job.queuename = dailyDay;
select d,count(distinct cid) as `APPbanner曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id = 11341 and d between '2020-05-15' and '2020-05-21' and ctype in (1,2)
and get_json_object(args,"$.card_box_type") = "5"
and get_json_object(args,"$.redirect_target") = "109"
group by d;
select d,count(distinct cid) as `小程序banner曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21' and ctype = 4
and get_json_object(args,"$.card_box_type") = "5"
and ((get_json_object(args,"$.redirect_target") = "109" and cname = "WXAPP_YCYH_PLUS") or (get_json_object(args,"$.redirect_target") = "110" and cname = "WXAPP_YCQKJ"))
group by d;
select d,count(distinct cid) as `APP卡片曝光人数`
from iyourcar_dw.dwd_all_action_hour_log
where id in (11341)  and d between '2020-05-16' and '2020-05-21' and ctype in(1,2)
and get_json_object(args,"$.card_box_type") = "1"
and get_json_object(args,"$.redirect_target") = "503383"
group by d;
select d,count(distinct cid) as `APP卡片点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11340)  and d between '2020-05-16' and '2020-05-21' and ctype in (1,2)
and get_json_object(args,"$.redirect_target") = "503383"
and get_json_object(args,"$.card_box_type") = "1"
group by d;
select d,count(distinct cid) as `小程序卡片曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21' and ctype = 4
and get_json_object(args,"$.card_box_type") = "1"
and ((get_json_object(args,"$.redirect_target") = "503383" and cname = "WXAPP_YCYH_PLUS") or (get_json_object(args,"$.redirect_target") = "503382" and cname = "WXAPP_YCQKJ"))
group by d;
select open.d,count(distinct open.cid) as `小程序广告图扫码人数` from (select * from iyourcar_dw.dwd_all_action_hour_log
where id in (316)  and d between '2020-05-16' and '2020-05-21' and ctype = 4
and get_json_object(args,"$.scene_id") in (1047,1048,1049)) as open
inner join
(select * from iyourcar_dw.dwd_all_action_hour_log where id = 293 and d between '2020-05-16' and '2020-05-21' and ctype = 4 ) as mall
on open.cid = mall.cid and open.session = mall.session
group by open.d;


-- 曝光-下单人数-活动商品
select orders.d,(sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,count(distinct orders.uid) as `APP弹窗曝光下单人数` from
(select distinct d,uid from iyourcar_dw.dwd_all_action_hour_log
where id in (11509,11510)  and d between '2020-05-16' and '2020-05-21' and ctype in (1,2)
and get_json_object(args,"$.gid") = "501#109") as t1
inner join
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21' and order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid and t1.d =orders.d
inner join
    (
select *,substr(createtime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
and item_id in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889)
        ) as items
on orders.order_no = items.order_no
group by orders.d;
select orders.d,(sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,count(distinct orders.uid) as `小程序弹窗曝光下单人数` from
    (select distinct d,uid from iyourcar_dw.dwd_all_action_hour_log
where id in (11511)  and d between '2020-05-16' and '2020-05-21' and ctype = 4
and get_json_object(args,"$.gid") = "501#109") as t1
inner join
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21' and order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid and t1.d =orders.d
inner join
    (
        select *,substr(createtime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
        where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
        and item_id in
            (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
    ) as items
    on orders.order_no = items.order_no
group by orders.d;
select orders.d,(sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,count(distinct orders.uid) as `APPbanner曝光下单人数` from
 ( select distinct d,uid from iyourcar_dw.dwd_all_action_hour_log
where id = 11341 and d between '2020-05-15' and '2020-05-21' and ctype in (1,2)
and get_json_object(args,"$.card_box_type") = "5"
and get_json_object(args,"$.redirect_target") = "109"
) as t1
inner join
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'  and order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid and t1.d =orders.d
inner join
 (
     select *,substr(createtime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
     where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
       and item_id in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889)
 ) as items
 on orders.order_no = items.order_no
group by orders.d;
select orders.d,(sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,count(distinct orders.uid) as `小程序banner曝光下单人数`from (
 select distinct d,uid from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21' and ctype = 4
and get_json_object(args,"$.card_box_type") = "5"
and ((get_json_object(args,"$.redirect_target") = "109" and cname = "WXAPP_YCYH_PLUS") or (get_json_object(args,"$.redirect_target") = "110" and cname = "WXAPP_YCQKJ"))
    ) as t1
    inner join
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21' and  order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid and t1.d =orders.d
    inner join
  (
      select *,substr(createtime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
      where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
        and item_id in
            (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
  ) as items
  on orders.order_no = items.order_no
group by orders.d;
select orders.d,(sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,count(distinct orders.uid)as `APP卡片点击下单人数` from
(select distinct d,uid from iyourcar_dw.dwd_all_action_hour_log
where id in (11340)  and d between '2020-05-16' and '2020-05-21' and ctype in(1,2)
and get_json_object(args,"$.card_box_type") = "1"
and get_json_object(args,"$.redirect_target") = "503383") as t1
inner join
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21' and order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid and t1.d =orders.d
inner join
(
    select *,substr(createtime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
    where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
      and item_id in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889)
) as items
on orders.order_no = items.order_no
group by orders.d;
select orders.d,(sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,count(distinct orders.uid)as `小程序卡片曝光下单人数` from ( select distinct d,uid from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21' and ctype = 4
and get_json_object(args,"$.card_box_type") = "1"
and ((get_json_object(args,"$.redirect_target") = "503383" and cname = "WXAPP_YCYH_PLUS") or (get_json_object(args,"$.redirect_target") = "503382" and cname = "WXAPP_YCQKJ")))as t1
inner join
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'  and order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid and t1.d =orders.d
inner join
(
  select *,substr(createtime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
  where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
    and item_id in
        (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no
group by orders.d;


select orders.d,(sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,
       count(distinct orders.uid)as `小程序广告图扫码下单人数` from
    (select distinct open.d,open.uid from
(select * from iyourcar_dw.dwd_all_action_hour_log
where id in (316)  and d between '2020-05-16' and '2020-05-21' and ctype = 4
and get_json_object(args,"$.scene_id") in (1047,1048,1049)) as open
inner join
(select * from iyourcar_dw.dwd_all_action_hour_log where id = 293 and d between '2020-05-16' and '2020-05-21' and ctype = 4 ) as mall
on open.cid = mall.cid and open.session = mall.session) as t1
inner join
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'  and order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid and t1.d =orders.d
inner join
(
  select *,substr(createtime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
  where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
    and item_id in
        (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no
group by orders.d;




-- 商品销售情况
select t1.*,t2.`APP商品点击人数`,t3.`APP商品详情页人数`,t4.`APP商品详情页人均停留时长`,t5.`APP商品加购人数`,t6.`APP商品下单人数_约束加购` from
(
	select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `APP商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
	where id in (11341) and d between '2020-05-16' and '2020-05-21'
	and (
	(cname = 'APP_SUV' and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
	)
	group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `APP商品点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11340) and d between '2020-05-16' and '2020-05-21'
and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889)
group by d,get_json_object(args,"$.redirect_target")
) as t2
on t1.d = t2.d and t1.`商品spu`= t2.`商品spu`
left join
(
    select d,get_json_object(args,"$.spu") as `商品spu`,count(distinct cid) as  `APP商品详情页人数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 29) as e_group
    on action.id = e_group.event_id
    where action.d between '2020-05-16' and '2020-05-21'  and ctype in (1,2)
    and
    (
		get_json_object(args,"$.spu") in
		(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889)
	)
    group by d,get_json_object(args,"$.spu")
) as t3
on t2.d = t3.d and t2.`商品spu`= t3.`商品spu`
left join
(
select d,get_json_object(args,"$.spu") as `商品spu`,sum(et-st)/count(distinct cid) as  `APP商品详情页人均停留时长` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 29) as e_group
    on action.id = e_group.event_id
    where action.d between '2020-05-16' and '2020-05-21'  and ctype in (1,2)
    and
    (
		get_json_object(args,"$.spu") in
		(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889)
	)
    group by d,get_json_object(args,"$.spu")
) as t4
on t3.d = t4.d and t3.`商品spu`= t4.`商品spu`
left join
(
select d,sku_spu_map.spu as `商品spu`,sum(if(isnull(cid),0,1)) as  `APP商品加购人数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 26) as e_group
    on action.id = e_group.event_id
    left join
    (select id as sku,item_id as spu from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku where item_id in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
    	as sku_spu_map
    on sku_spu_map.sku  = get_json_object(args,"$.sku")
    where action.d between '2020-05-16' and '2020-05-21'  and ctype in (1,2)
    group by d,sku_spu_map.spu
) as t5
on t4.d = t5.d and t4.`商品spu`= t5.`商品spu`
left join
(
select orders.d,items.item_id as `商品spu` ,count(distinct orders.uid) as `APP商品下单人数_约束加购` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'  and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-05-16' and '2020-05-21' and item_id in
		(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t6
on t5.d = t6.d and t5.`商品spu`= t6.`商品spu`;




-- 商品-下单-GMV
select t1.*,t6.`APP商品下单人数`,t6.`GMV` from
(
	select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `APP商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
	where id in (11341) and d between '2020-05-16' and '2020-05-21'
	and (
	(cname = 'APP_SUV' and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
	)
	group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
    (
select orders.d,items.item_id as `商品spu` ,count(distinct orders.uid) as `APP商品下单人数`,(sum(items.all_price-items.all_deduction_price))/100 as `GMV` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'  and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-05-16' and '2020-05-21' and item_id in
		(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t6
on t1.d = t6.d and t1.`商品spu`= t6.`商品spu`;




-- 小程序端商品销售情况
--set mapreduce.job.queuename = dailyDay;
select t1.*,t2.`小程序商品点击人数`,t3.`小程序商品详情页人数`,t4.`小程序商品详情页人均停留时长`,t5.`小程序商品加购人数`,t6.`小程序商品下单人数` from
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21'
and (
(cname = 'WXAPP_YCYH_PLUS' and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
or
(cname = 'WXAPP_YCQKJ' and get_json_object(args,"$.redirect_target") in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
)
group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11339,11828)  and d between '2020-05-16' and '2020-05-21'
and (
(cname = 'WXAPP_YCYH_PLUS' and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
or
(cname = 'WXAPP_YCQKJ' and get_json_object(args,"$.redirect_target") in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
)
group by d,get_json_object(args,"$.redirect_target")
) as t2
on t1.d = t2.d and t1.`商品spu`= t2.`商品spu`
left join
(
    select d,get_json_object(args,"$.spu") as `商品spu`,count(distinct cid) as  `小程序商品详情页人数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 29) as e_group
    on action.id = e_group.event_id
    where action.d between '2020-05-16' and '2020-05-21'  and ctype = 4
    and
    (
    	get_json_object(args,"$.spu") in
    	(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
    )
    group by d,get_json_object(args,"$.spu")
) as t3
on t2.d = t3.d and t2.`商品spu`= t3.`商品spu`
left join
(
    select d,get_json_object(args,"$.spu") as `商品spu`,sum(et-st)/count(distinct cid) as  `小程序商品详情页人均停留时长` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 29) as e_group
    on action.id = e_group.event_id
    where action.d between '2020-05-16' and '2020-05-21'  and ctype = 4
    and
    (
    	get_json_object(args,"$.spu") in
    	(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
    )
    group by d,get_json_object(args,"$.spu")
) as t4
on t3.d = t4.d and t3.`商品spu`= t4.`商品spu`
left join
(
    select d,sku_spu_map.spu as `商品spu`,sum(if(isnull(cid),0,1)) as  `小程序商品加购人数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 26) as e_group
    on action.id = e_group.event_id
    left join
    (select id as sku,item_id as spu from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku where item_id in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
    	as sku_spu_map
    on sku_spu_map.sku  = get_json_object(args,"$.sku")
    where action.d between '2020-05-16' and '2020-05-21'  and ctype = 4
    group by d,sku_spu_map.spu
) as t5
on t4.d = t5.d and t4.`商品spu`= t5.`商品spu`
left join
(
select orders.d,items.item_id as `商品spu`,count(distinct orders.uid) as `小程序商品下单人数` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'   and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-05-16' and '2020-05-21' and item_id in
		(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t6
on t5.d = t6.d and t5.`商品spu`= t6.`商品spu`;
select t1.*,t6.`小程序商品下单人数`,t6.`GMV` from
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21'
and (
(cname = 'WXAPP_YCYH_PLUS' and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
or
(cname = 'WXAPP_YCQKJ' and get_json_object(args,"$.redirect_target") in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
)
group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
    (
select orders.d,items.item_id as `商品spu`,count(distinct orders.uid) as `小程序商品下单人数`,(sum(items.all_price-items.all_deduction_price))/100 as `GMV` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'   and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-05-16' and '2020-05-21' and item_id in
		(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t6
on t1.d = t6.d and t1.`商品spu`= t6.`商品spu`;




--有车以后+
select t1.*,t2.`小程序商品点击人数`,t3.`小程序商品详情页人数`,t4.`小程序商品详情页人均停留时长`,t5.`小程序商品加购人数`,t6.`小程序商品下单人数` from
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21'
and (
(cname = 'WXAPP_YCYH_PLUS' and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
or
(cname = 'WXAPP_YCQKJ' and get_json_object(args,"$.redirect_target") in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
)
group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11339,11828)  and d between '2020-05-16' and '2020-05-21'
and (
(cname = 'WXAPP_YCYH_PLUS' and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
or
(cname = 'WXAPP_YCQKJ' and get_json_object(args,"$.redirect_target") in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
)
group by d,get_json_object(args,"$.redirect_target")
) as t2
on t1.d = t2.d and t1.`商品spu`= t2.`商品spu`
left join
(
    select d,get_json_object(args,"$.spu") as `商品spu`,count(distinct cid) as  `小程序商品详情页人数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 29) as e_group
    on action.id = e_group.event_id
    where action.d between '2020-05-16' and '2020-05-21'  and ctype = 4
    and
    (
    	get_json_object(args,"$.spu") in
    	(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
    )
    group by d,get_json_object(args,"$.spu")
) as t3
on t2.d = t3.d and t2.`商品spu`= t3.`商品spu`
left join
(
    select d,get_json_object(args,"$.spu") as `商品spu`,sum(et-st)/count(distinct cid) as  `小程序商品详情页人均停留时长` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 29) as e_group
    on action.id = e_group.event_id
    where action.d between '2020-05-16' and '2020-05-21'  and ctype = 4
    and
    (
    	get_json_object(args,"$.spu") in
    	(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
    )
    group by d,get_json_object(args,"$.spu")
) as t4
on t3.d = t4.d and t3.`商品spu`= t4.`商品spu`
left join
(
    select d,sku_spu_map.spu as `商品spu`,sum(if(isnull(cid),0,1)) as  `小程序商品加购人数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 26) as e_group
    on action.id = e_group.event_id
    left join
    (select id as sku,item_id as spu from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku where item_id in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
    	as sku_spu_map
    on sku_spu_map.sku  = get_json_object(args,"$.sku")
    where action.d between '2020-05-16' and '2020-05-21'  and ctype = 4
    group by d,sku_spu_map.spu
) as t5
on t4.d = t5.d and t4.`商品spu`= t5.`商品spu`
left join
(
select orders.d,items.item_id as `商品spu`,count(distinct orders.uid) as `小程序商品下单人数` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'   and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-05-16' and '2020-05-21' and item_id in
		(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t6
on t5.d = t6.d and t5.`商品spu`= t6.`商品spu`;




--有车以后+
select t1.*,t6.`小程序(有车以后+)商品下单人数`,t6.`GMV` from
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21'
and (
(cname = 'WXAPP_YCYH_PLUS' and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
)
group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
    (
select orders.d,items.item_id as `商品spu`,count(distinct orders.uid) as `小程序(有车以后+)商品下单人数`,(sum(items.all_price-items.all_deduction_price))/100 as `GMV` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'   and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-05-16' and '2020-05-21' and item_id in
		(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t6
on t1.d = t6.d and t1.`商品spu`= t6.`商品spu`;



--群空间
select t1.*,t2.`小程序(群空间)商品点击人数`,t3.`小程序商品详情页人数`,t4.`小程序商品详情页人均停留时长`,t5.`小程序商品加购人数`,t6.`小程序商品下单人数` from
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21'
and (
(cname = 'WXAPP_YCQKJ' and get_json_object(args,"$.redirect_target") in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
)
group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序(群空间)商品点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11339,11828)  and d between '2020-05-16' and '2020-05-21'
and (
(cname = 'WXAPP_YCQKJ' and get_json_object(args,"$.redirect_target") in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
)
group by d,get_json_object(args,"$.redirect_target")
) as t2
on t1.d = t2.d and t1.`商品spu`= t2.`商品spu`
left join
(
    select d,get_json_object(args,"$.spu") as `商品spu`,count(distinct cid) as  `小程序商品详情页人数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 29) as e_group
    on action.id = e_group.event_id
    where action.d between '2020-05-16' and '2020-05-21'  and ctype = 4
    and
    (
    	get_json_object(args,"$.spu") in
    	(909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
    )
    group by d,get_json_object(args,"$.spu")
) as t3
on t2.d = t3.d and t2.`商品spu`= t3.`商品spu`
left join
(
    select d,get_json_object(args,"$.spu") as `商品spu`,sum(et-st)/count(distinct cid) as  `小程序商品详情页人均停留时长` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 29) as e_group
    on action.id = e_group.event_id
    where action.d between '2020-05-16' and '2020-05-21'  and ctype = 4
    and
    (
    	get_json_object(args,"$.spu") in
    	(909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
    )
    group by d,get_json_object(args,"$.spu")
) as t4
on t3.d = t4.d and t3.`商品spu`= t4.`商品spu`
left join
(
    select d,sku_spu_map.spu as `商品spu`,sum(if(isnull(cid),0,1)) as  `小程序商品加购人数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 26) as e_group
    on action.id = e_group.event_id
    left join
    (select id as sku,item_id as spu from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku where item_id in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
    	as sku_spu_map
    on sku_spu_map.sku  = get_json_object(args,"$.sku")
    where action.d between '2020-05-16' and '2020-05-21'  and ctype = 4
    group by d,sku_spu_map.spu
) as t5
on t4.d = t5.d and t4.`商品spu`= t5.`商品spu`
left join
(
select orders.d,items.item_id as `商品spu`,count(distinct orders.uid) as `小程序商品下单人数` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'   and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-05-16' and '2020-05-21' and item_id in
		(909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t6
on t5.d = t6.d and t5.`商品spu`= t6.`商品spu`;


--群空间
select t1.*,t6.`小程序(群空间)商品下单人数`,t6.`GMV` from
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21'
and (
(cname = 'WXAPP_YCQKJ' and get_json_object(args,"$.redirect_target") in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
)
group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
    (
select orders.d,items.item_id as `商品spu`,count(distinct orders.uid) as `小程序(群空间)商品下单人数`,(sum(items.all_price-items.all_deduction_price))/100 as `GMV` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'   and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-05-16' and '2020-05-21' and item_id in
		(909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t6
on t1.d = t6.d and t1.`商品spu`= t6.`商品spu`;



--活动后总结
-- 曝光-下单人数-活动商品
select (sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,count(distinct t1.uid) as `人数`,count(distinct orders.uid) as `弹窗曝光下单人数` from
(select distinct uid from iyourcar_dw.dwd_all_action_hour_log
where id in (11509,11510,11511)  and d between '2020-05-16' and '2020-05-21' and ctype in (1,2,4)
and get_json_object(args,"$.gid") = "501#109") as t1
inner join
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21' and order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid
inner join
    (
select *,substr(createtime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
and item_id in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
        ) as items
on orders.order_no = items.order_no;
select (sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,count(distinct t1.uid) as `人数`,count(distinct orders.uid) as `banner曝光下单人数`from (
 select distinct uid from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829)  and d between '2020-05-16' and '2020-05-21' and ctype in (1,2,4)
and get_json_object(args,"$.card_box_type") = "5"
and (
    (get_json_object(args,"$.redirect_target") = "109" and cname = "APP_SUV") or
    (get_json_object(args,"$.redirect_target") = "109" and cname = "WXAPP_YCYH_PLUS") or (get_json_object(args,"$.redirect_target") = "110" and cname = "WXAPP_YCQKJ"))
    ) as t1
    inner join
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21' and  order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid
    inner join
  (
      select *,substr(createtime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
      where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
        and item_id in
            (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
  ) as items
  on orders.order_no = items.order_no;
select (sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,count(distinct t1.uid) as `人数`,count(distinct orders.uid)as `APP卡片点击+小程序卡片曝光下单人数` from
           ( select distinct uid from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829,11340)  and d between '2020-05-16' and '2020-05-21' and ctype in (1,2,4)
and get_json_object(args,"$.card_box_type") = "1"
and (
    (get_json_object(args,"$.redirect_target") = "503383" and cname = "APP_SUV")  or
    (get_json_object(args,"$.redirect_target") = "503383" and cname = "WXAPP_YCYH_PLUS")
        or (get_json_object(args,"$.redirect_target") = "503382" and cname = "WXAPP_YCQKJ")))as t1
inner join
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'  and order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid
inner join
(
  select *,substr(createtime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
  where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
    and item_id in
        (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no;


select (sum(items.all_price-items.all_deduction_price))/100 as `产生GMV`,
       count(distinct t1.uid) as `人数`,
       count(distinct orders.uid)as `小程序广告图扫码下单人数` from
    (select distinct open.uid from
(select * from iyourcar_dw.dwd_all_action_hour_log
where id in (316)  and d between '2020-05-16' and '2020-05-21' and ctype = 4
and get_json_object(args,"$.scene_id") in (1047,1048,1049)) as open
inner join
(select * from iyourcar_dw.dwd_all_action_hour_log where id = 293 and d between '2020-05-16' and '2020-05-21' and ctype = 4 ) as mall
on open.cid = mall.cid and open.session = mall.session) as t1
inner join
(
select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'  and order_status  in (1,2,3)
) as orders
on t1.uid = orders.uid
inner join
(
  select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
  where substr(createtime,0,10) between '2020-05-16' and '2020-05-21'
    and item_id in
        (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no;


--有车以后+
select t6.`商品名称`,t1.`小程序商品曝光人数`,t2.`小程序商品点击人数`,t6.`小程序商品下单人数`,t6.`GMV` from
(
select get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829,11341)  and d between '2020-05-16' and '2020-05-21'
and (
((cname = 'WXAPP_YCYH_PLUS' or cname = 'APP_SUV') and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
or
(cname = 'WXAPP_YCQKJ' and get_json_object(args,"$.redirect_target") in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
)
group by get_json_object(args,"$.redirect_target")
) as t1
left join
(
select get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11339,11828,11340)  and d between '2020-05-16' and '2020-05-21'
and (
((cname = 'WXAPP_YCYH_PLUS' or cname = 'APP_SUV') and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
or
(cname = 'WXAPP_YCQKJ' and get_json_object(args,"$.redirect_target") in (909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888))
)
group by get_json_object(args,"$.redirect_target")
) as t2
on  t1.`商品spu`= t2.`商品spu`
left join
(
select items.item_id as `商品spu`,items.item_name as `商品名称`,count(distinct orders.uid) as `小程序商品下单人数`,sum(items.all_price) as `GMV` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'   and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-05-16' and '2020-05-21' and item_id in
		(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no
group by items.item_id,items.item_name
) as t6
on  t2.`商品spu`= t6.`商品spu`;

--有车以后+
select t1.*,t6.`小程序(有车以后+)商品下单人数`,t6.`GMV` from
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `小程序商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829,11341)  and d between '2020-05-16' and '2020-05-21'
and (
(cname = 'WXAPP_YCYH_PLUS' and get_json_object(args,"$.redirect_target") in (908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889))
)
group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
    (
select items.item_id as `商品spu`,items.item_name as `商品名`,count(distinct orders.uid) as `小程序(有车以后+)商品下单人数`,(sum(items.all_price-items.all_deduction_price))/100 as `GMV` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-05-16' and '2020-05-21'   and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-05-16' and '2020-05-21' and item_id in
		(908,1709,1760,848,964,1774,1727,1744,1660,1674,472,904,973,707,397,1758,852,889,909,1708,1761,849,965,1775,1728,1745,1659,1673,479,905,972,708,398,1759,853,888)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id,items.item_name
) as t6
on  t1.`商品spu`= t6.`商品spu`;



select count(distinct cid) as `banner曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829,11341)  and d between '2020-05-16' and '2020-05-21' and ctype in (1,2,4)
and get_json_object(args,"$.card_box_type") = "5"
and ((get_json_object(args,"$.redirect_target") = "109" and cname in ("WXAPP_YCYH_PLUS","APP_SUV") )or (get_json_object(args,"$.redirect_target") = "110" and cname = "WXAPP_YCQKJ"));
select count(distinct cid) as `APP卡片点击+小程序卡片曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829,11340)  and d between '2020-05-16' and '2020-05-21' and ctype in (1,2,4)
and get_json_object(args,"$.redirect_target") = "503383"
and get_json_object(args,"$.card_box_type") = "1"
  and ((get_json_object(args,"$.redirect_target") = "503383" and cname in ("WXAPP_YCYH_PLUS","APP_SUV")) or (get_json_object(args,"$.redirect_target") = "503382" and cname = "WXAPP_YCQKJ"))
select count(distinct open.cid) as `小程序广告图扫码人数` from (select * from iyourcar_dw.dwd_all_action_hour_log
where id in (316)  and d between '2020-05-16' and '2020-05-21' and ctype = 4
and get_json_object(args,"$.scene_id") in (1047,1048,1049)) as open
inner join
(select * from iyourcar_dw.dwd_all_action_hour_log where id = 293 and d between '2020-05-16' and '2020-05-21' and ctype = 4 ) as mall
on open.cid = mall.cid and open.session = mall.session;
select count(distinct cid) as `弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11509,11510,11511)  and d between '2020-05-16' and '2020-05-21' and ctype in (1,2,4)
and get_json_object(args,"$.gid") = "501#109";