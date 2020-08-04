--分类榜单整体点击率
select d,
count(distinct case when a='s' then cid end) as s_event,
count(distinct case when a='e' then cid end) as e_event
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-08-02'
and id in(11829,11828,11339,11338)
and get_json_object(args,'$.redirect_target') in('716502','716773','717887')
group by d;

--三个榜单的点击率
select d,
get_json_object(args,'$.redirect_target') as page_id,
count(distinct case when a='s' then cid end) as s_event,
count(distinct case when a='e' then cid end) as e_event
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-08-02'
and id in(11829,11828,11339,11338)
and get_json_object(args,'$.redirect_target') in('716502','716773','717887')
group by d,get_json_object(args,'$.redirect_target');

--总体的曝光下单率
select
s_log.d,
count(distinct s_log.cid),
count(distinct case when order_item.st>e_log.st then order_item.uid end)
from
( select distinct d,ctype,cid
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-08-02'
and id in(11338)
and get_json_object(args,'$.redirect_target') in('716502','716773','717887')
 ) as s_log
left join
(
    select
distinct get_json_object(args,'$.redirect_target') as spu,cid,d,ctype,st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-08-02'
and id in(11339)
and split(get_json_object(args,'$.gid'),'#')[1] in('716502','716773','717887')
    ) as e_log
on s_log.ctype=e_log.ctype and e_log.d=s_log.d and  e_log.cid=s_log.cid
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
    on maps.cid=e_log.cid
left join (
    select orders.order_no,orders.uid,item_id,d,st
    from
    (select uid,order_no,substr(ordertime,0,10) as d,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-27' and '2020-08-02'
        and order_status in(1,2,3)
        and biz_type in(1,3)
        and all_price>0) orders
    join
    iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
    on item.order_no = orders.order_no
    ) as order_item
on order_item.item_id=e_log.spu and order_item.uid=maps.uid and order_item.d=e_log.d
group by s_log.d
;

--三个榜单的曝光下单率
select
s_log.page_id,s_log.d,
count(distinct s_log.cid),
count(distinct case when order_item.st>e_log.st then order_item.uid end)
from
( select distinct d,ctype,cid,get_json_object(args,'$.redirect_target') as page_id
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-08-02'
and id in(11829,11828,11339,11338)
and get_json_object(args,'$.redirect_target') in('716502','716773','717887')
 ) as s_log
left join
(
    select
distinct get_json_object(args,'$.redirect_target') as spu,cid,d,ctype,st,split(get_json_object(args,'$.gid'),'#')[1] as page_id
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-08-02'
and id in(11341,11338)
and split(get_json_object(args,'$.gid'),'#')[1] in('716502','716773','717887')
    ) as e_log
on s_log.ctype=e_log.ctype and e_log.d=s_log.d and  e_log.cid=s_log.cid and s_log.page_id=e_log.page_id
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
    on maps.cid=e_log.cid
left join (
    select orders.order_no,orders.uid,item_id,d,st
    from
    (select uid,order_no,substr(ordertime,0,10) as d,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-27' and '2020-08-02'
        and order_status in(1,2,3)
        and biz_type in(1,3)
        and all_price>0) orders
    join
    iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
    on item.order_no = orders.order_no
    ) as order_item
on order_item.item_id=e_log.spu and order_item.uid=maps.uid and order_item.d=e_log.d
group by s_log.page_id,s_log.d
;

--三个榜单的商品的点击率，曝光下单率
select
e_log.page_id,e_log.spu,
count(distinct s_log.cid),
count(distinct e_log.cid),
count(distinct case when order_item.st>e_log.st then order_item.uid end)
from
(select
distinct get_json_object(args,'$.redirect_target') as spu,cid,d,ctype,split(get_json_object(args,'$.gid'),'#')[1] as page_id
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-08-02'
and id in(11341,11338)
and split(get_json_object(args,'$.gid'),'#')[1] in('716502','716773','717887')) as s_log
left join
(
    select
distinct get_json_object(args,'$.redirect_target') as spu,cid,d,ctype,st,split(get_json_object(args,'$.gid'),'#')[1] as page_id
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-08-02'
and id in(11340,11339)
and split(get_json_object(args,'$.gid'),'#')[1] in('716502','716773','717887')
    ) as e_log
on s_log.ctype=e_log.ctype and e_log.d=s_log.d and e_log.spu=s_log.spu and e_log.cid=s_log.cid and e_log.page_id=s_log.page_id
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
    on maps.cid=e_log.cid
left join (
    select orders.order_no,orders.uid,item_id,d,st
    from
    (select uid,order_no,substr(ordertime,0,10) as d,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-27' and '2020-08-02'
        and order_status in(1,2,3)
        and biz_type in(1,3)
        and all_price>0) orders
    join
    iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
    on item.order_no = orders.order_no
    ) as order_item
on order_item.item_id=e_log.spu and order_item.uid=maps.uid and order_item.d=e_log.d
group by e_log.page_id,e_log.spu
;

--

select get_json_object(args,'$.redirect_target'),
       count(distinct case when a='s' then cid end),
       count(distinct case when a='e' then cid end)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-08-02'
and id in(11341,11340,11339,11338)
and split(get_json_object(args,'$.gid'),'#')[1] in('716502','716773','717887')
group by get_json_object(args,'$.redirect_target')
;

--各榜单商品曝光
select
split(get_json_object(args,'$.gid'),'#')[1] as page_id,get_json_object(args,'$.redirect_target') as spu,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-08-02'
and id in(11341,11338)
and split(get_json_object(args,'$.gid'),'#')[1] in('716502','716773','717887')
group by split(get_json_object(args,'$.gid'),'#')[1],get_json_object(args,'$.redirect_target');