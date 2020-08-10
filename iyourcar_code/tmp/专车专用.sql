--以下商品的每日曝光人数（搜索，列表曝光），点击人数，下单人数

select visit.spu,visit.d,
       count(distinct case when visit.a='s' then visit.cid end),
       count(distinct case when visit.a='e' then visit.cid end),
       count(distinct orders.uid)
from
(
select distinct cid,a,d,get_json_object(args,'$.spu') as spu
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-20' and '2020-08-09'
and id in(1378,1377,1447,1448,1453,1454,544,543,364,365,319,317)
and get_json_object(args,'$.spu')
in (
577,
582,
776,
928,
987,
1019,
1555,
1623,
1699,
1726,
1949,
1956,
2127,
2276,
2290,
2308,
2310,
2311,
2323,
2500,
2504 )) as visit
left join
iyourcar_dw.dws_extend_day_cid_map_uid as maps
on visit.cid=maps.cid
left join
(
    select c.uid,substr(ordertime,0,10) as d,item_id,'e' as a
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as c
    join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as b
    on c.order_no=b.order_no
    where substr(ordertime,0,10) between '2020-07-20' and '2020-08-09'
    and order_status in(1,2,3)
    and c.biz_type in(1,3)
    and c.all_price>0
    ) as orders
on
orders.uid=maps.uid and orders.d=visit.d and orders.a=visit.a and orders.item_id=visit.spu
group by visit.spu,visit.d;