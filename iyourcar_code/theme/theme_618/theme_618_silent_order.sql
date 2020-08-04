--点击客服按钮埋点组
with all_ask as
         (select distinct uid, d
          from iyourcar_dw.dwd_all_action_hour_log
          where d >= '2020-07-01'
            and d <= '2020-07-20'
            and id in (1208, 1222, 1223, 1383, 1395, 1419, 1439, 1458, 1474, 11251, 11267)
            and uid != 'visitor'
         )

--看半年来是否静默下单的人的失败和成功的人数（把推文影响去掉）
select count(distinct
             case when all_ask.uid is null and order_status in (1, 2, 3) then all_consumers.uid end)  as `静默下单成功人数`,
       count(distinct case
                          when all_ask.uid is null and order_status in (4, 5)
                              then all_consumers.uid end)         as `静默下单失败人数`,
       count(distinct case
                          when all_ask.uid is not null and order_status in (1, 2, 3)
                              then all_consumers.uid end)         as `非静默下单成功人数`,
       count(distinct case
                          when all_ask.uid is not null and order_status in (4, 5)
                              then all_consumers.uid end)         as `非静默下单失败人数`,
       count(case
                 when all_ask.uid is null and order_status in (1, 2, 3)
                     then all_consumers.order_no end)              as `静默下单成功单数`,
       count(case
                 when all_ask.uid is null and order_status in (4, 5)
                     then all_consumers.order_no end)               as `静默下单失败单数`,
       count(case
                 when all_ask.uid is not null and order_status in (1, 2, 3)
                     then all_consumers.order_no end)               as `非静默下单成功单数`,
       count(case
                 when all_ask.uid is not null and order_status in (4, 5)
                     then all_consumers.order_no end)              as `非静默下单失败单数`
from (
         select uid, order_status, substr(ordertime, 0, 10) as d, order_no
         from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
         where substr(ordertime, 0, 10) between '2020-07-01' and '2020-07-20'
           and biz_type in (1, 3)
     ) as all_consumers
         inner join
     (select order_no, item_name
      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
      where substr(createtime, 0, 10) between '2020-07-01' and '2020-07-20'
        and item_id not in
            (1953,2180)
     ) as items
     on all_consumers.order_no = items.order_no
         left join
     all_ask
     on all_consumers.uid = all_ask.uid and all_consumers.d = all_ask.d;



-- select min(d) from iyourcar_dw.dwd_all_action_hour_log
-- where d >= '2020-01-15' and d <= '2020-06-01' and id in (1208,1222,1223,1383,1395,1419,1439,1458,1474,11251,11267)
--     and uid != 'visitor'


with all_ask as
    (select distinct uid,d from iyourcar_dw.dwd_all_action_hour_log
where d >= '2020-01-15' and d <= '2020-06-10' and id in (1208,1222,1223,1383,1395,1419,1439,1458,1474,11251,11267)
    and uid != 'visitor'
    )


select
       items.item_name as `商品名`,
    count( case when all_ask.uid is  null and order_status in (1,2,3) then all_consumers.order_no end) as `静默下单成功单数`,
    count( case when all_ask.uid is  null and order_status in (4,5) then all_consumers.order_no end) as `静默下单失败单数`,
    count( case when all_ask.uid is not null and order_status in (1,2,3) then all_consumers.order_no end) as `非静默下单成功单数`,
    count( case when all_ask.uid is not null and order_status in (4,5)then all_consumers.order_no end) as `非静默下单失败单数`
from
(
    select  uid,order_status,substr(ordertime,0,10) as d,order_no from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between  '2020-01-15' and '2020-06-10' and biz_type in (1,3)
    ) as all_consumers
    inner join
    (select order_no,item_name from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
        where substr(createtime,0,10) between  '2020-01-15' and '2020-06-10' ) as items
    on all_consumers.order_no = items.order_no
left join
all_ask
on all_consumers.uid = all_ask.uid and all_consumers.d = all_ask.d
group by  items.item_name
order by `静默下单成功单数` desc
;




with all_ask as
    (select distinct uid,d from iyourcar_dw.dwd_all_action_hour_log
where d >= '2020-01-15' and d <= '2020-06-10' and id in (1208,1222,1223,1383,1395,1419,1439,1458,1474,11251,11267)
    and uid != 'visitor'
    )

select
       items.item_name as `商品名`,
    count( case when all_ask.uid is  null and order_status in (1,2,3) then all_consumers.order_no end) as `静默下单成功单数`,
    count( case when all_ask.uid is  null and order_status in (4,5) then all_consumers.order_no end) as `静默下单失败单数`,
    count( case when all_ask.uid is not null and order_status in (1,2,3) then all_consumers.order_no end) as `非静默下单成功单数`,
    count( case when all_ask.uid is not null and order_status in (4,5)then all_consumers.order_no end) as `非静默下单失败单数`
from
(
    select  uid,order_status,substr(ordertime,0,10) as d,order_no from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between  '2020-01-15' and '2020-06-10' and biz_type in (1,3)
    ) as all_consumers
    inner join
    (select order_no,item_name from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
        where substr(createtime,0,10) between  '2020-01-15' and '2020-06-10' and item_id not in
        (409,
1298,
469,
1881,
1882,
1890,
643,
406,
1899,
401,
446,
509,
1897,
458,
1900,
505,
415,
1956,
501,
411,
1299,
474,
1880,
1883,
1891,
645,
407,
1898,
402,
449,
510,
1896,
462,
1901,
506,
418,
1955,
511)
        ) as items
    on all_consumers.order_no = items.order_no
left join
all_ask
on all_consumers.uid = all_ask.uid and all_consumers.d = all_ask.d
group by items.item_name
order by `静默下单成功单数` desc
;