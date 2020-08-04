--1.订单关闭召回
--还需要分价格区间
--1.1总体，不管订单关闭后有没有买相同的商品，只管有没有再次箱单
--1.1.1因超时未支付而关闭的订单
select count(order_no)/count(distinct substr(ordertime,0,10))
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where order_status in (4, 5)
  and all_price > 0
  and paytime is null
;


--1.1.2订单关闭且关闭后有订单的人and再次购买的间隔
select `时间差`, count(`订单关闭的订单号`)
from (select `订单关闭的用户`, `订单关闭的订单号`, min(diff) as `时间差`
      from (select close_order.uid                                                                 as `订单关闭的用户`,
                   close_order.order_no                                                            as `订单关闭的订单号`,
                   datediff(substr(orders.ordertime, 0, 10), substr(close_order.ordertime, 0, 10)) as diff
            from (select uid, order_no, ordertime
                  from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                  where order_status in (4, 5)
                    and paytime is null
                    and all_price > 0) as close_order
                     join(
                select uid, order_no, ordertime
                from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                where order_status in (1, 2, 3)
                  and all_price > 0)  as orders
                              on close_order.uid = orders.uid
            where orders.ordertime>close_order.ordertime
           ) as a
     where diff<8
      group by `订单关闭的用户`, `订单关闭的订单号`
     ) as b
group by `时间差`;

--1.2上一个订单的商品
--1.2.1交易关闭的订单中下一单包含上一单的商品的时间间隔
--数据太大，将关闭的订单和新订单提取出来（第1部分）
create table tmp.lmh_close_orders_a as
    select c_order,new_order,d_time
    from (select c_order,new_order,d_time,row_number() over (partition by c_order order by d_time) as rank
          from (select close_order.order_no  as c_order,
                       orders.order_no as new_order,
                       datediff(substr(orders.ordertime, 0, 10), substr(close_order.ordertime, 0, 10)) as d_time
                from (select uid, order_no, ordertime
                      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                      where order_status in (4, 5)
                        and paytime is null
                        and all_price > 0) as close_order
                         join(
                    select uid, order_no, ordertime
                    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                    where order_status in (1, 2, 3)
                      and all_price > 0)  as orders
                                  on close_order.uid = orders.uid
                where orders.ordertime>close_order.ordertime
               ) as a
         where d_time<8
         ) as b
    where rank=1;

drop table tmp.lmh_close_orders_a;

--第二部分
select c_order, new_order, d_time
from tmp.lmh_close_orders_a as c
         join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item_1
              on c.c_order = item_1.order_no
         join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item_2
              on c.new_order = item_2.order_no and item_1.item_id = item_2.item_id;


--1.3 订单关闭当天再次购买的时间间隔
select diff,count(order_no)
    from (select order_no,min(diff) as diff
          from (select close_order.order_no,(orders.st-close_order.st)/60 as diff
                from
                ( select b.* from
                    (select uid, order_no, ordertime,substr(ordertime,0,10) as d,unix_timestamp(ordertime) as st,row_number() over (partition by uid,substr(ordertime,0,10) order by ordertime) as rank
                      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                      where order_status in (4, 5)
                        and substr(ordertime,0,10) between '2020-06-21' and '2020-07-26'
                        and biz_type in(1,3)
                        and paytime is null
                        and all_price > 0) as b where rank=1) as close_order
                         join
                         ( select uid, order_no, ordertime,substr(ordertime,0,10) as d,unix_timestamp(ordertime) as st
                    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                    where order_status in (1, 2, 3)
                      and substr(ordertime,0,10) between '2020-06-21' and '2020-07-26'
                      and biz_type in(1,3)
                      and all_price > 0) as orders
                                  on close_order.uid = orders.uid and close_order.d=orders.d
                where orders.st>close_order.st
               ) as a
group by order_no) as d
group by diff;

--1.4.关闭订单后第二天下单是星期几
select weekday,count(order_no)
          from (select close_order.order_no,dayofweek(orders.ordertime) as weekday
                from
                ( select b.* from
                    (select uid, order_no, ordertime,substr(ordertime,0,10) as d,unix_timestamp(ordertime) as st,row_number() over (partition by uid,substr(ordertime,0,10) order by ordertime) as rank
                      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                      where order_status in (4, 5)
                        and substr(ordertime,0,10) between '2020-06-21' and '2020-07-26'
                        and biz_type in(1,3)
                        and paytime is null
                        and all_price > 0) as b where rank=1) as close_order
                         join
                         ( select uid, order_no, ordertime,substr(ordertime,0,10) as d,unix_timestamp(ordertime) as st
                    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                    where order_status in (1, 2, 3)
                      and substr(ordertime,0,10) between '2020-06-21' and '2020-07-26'
                      and biz_type in(1,3)
                      and all_price > 0) as orders
                    on close_order.uid = orders.uid and date_add(close_order.d,1)=orders.d
               ) as a
group by weekday;


select hour(ordertime),count(order_no)
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-21' and '2020-07-26'
and all_price>0
and biz_type in(1,3)
and order_status in(1,2,3)
and ctype in(1,2)
group by hour(ordertime);

--1.5 关闭订单中第2单买的商品的价格在上一单中的价格偏高还是低
select close_no,item_c.item_name,item_c.item_price,normal_no,item_d.item_name,item_d.item_price
          from (select distinct close_order.order_no as close_no,orders.order_no as normal_no
                from
                ( select b.* from
                    (select uid, order_no, ordertime,substr(ordertime,0,10) as d,unix_timestamp(ordertime) as st,row_number() over (partition by uid,substr(ordertime,0,10) order by ordertime) as rank
                      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                      where order_status in (4, 5)
                        and substr(ordertime,0,10) between '2020-06-21' and '2020-07-29'
                        and biz_type in(1,3)
                        and paytime is null
                        and all_price > 0) as b where rank=1) as close_order
                         join
                         ( select uid, order_no, ordertime,substr(ordertime,0,10) as d,unix_timestamp(ordertime) as st
                    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                    where order_status in (1, 2, 3)
                      and substr(ordertime,0,10) between '2020-06-21' and '2020-07-29'
                      and biz_type in(1,3)
                      and all_price > 0) as orders
                                  on close_order.uid = orders.uid and close_order.d=orders.d
                        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item_a
                        on close_order.order_no=item_a.order_no
                        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item_b
                        on item_b.order_no=orders.order_no and item_b.item_id=item_a.item_id
                where orders.st>close_order.st
               ) as a
        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item_c
        on item_c.order_no=close_no
        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item_d
        on item_d.order_no=normal_no;


--2.购物车
select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart where substr(createtime,0,10)='2020-01-01';
desc iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart;

--2.1购买商品的用户当天加购的概率
select orders.d,count(orders.uid),count(cart.d)
from (select distinct uid,substr(ordertime,0,10) as d
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where (substr(ordertime,0,10) between '2020-04-01' and '2020-05-31' or substr(ordertime,0,10) between '2020-06-21' and '2020-07-12')
            and all_price>0
            and mall_type=1
            and order_status in(1,2,3)
         ) as orders
    left join
        (
            select distinct uid,substr(createtime,0,10) as d
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart
            where mall_type=1
            and (substr(createtime,0,10) between '2020-04-01' and '2020-05-31' or substr(createtime,0,10) between '2020-06-21' and '2020-07-12')
            ) as cart
    on orders.uid=cart.uid and orders.d=cart.d
    group by orders.d;

--2.2购买商品的用户当天加购相同商品的概率
select orders.d,count(distinct orders.order_no)
from (select distinct uid,substr(ordertime,0,10) as d,order_no
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10) between '2020-05-01' and '2020-05-31'
            and all_price>0
            and mall_type=1
            and order_status in(1,2,3)
         ) as orders
    join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
    on orders.order_no=item.order_no
    join
        (
            select distinct uid,substr(createtime,0,10) as d,item_id
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart
            where mall_type=1
            and substr(createtime,0,10) between '2020-05-01' and '2020-05-31'
            ) as cart
    on orders.uid=cart.uid and orders.d=cart.d and cart.item_id=item.item_id
    group by orders.d;

--2.3加购当天购买商品的概率
select orders.d,count(orders.uid),count(cart.d)
from (select distinct uid,substr(ordertime,0,10) as d
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where (substr(ordertime,0,10) between '2020-04-01' and '2020-05-31' or substr(ordertime,0,10) between '2020-06-21' and '2020-07-12')
            and all_price>0
            and mall_type=1
            and order_status in(1,2,3)
         ) as orders
    right join
        (
            select distinct uid,substr(createtime,0,10) as d
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart
            where mall_type=1
            and (substr(createtime,0,10) between '2020-04-01' and '2020-05-31' or substr(createtime,0,10) between '2020-06-21' and '2020-07-12')
            ) as cart
    on orders.uid=cart.uid and orders.d=cart.d
    group by orders.d;

--2.4商品加入购物车后14天内购买相同商品的概率(只有当天有可能买)
select
date_range,
count(uid)
from
(select
cart.*,
case when order_item.order_no is null then -1
     else datediff(order_item.d,cart.d) end as date_range
from (
         select uid, item_id,substr(createtime, 0, 10) as d
         from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart
         where mall_type = 1
           and (substr(createtime,0,10) between '2020-04-01' and '2020-05-31' or substr(createtime,0,10) between '2020-06-21' and '2020-07-12')
     ) as cart
left join
    (   select orders.*,item_id
        from
        (select uid, substr(ordertime,0,10) as d,order_no
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where (substr(ordertime,0,10) between '2020-04-01' and '2020-05-31' or substr(ordertime,0,10) between '2020-06-21' and '2020-07-12')
        and mall_type=1
        and order_status in(1,2,3)
        and all_price>0) as orders
        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
        on item.order_no=orders.order_no
        ) as order_item
on  cart.uid=order_item.uid and order_item.item_id=cart.item_id
where order_item.d between cart.d and date_add(cart.d,14) or order_item.order_no is null) as a
group by date_range;

--2.5商品加入购物车后离购买的时长，精确到分钟（20分钟内购买的80%，超过20分钟基本上就不会买购买
select
date_range,
count(uid)
from
(select
cart.*,
case when order_item.order_no is null then -1
     else (order_item.st-cart.st)/60 end as date_range
from (
         select uid, item_id,substr(createtime, 0, 10) as d,unix_timestamp(createtime) as st
         from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart
         where mall_type = 1
           and (substr(createtime,0,10) between '2020-04-01' and '2020-05-31' or substr(createtime,0,10) between '2020-06-21' and '2020-07-12')
     ) as cart
left join
    (   select orders.*,item_id
        from
        (select uid, substr(ordertime,0,10) as d,order_no,unix_timestamp(ordertime) as st
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where (substr(ordertime,0,10) between '2020-04-01' and '2020-06-14' or substr(ordertime,0,10) between '2020-06-21' and '2020-07-12')
        and mall_type=1
        and order_status in(1,2,3)
        and all_price>0) as orders
        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
        on item.order_no=orders.order_no
        ) as order_item
on  cart.uid=order_item.uid and order_item.item_id=cart.item_id
where order_item.d=cart.d or order_item.order_no is null) as a
group by date_range;

--2.6每日会有多少加入购物车
select count(cart_no)/count(distinct substr(createtime,0,10))
from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart
where (substr(createtime,0,10) between '2020-04-01' and '2020-05-31' or substr(createtime,0,10) between '2020-06-21' and '2020-07-12');

--2.7.1加入购物车后当天购买该商品的件单价分布
select
ori_sale_price,count(order_no)
from
(select
        ori_sale_price,order_no
from (
         select uid,item_id,substr(createtime, 0, 10) as d,unix_timestamp(createtime) as st
         from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart
         where mall_type = 1
           and substr(createtime,0,10) between '2020-06-20' and '2020-07-27'
     ) as cart
left join
    (   select orders.*,item_id,ori_sale_price
        from
        (select uid, substr(ordertime,0,10) as d,order_no,unix_timestamp(ordertime) as st
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10) between '2020-06-20' and '2020-07-27'
        and mall_type=1
        and order_status in(1,2,3)
        and biz_type in(1,3)
        and all_price>0) as orders
        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
        on item.order_no=orders.order_no
        ) as order_item
on  cart.uid=order_item.uid and order_item.item_id=cart.item_id
where order_item.d=cart.d and order_item.st>cart.st ) as a
group by ori_sale_price
;

--2.7.2 加入购物车但没有购买的客单价分布
select
sale_price,
count(uid)
from
(select
        case when order_item.d=cart.d and order_item.st>cart.st then 0 else sku.sale_price end as sale_price,
        case when order_item.d=cart.d and order_item.st>cart.st then null else cart.uid end as uid
from (
         select uid, item_sku_id,item_id,substr(createtime, 0, 10) as d,unix_timestamp(createtime) as st
         from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart
         where mall_type = 1
           and substr(createtime,0,10) between '2020-06-20' and '2020-07-27'
     ) as cart
join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku as sku
    on cart.item_sku_id=sku.id
left join
    (   select orders.*,item_id,item_sku_id
        from
        (select uid, substr(ordertime,0,10) as d,order_no,unix_timestamp(ordertime) as st
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10) between '2020-06-20' and '2020-07-27'
        and mall_type=1
        and order_status in(1,2,3)
        and biz_type in(1,3)
        and all_price>0) as orders
        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
        on item.order_no=orders.order_no
        ) as order_item
on  cart.uid=order_item.uid and order_item.item_id=cart.item_id
 ) as a
group by sale_price
;

--2.8 购物车购买的时间点
select
h,count(order_no)
from
(select
        hour(ordertime) as h,order_no
from (
         select uid,item_id,substr(createtime, 0, 10) as d,unix_timestamp(createtime) as st
         from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart
         where mall_type = 1
           and substr(createtime,0,10) between '2020-04-26' and '2020-07-27'
     ) as cart
left join
    (   select orders.*,item_id,ori_sale_price
        from
        (select uid, substr(ordertime,0,10) as d,order_no,unix_timestamp(ordertime) as st,ordertime
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10) between '2020-04-26' and '2020-07-27'
        and mall_type=1
        and order_status in(1,2,3)
        and biz_type in(1,3)
        and ctype in(1,2)
        and all_price>0) as orders
        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
        on item.order_no=orders.order_no
        ) as order_item
on  cart.uid=order_item.uid and order_item.item_id=cart.item_id
where date_add(order_item.d,1)=cart.d or order_item.d=cart.d ) as a
group by h
;

--3.搜索

--3.1有搜索行为的用户当日的下单率（总体）
select
d,
count(cid),
count(case when is_order>0 then cid end)
from
(select
     action.d as d,action.cid as cid,count(orders.order_no) as is_order
from (select * from iyourcar_dw.dwd_all_action_hour_log
    where (d between '2020-04-01' and '2020-05-31' or d between '2020-06-21' and '2020-07-12')
    and ctype in(1,2,4)
    and cname in('APP_SUV','WXAPP_YCYH_PLUS')
    ) as action
    inner join
     (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id = 28 ) as evt
on action.id = evt.event_id
    left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
    on maps.cid=action.cid
    left join
    iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
    on orders.uid=maps.uid and substr(orders.ordertime,0,10)=action.d
    where length(get_json_object(args,"$.search_key"))!= 0
group by action.d,action.cid) as a
group by d;

--3.2用户下单前7天有搜索行为的单数/用户下单的单数(5365    629)
select count(*),sum(case when s_users>0 then 1 else 0 end)
from
(select orders.uid,orders.d,count(search.uid) as s_users
from (select distinct uid, substr(ordertime, 0, 10) as d
      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
      where substr(ordertime, 0, 10) between '2020-06-17' and '2020-07-07'
        and order_status in (1, 2, 3)
        and mall_type = 1
        and all_price > 0) as orders
         left join
     (
         select distinct maps.uid as uid,
                         action.d as d
         from (select *
               from iyourcar_dw.dwd_all_action_hour_log
               where d between '2020-06-10' and '2020-07-07'
                 and ctype in (1, 2, 4)
                 and cname in ('APP_SUV', 'WXAPP_YCYH_PLUS')
              ) as action
                  inner join
              (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id = 28) as evt
              on action.id = evt.event_id
                  join iyourcar_dw.dws_extend_day_cid_map_uid as maps
                       on action.cid = maps.cid
         where length(get_json_object(action.args, "$.search_key")) != 0
     ) as search
     on orders.uid = search.uid
where search.uid is not null
  and orders.d >= search.d
  and datediff(orders.d, search.d) < 8
group by orders.uid,orders.d) as a;

--3.3用户下单前7天，每一天搜索的概率
select orders.*, datediff(orders.d, search.d)
from (select distinct uid, substr(ordertime, 0, 10) as d
      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
      where substr(ordertime, 0, 10) between '2020-06-17' and '2020-07-07'
        and order_status in (1, 2, 3)
        and mall_type = 1
        and all_price > 0) as orders
         left join
     (
         select distinct maps.uid as uid,
                         action.d as d
         from (select *
               from iyourcar_dw.dwd_all_action_hour_log
               where d between '2020-06-10' and '2020-07-07'
                 and ctype in (1, 2, 4)
                 and cname in ('APP_SUV', 'WXAPP_YCYH_PLUS')
              ) as action
                  inner join
              (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id = 28) as evt
              on action.id = evt.event_id
                  join iyourcar_dw.dws_extend_day_cid_map_uid as maps
                       on action.cid = maps.cid
         where length(get_json_object(action.args, "$.search_key")) != 0
     ) as search
     on orders.uid = search.uid
where search.uid is not null
  and orders.d >= search.d
  and datediff(orders.d, search.d) < 8;

--3.4在0706-0712中有搜索的且下单有下到含有该关键词的商品的人(1412,181)
select
count(distinct log.cid,log.d,key),
count(case when log.st<order_spu.st and datediff(order_spu.d,log.d)<8 and item_name like concat('%',key,'%') then order_no end)
from
(select cid,uid,get_json_object(args,'$.search_key') as key,st,d
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-06' and '2020-07-12'
and cname in('APP_SUV','WXAPP_YCYH_PLUS')
and ctype in(1,2,4)
and id in(1204,1376,1444,1452)
and length(get_json_object(args,'$.search_key'))!=0) as log
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on log.cid=maps.cid
left join
(
    select orders.*,item.item_name
    from
    (
        select uid,order_no,unix_timestamp(ordertime)*1000 as st,substr(ordertime,0,10) as d
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10) between '2020-07-06' and '2020-07-16'
        and all_price>0
        and order_status in(1,2,3)
        and mall_type=1) as orders
    join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
    on orders.order_no=item.order_no
    ) as order_spu
on maps.uid=order_spu.uid
;



--4.购买前商品详情页访问次数，总体的访问次数（总数239个，196个高于平均值，当日访问详情页次数达到2.6倍）
select avg_data.spu, avg_click, order_click,buy_user
from (select spu, avg(click) as avg_click
      from (select get_json_object(args, '$.spu') as spu, d, count(cid) / count(distinct cid) as click
            from iyourcar_dw.dwd_all_action_hour_log
            where (d between '2020-04-01' and '2020-05-31' or d between '2020-06-21' and '2020-07-12')
              and ctype in (1, 2, 4)
              and cname in ('APP_SUV', 'WXAPP_YCYH_PULS')
              and id in (267, 379, 302)
            group by get_json_object(args, '$.spu'), d) as a
      group by spu) as avg_data
         left join
     (select spu, avg(click) as order_click,sum(buy) as buy_user
      from (
               select item.item_id as spu, orders.d as d, count(orders.uid) / count(distinct orders.uid) as click,count(distinct orders.uid) as buy
               from (select uid, substr(ordertime, 0, 10) as d, unix_timestamp(ordertime) * 1000 as st, order_no
                     from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                     where (substr(ordertime, 0, 10) between '2020-04-01' and '2020-05-31' or substr(ordertime, 0, 10) between '2020-06-21' and '2020-07-12')
                       and order_status in (1, 2, 3)
                       and all_price > 0
                       and mall_type = 1) as orders
                        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
                             on orders.order_no = item.order_no
                        join
                    (
                        select log.*, uid
                        from iyourcar_dw.dws_extend_day_cid_map_uid as maps
                                 join
                             (
                                 select get_json_object(args, '$.spu') as spu, d, cid, et
                                 from iyourcar_dw.dwd_all_action_hour_log
                                 where (d between '2020-04-01' and '2020-05-31' or d between '2020-06-21' and '2020-07-12')
                                   and ctype in (1, 2, 4)
                                   and cname in ('APP_SUV', 'WXAPP_YCYH_PULS')
                                   and id in (267, 379, 302)
                             ) as log
                             on log.cid = maps.cid
                    ) as action
                    on item.item_id = action.spu and orders.d = action.d and orders.uid = action.uid
               where action.et < orders.st
               group by item.item_id, orders.d
           ) as b
      group by spu
     ) as order_data
     on order_data.spu = avg_data.spu;

--6月17日-7月16日每日关闭订单人数
select count(order_no)/30
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-17' and '2020-07-16'
and order_status in(4,5)
and all_price>0;

--6月17日-7月16日每日有加购行为的人数
select  sum(user)/30
from
(select substr(createtime,0,10),count(distinct uid) as user
from iyourcar_dw.stage_all_service_day_iyourcar_mall_cart_mall_cart
where substr(createtime,0,10) between '2020-06-17' and '2020-07-16'
group by substr(createtime,0,10)) as a;

--6月17日-7月16日每日有搜索行为的人数
select sum(user)/30
from
(select d,count(distinct cid) as user
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-06-17' and '2020-07-16'
and id in(1204,1376,1444,1452)
and length(get_json_object(args,'$.search_key'))!=0
group by d) as a;

--6月17日-7月16日每日关闭订单数占比
select count(case when order_status in(4,5) then order_no end)/count(order_no),count(order_no)
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-17' and '2020-07-16'
and all_price>0;