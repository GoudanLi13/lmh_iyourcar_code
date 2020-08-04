--不同时间内（5月、6月、7月至今）下单的订单，交易关闭率（交易关闭订单数/下单订单总数）数据对比
select mall_type,ctype,month(ordertime) as month,
count(order_no) as count_total,
count(case when order_status=5 then order_no end) as close_order
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10)>='2020-04-01'
and all_price>0
group by mall_type,ctype,month(ordertime);

select mall_type,ctype,
count(order_no) as count_total,
count(case when order_status=5 then order_no end) as close_order
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10)>='2020-03-01'
and substr(ordertime,0,10)<='2020-03-31'
and all_price>0
group by mall_type,ctype;

--2、不同时间内（5月、6月、7月至今）、不同业务类型，订单交易关闭的特征
--(1）各原因占比：超时未支付、支付后取消订单、售后退款等
--(2）用户特征占比：是否首单、会员/非会员

select
mall_type,ctype,month,is_privilege_order,is_first_order,
count(distinct close_order.order_no) as orders,
count(distinct case when paytime is null then close_order.order_no end) as nupay,
count(distinct case when refund_status in(0,1) then close_order.order_no end) as return
from
       (select order_no,mall_type,ctype,is_first_order,is_privilege_order,paytime,month(ordertime) as month
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime, 0, 10) >= '2020-05-01'
        and order_status=5
        and all_price>0) as close_order
        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
        on close_order.order_no=item.order_no
group by mall_type,ctype,month,is_privilege_order,is_first_order
;


select
min(ordertime)
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where is_first_order=1
;

--3.是否黑金卡和首单对订单关闭率的影响
select mall_type,ctype,month(ordertime),is_open_privilege_card,
count(order_no) as count_total,
count(case when order_status=5 then order_no end) as close_order
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10)>='2020-05-01'
and all_price>0
group by mall_type,ctype,month(ordertime),is_open_privilege_card;

--4.各端5，6月份黑金卡用户的关闭原因占比
select
mall_type,ctype,month,is_open_privilege_card,
count(distinct close_order.order_no) as orders,
count(distinct case when paytime is null then close_order.order_no end) as nupay,
count(distinct case when refund_status in(0,1) then close_order.order_no end) as return
from
       (select order_no,mall_type,ctype,is_first_order,is_open_privilege_card,paytime,month(ordertime) as month
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime, 0, 10) >= '2020-05-01'
        and substr(ordertime, 0, 10) <= '2020-06-30'
        and order_status=5
        and all_price>0) as close_order
        join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
        on close_order.order_no=item.order_no
group by mall_type,ctype,month,is_open_privilege_card;

-----
select id `spu`,name `商品名称`,item_stock `库存`,visitor_num `商品浏览人数`,sell_price `销售价格`,cost_price `成本价`,privilege_price `黑金价`,putawaytime `上架日期`
from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info
where substr(putawaytime,0,10)>='2020-04-09';

--开黑金卡后且订单关闭且之后再下订单的人
select
count(distinct close_order.order_no),
count(distinct orders.order_no)
from (select uid, order_no, ordertime
                  from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                  where order_status=5
                    and is_open_privilege_card=1
                    and all_price > 0) as close_order
                     left join(
                select uid, order_no, ordertime
                from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                where order_status in (1, 2, 3)
                  and all_price > 0)  as orders
                              on close_order.uid = orders.uid and substr(orders.ordertime,0,10)=substr(close_order.ordertime,0,10);

--开黑金卡后且订单关闭且之后再下订单且有相同商品的人
select
count(distinct close_order.order_no)
from (select uid, order_no, ordertime
                  from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                  where order_status=5
                    and is_open_privilege_card=1
                    and all_price > 0) as close_order
                     join(
                select uid, order_no, ordertime
                from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                where order_status in (1, 2, 3)
                  and all_price > 0)  as orders
                              on close_order.uid = orders.uid and substr(orders.ordertime,0,10)=substr(close_order.ordertime,0,10)
                    join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item_1
                    on close_order.order_no=item_1.order_no
                    join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item_2
                    on orders.order_no=item_2.order_no and item_1.item_id=item_2.item_id
                    ;

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

