--查推文带来的订单
--建立推文访问记录表
drop table tmp.lmh_push_visiter_0803;

create table if not exists tmp.lmh_push_visiter_0803 as
select b.d as d,c.uid as uid,b.sku_id as spu_id,b.st as st
from
(select d,cid,session,get_json_object(args,'$.wx_page') from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-05' and '2020-07-31' and id=316 and (get_json_object(args,'$.wx_page') like '_\_%' or get_json_object(args,'$.wx_page') like '_-%')) a
join
(select p_log.d as d,cid,session,sku_id,st
 from (select * from iyourcar_dw.dwd_all_action_hour_log where d between '2020-07-05' and '2020-07-31' and id=302) as p_log
 join tmp.lmh_xcxpush_spu_list_0803 as push
 on get_json_object(p_log.args,'$.spu')=push.sku_id
where p_log.d =push.d or p_log.d=date_add(push.d,1)
) as b
on a.d=b.d and a.cid =b.cid and a.session =b.session
join iyourcar_dw.dws_extend_day_cid_map_uid as c
on b.cid=c.cid;

select count(distinct uid) from tmp.lmh_push_visiter_0803;

select * from  tmp.lmh_xcxpush_spu_list_0803 where d='2020-07-18';
--推文带来的GMV（含推文商品的订单）
select count(distinct uid),sum(all_price)
from
        (select distinct order_no,uid,ordertime,all_price
        from (
                 select order_list.order_no as order_no,order_list.uid as uid,unix_timestamp(order_list.ordertime)*1000 - st as time_diff,ordertime,order_list.all_price
                 from (select order_no,uid,all_price,ordertime
                      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                        where ctype=4
                        and mall_type=1
                        and order_status in(1,2,3)
                        and biz_type in(1,3)
                      ) as order_list
                          join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as order_item
                               on order_list.order_no = order_item.order_no
                          join tmp.lmh_push_visiter_0803 as visiter_list
                               on order_item.item_id = visiter_list.spu_id and
                                  visiter_list.d = substr(order_list.ordertime, 0, 10) and visiter_list.uid = order_list.uid
             ) as a
where time_diff between 0 and 3600000) as b
;

--推文带来的GMV
select count(distinct uid),sum(all_price)
from
        (select distinct order_no,uid,ordertime,all_price
        from (
                 select order_list.order_no as order_no,order_list.uid as uid,unix_timestamp(order_list.ordertime)*1000 - st as time_diff,ordertime,order_list.all_price
                 from (select order_no,uid,all_price,ordertime
                      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                        where ctype=4
                        and mall_type=1
                        and order_status in(1,2,3)
                        and biz_type in(1,3)
                      ) as order_list
                       join tmp.lmh_push_visiter_0803 as visiter_list
                        on visiter_list.d = substr(order_list.ordertime, 0, 10) and visiter_list.uid = order_list.uid
             ) as a
where time_diff>0) as b
;

--0712的无埋点推文
select count(distinct uid),sum(all_price)
        from (
                 select distinct order_list.order_no
                 from (select order_no,uid,all_price,ordertime
                      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                        where ctype=4
                        and substr(ordertime,0,10) between '2020-07-12' and '2020-07-13'
                        and mall_type=1
                        and order_status in(1,2,3)
                        and biz_type in(1,3)
                        and all_price>0
                      ) as order_list
                          join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as order_item
                               on order_list.order_no = order_item.order_no
                    where item_id= 2180) as item
                join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
                on item.order_no=orders.order_no;

--7月18日1953金额
select sum(all_price)
from
(select distinct orders.order_no,orders.all_price
from
(select uid,order_no,all_price
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where ctype=4
                        and substr(ordertime,0,10) = '2020-07-18'
                        and mall_type=1
                        and order_status in(1,2,3)
                        and biz_type in(1,3)
                        and all_price>0) as orders
join (select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item where item_id=1953) as items
on items.order_no=orders.order_no) as a;

select name
from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info
where id = 1953;


--查询当月只有推文当天有进来的人
select count(distinct alone_cid.cid)
from
(
    select cid,min(d) as d
from (
      select cid,d,id from  iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-01' and '2020-07-31'
and cname='WXAPP_YCYH_PLUS') as log
join (select * from iyourcar_dw.dwd_all_action_day_event_group where event_group_id=20) as groups
on groups.event_id=log.id
        group by cid
    ) as alone_cid
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on alone_cid.cid=maps.cid
join tmp.lmh_push_visiter_0803 as push
on push.uid=maps.uid and alone_cid.d=push.d;

--当月仅有1单，且这1单是推文带来的人
select count(push_user.uid)
from
(select distinct uid
from
        (select distinct order_no,uid,ordertime,all_price
        from (
                 select order_list.order_no as order_no,order_list.uid as uid,unix_timestamp(order_list.ordertime)*1000 - st as time_diff,ordertime,order_list.all_price
                 from (select order_no,uid,all_price,ordertime
                      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                        where ctype=4
                        and mall_type=1
                        and order_status in(1,2,3)
                        and biz_type in(1,3)
                      ) as order_list
                       join tmp.lmh_push_visiter_0803 as visiter_list
                        on visiter_list.d = substr(order_list.ordertime, 0, 10) and visiter_list.uid = order_list.uid
             ) as a
where time_diff>0) as b)
as push_user
join
(
select uid
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-07-01' and '2020-07-31'
and all_price>0
and order_status in(1,2,3)
and biz_type in(1,3)
group by uid
having count(order_no)=1) as one_buyer
on one_buyer.uid=push_user.uid;

--推文带来的gmv（按日算)
select d,sum(all_price)
from
        (select distinct order_no,uid,d,all_price
        from (
                 select order_list.order_no as order_no,order_list.uid as uid,unix_timestamp(order_list.ordertime)*1000 - st as time_diff,substr(ordertime,0,10) as d,order_list.all_price
                 from (select order_no,uid,all_price,ordertime
                      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                        where ctype=4
                        and mall_type=1
                        and order_status in(1,2,3)
                        and biz_type in(1,3)
                      ) as order_list
                       join tmp.lmh_push_visiter_0803 as visiter_list
                        on visiter_list.d = substr(order_list.ordertime, 0, 10) and visiter_list.uid = order_list.uid
             ) as a
where time_diff>0) as b
group by d
;

select  substr(ordertime,0,10),sum(all_price)
                      from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                        where
                        substr(ordertime,0,10) between '2020-07-01' and '2020-07-31'
                        and order_status in(1,2,3)
                        and biz_type in(1,3)
                        and all_price>0
group by  substr(ordertime,0,10);

--活动期间访问过活动商品详情页的人
select d,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-07-20'
and ctype=2
and id in(302,1024,379,267)
and get_json_object(args,'$.spu')
in(
    401,
446,
409,
469,
458,
643,
406,
415,
509,
1900,
1899,
1897,
1890,
1882,
1881,
1298,
505,
501,
2234,
2236,
2238,
2241,
776,
582,
577,
1949,
928,
1218,
2274,
1653,
987,
1699,
731,
1623,
2127,
1953,
2180,
1765,
1237,
387,
687,
1420,
402,
449,
411,
474,
462,
645,
407,
418,
510,
1901,
1898,
1896,
1891,
1883,
1880,
1299,
506,
511,
2235,
2237,
2239,
2240,
777,
580,
579,
929,
1219,
2275,
1654,
986,
1698,
732,
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421 )
group by d;

--商城dau
select d,count(distinct cid)
from
(
      select cid,d,id from  iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-07-20' and ctype=2 and cname='APP_SUV') as log
join (select * from iyourcar_dw.dwd_all_action_day_event_group where event_group_id=20) as groups
on groups.event_id=log.id
group by d;