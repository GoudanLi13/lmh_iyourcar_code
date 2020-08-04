--建立推文对应商品表
drop table tmp.lmh_xcxpush_spu_list_0618;
create table tmp.lmh_xcxpush_spu_list_0803
(
    d string comment '推文日期',
    sku_id bigint comment '商品id'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into tmp.lmh_xcxpush_spu_list_0618 values
('2020-02-09',1104),
('2020-02-09',460),
('2020-02-15',1123),
('2020-02-16',1152),
('2020-02-23',866),
('2020-02-23',378),
('2020-02-23',866),
('2020-02-23',1203),
('2020-02-29',1237),
('2020-02-29',347),
('2020-02-29',486),
('2020-03-06',1262),
('2020-03-06',1210),
('2020-03-15',569),
('2020-03-15',387),
('2020-03-18',927),
('2020-03-18',1303),
('2020-03-22',1308),
('2020-03-22',1222),
('2020-03-26',1222),
('2020-03-29',1165),
('2020-03-29',1425),
('2020-03-31',401),
('2020-04-05',870),
('2020-04-05',1237),
('2020-04-05',419),
('2020-04-12',1425),
('2020-04-12',868),
('2020-04-19',651),
('2020-04-25',1222),
('2020-04-26',1647),
('2020-04-26',390),
('2020-04-26',330),
('2020-04-29',870),
('2020-05-03',1222),
('2020-05-05',1233),
('2020-05-09',405),
('2020-05-09',1754),
('2020-05-14',1285),
('2020-05-17',1765),
('2020-05-24',1692),
('2020-05-28',459),
('2020-06-06',401),
('2020-06-07',1222),
('2020-06-13',2006),
('2020-06-14',2006);

insert into tmp.lmh_xcxpush_spu_list_0803 values
('2020-07-05',1953),
('2020-07-12',2180),
('2020-07-18',1953),
('2020-07-22',2180),
('2020-07-23',1692),
('2020-07-25',405),
('2020-07-26',2323),
('2020-07-26',1403),
('2020-07-29',2323),
('2020-07-30',1222),
('2020-07-30',1692);

select * from tmp.lmh_xcxpush_spu_list_0803;

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
 on p_log.d=push.d and get_json_object(p_log.args,'$.spu')=push.sku_id
) as b
on a.d=b.d and a.cid =b.cid and a.session =b.session
join iyourcar_dw.dws_extend_day_cid_map_uid as c
on b.cid=c.cid;

select count(distinct uid) from tmp.lmh_push_visiter_0803;


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
                        and substr(ordertime,0,10) ='2020-07-12'
                        and mall_type=1
                        and order_status in(1,2,3)
                        and biz_type in(1,3)
                      ) as order_list
                          join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as order_item
                               on order_list.order_no = order_item.order_no
                    where item_id= 2180) as item
                join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
                on item.order_no=orders.order_no;

--查询当月只有推文当天有进来的人
select count(distinct alone_cid.cid)
from
(
    select cid
    from
    (select distinct cid,d
from (
      select cid,d,id from  iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-01' and '2020-07-31'
and cname='WXAPP_YCYH_PLUS') as log
join (select * from iyourcar_dw.dwd_all_action_day_event_group where event_group_id=20) as groups
on groups.event_id=log.id
) as a
    group by cid
having count(d)=1

    ) as alone_cid
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on alone_cid.cid=maps.cid
join tmp.lmh_push_visiter_0803 as push
on push.uid=maps.cid;
