--帖子阅读人数
select get_json_object(args,'$.gid'),count(distinct cid),count(cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-01' and '2020-08-02'
and id in(105,11875,183,99,11674)
and split(get_json_object(args,'$.gid'),'#')[1] in(5246665,5290773)
group by get_json_object(args,'$.gid');

--阅读完成数（卡片的的曝光数）
select get_json_object(args,'$.spu'),count(distinct cid),count(cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-01' and '2020-08-02'
and id in(12607,12602,12597,12592)
and get_json_object(args,'$.spu') in(541,419)
group by get_json_object(args,'$.spu');

--卡片的点击人数
select get_json_object(args,'$.spu'),count(distinct cid),count(cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-01' and '2020-08-02'
and id in(12608,12603,12598,12593)
and get_json_object(args,'$.spu') in(541,419)
group by get_json_object(args,'$.spu');

--购买对应商品的人数



--购买商品的人数
select spu,count(distinct orders.uid),count(distinct order_no)
from
(select d,ctype,cid,get_json_object(args,'$.spu') as spu,st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-01' and '2020-08-02'
and id in(12608,12603,12598,12593)
and get_json_object(args,'$.spu') in(541,419))
as log
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.cid=log.cid
join
(
    select uid,order_no,substr(ordertime,0,10) as d,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-08-01' and '2020-08-02'
    and all_price>0
    and order_status in(1,2,3)
    and biz_type in(1,3)
    and mall_type=1
    ) as orders
on orders.uid=maps.uid and log.d=orders.d
group by spu
;


--tmp
select spu,count(distinct orders.uid),count(distinct order_no)
from
(select d,ctype,cid,get_json_object(args,'$.spu') as spu,st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-07-27'
and id in(12598,12593)
and get_json_object(args,'$.spu') in(1974))
as log
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.cid=log.cid
join
(
    select uid,order_no,substr(ordertime,0,10) as d,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-27' and '2020-07-27'
    and all_price>0
    and order_status in(1,2,3)
    and biz_type in(1,3)
    and mall_type=1
    ) as orders
on orders.uid=maps.uid and log.d=orders.d
where orders.st>log.st
group by spu
;

select get_json_object(args,'$.spu'),ctype,count(distinct cid),count(cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-07-27'
and id in(12598,12593)
and get_json_object(args,'$.spu') in(1974,518)
group by get_json_object(args,'$.spu'),ctype;

select get_json_object(args,'$.spu'),ctype,count(distinct cid),count(cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-27' and '2020-07-27'
and id in(12597,12592)
and get_json_object(args,'$.spu') in(1974,518)
group by get_json_object(args,'$.spu'),ctype;