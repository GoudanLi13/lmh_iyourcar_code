select d,ctype,c_ver,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-06-30' and '2020-07-05'
and ctype in(2,4)
and cname in('WXAPP_YCYH_PLUS','APP_SUV')
group by d,ctype,c_ver;


--查出7月3日-7月5日不同版本的有车币商城 651，1237，1974，382,1765的商品详情页访问人数、下单人数对比
select
       get_json_object(args,'$.spu') as spu,
       ctype,
       count(distinct case when c_ver>=427000 then cid end) as `有车主秀人数`,
       count(distinct case when c_ver<427000 then cid end) as `无车主秀人数`
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-06-29' and '2020-07-09'
and ctype in(2,4)
and cname in('WXAPP_YCYH_PLUS','APP_SUV')
and id in(267,302)
and get_json_object(args,'$.spu') in(651,1237,1974,382,1765)
group by get_json_object(args,'$.spu'),ctype;


select
spu,
ctype,
count(distinct case when c_ver>=427000 then order_no end) as `有车主秀人数`,
count(distinct case when c_ver<427000 then order_no end) as `无车主秀人数`
from
    (select items.item_id as spu,orders.order_no as order_no,orders.ctype as ctype,max(c_ver) as c_ver
    from
         (  select order_no,ctype,uid,unix_timestamp(ordertime)*1000 as st,substr(ordertime,0,10) as d
            from  iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
             where substr(ordertime,0,10) between '2020-06-29' and '2020-07-09'
             and order_status in(1,2,3)
             and all_price>0
             and mall_type=1
             and ctype in(2,4)

        ) as orders
    join
             (
                 select order_no,item_id
                 from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
                 where item_id in(651,1237,1974,382,1765)
                 ) as items
    on orders.order_no=items.order_no
    join
        (
            select d,ctype,uid,c_ver,st
            from iyourcar_dw.dwd_all_action_hour_log
            where d between '2020-06-29' and '2020-07-09'
            and ctype in(2,4)
            and cname in('APP_SUV','WXAPP_YCYH_PLUS')
            ) as log
    on log.uid=orders.uid and orders.ctype=log.ctype and log.d=orders.d
    where log.st<orders.st
    group by items.item_id,orders.order_no,orders.ctype) as a
group by spu,ctype;


--查询新增的车主秀关联的商品的数据
--总体数据
--详情页访问人数
select
       get_json_object(log.args,'$.spu') as spu,
       count(distinct case when c_ver<427000 or (c_ver>=427000 and st<times) or show.item_id is null then cid end) as `无车主秀人数`
from (select * from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-06-21' and '2020-07-27'
and ctype in(2,4)
and cname in('WXAPP_YCYH_PLUS','APP_SUV')
and id in(267,302,379)
and get_json_object(args,'$.spu')
    in (
        399,
518,
408,
403,
543,
540,
537,
536,
533,
532,
530,
524,
2274,
1653,
401,
2268,
409,
469,
458,
643,
406,
415,
509,
1298,
505,
501,
1956,
1900,
1899,
1897,
1890,
1882,
1881,
987,
928,
776,
453,
1522,
807,
1442,
1460,
1285,
794,
433,
382,
870,
471,
435,
475,
343,
330,
733,
726,
722,
717,
715,
704,
696,
694,
689,
687,
685,
683,
680,
667,
625,
550,
486,
390,
358,
357,
617,
391,
669,
731,
582,
577,
615,
2290,
2276,
2232,
2214,
2127,
1990,
1974,
1961,
1953,
1949,
1911,
1907,
1778,
1766,
1699,
1690,
1623,
1496,
1444,
1405,
1290,
1019,
975,
800,
791,
750,
659,
651,
1086,
635,
609,
569,
1218,
441,
378,
1208,
868,
866,
833,
326,
1226,
1225,
1222,
698,
693,
662,
605,
514,
476,
345,
700,
525,
522,
496,
527,
563,
558,
546,
541,
387,
2205,
2180,
2124,
2121,
2109,
1989,
1947,
1945,
1943,
1930,
1928,
1925,
1922,
1919,
1917,
1915,
1913,
1909,
1791,
1786,
1785,
1783,
1765,
1723,
1692,
1688,
1649,
1581,
1308,
1237,
1233,
1220,
1216,
1214,
1212,
1210,
1203,
1173,
1171,
1123,
1084,
1082,
1076,
756,
691,
665,
405,
1574,
421,
419,
377,
423,
551,
1555,
1397,
766,
762,
623,
1754 )
    ) as log
left join
 (
     select item_id,unix_timestamp(b.createtime)*1000 as times
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_car_show as a
    join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_car_show_ref_item as b
    on a.id=b.show_id
    and is_show=1
     ) as show
on get_json_object(log.args,'$.spu')=show.item_id
group by get_json_object(log.args,'$.spu');


--总下单人数
select
spu,
ctype,
count(distinct case when c_ver>=427000  then order_no end) as `有车主秀人数`,
count(distinct case when c_ver<427000  then order_no end) as `无车主秀人数`
from
    (select items.item_id as spu,orders.order_no as order_no,orders.ctype as ctype,orders.st as st,times,max(c_ver) as c_ver
    from
         (  select order_no,ctype,uid,unix_timestamp(ordertime)*1000 as st,substr(ordertime,0,10) as d
            from  iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
             where substr(ordertime,0,10) between '2020-06-21' and '2020-07-27'
             and order_status in(1,2,3)
             and ctype in(1,2)
             and all_price>0
             and mall_type=1
             and biz_type in(1,3)
        ) as orders
    join
             (
                 select order_no,item_id
                 from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
                 where item_id in(
                                  2127,
419,
1237,
405,
382,
1953,
651,
1974,
1765,
1951,
377,
401,
1917,
1233,
569,
1692,
717,
509,
1285,
1961,
390,
518,
1308,
1754,
1949)
                 ) as items
    on orders.order_no=items.order_no
    join
        (
            select d,ctype,uid,c_ver,st
            from iyourcar_dw.dwd_all_action_hour_log
            where d between '2020-06-21' and '2020-07-27'
            and ctype in(2,4)
            and cname in('APP_SUV','WXAPP_YCYH_PLUS')
            ) as log
    on log.uid=orders.uid and orders.ctype=log.ctype and log.d=orders.d
    join
        (
     select item_id,unix_timestamp(b.createtime)*1000 as times
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_car_show as a
    join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_car_show_ref_item as b
    on a.id=b.show_id
    and is_show=1
     ) as show
    on items.item_id=show.item_id
    where log.st<orders.st
    group by items.item_id,orders.order_no,orders.ctype,orders.st,times) as a
group by spu,ctype;

--总下单人数，改
select
spu,
count(distinct case when c_ver>=427000  then order_no end) as `有车主秀人数`,
count(distinct case when c_ver<427000  then order_no end) as `无车主秀人数`
from
    (select items.item_id as spu,orders.order_no as order_no,max(c_ver) as c_ver
    from
         (  select order_no,ctype,uid,unix_timestamp(ordertime)*1000 as st,substr(ordertime,0,10) as d
            from  iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
             where substr(ordertime,0,10) between '2020-06-21' and '2020-07-19'
             and order_status in(1,2,3)
             and all_price>0
             and mall_type=1
             and ctype=4
             and biz_type in(1,3)
        ) as orders
    join
             (
                 select order_no,item_id
                 from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
                 where item_id in(
                                  2127,
419,
1237,
405,
382,
1953,
651,
1974,
1765,
1951,
377,
401,
1917,
1233,
569,
1692,
717,
509,
1285,
1961,
390,
518,
1308,
1754,
1949)
                 ) as items
    on orders.order_no=items.order_no
    join
        (
            select log_a.*,uid
            from
            (select d,ctype,cid,c_ver,st,get_json_object(args,'$spu') as spu
            from iyourcar_dw.dwd_all_action_hour_log
            where d between '2020-06-21' and '2020-07-19'
            and id=302) log_a
            left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
            on log_a.cid=maps.cid
            ) as log
    on log.uid=orders.uid and orders.ctype=log.ctype and log.d=orders.d and log.spu=item_id
    where log.st<orders.st
    group by items.item_id,orders.order_no,orders.ctype,orders.st) as a
group by spu;

--小程序数据-改
select spu,
        count(distinct case when c_ver>=428000 then log.cid end) as visit,
       count(distinct case when c_ver>=428000 and order_no is not null then order_no end) as buyer
from
(select cid,st,d,get_json_object(args,'$.spu') as spu,c_ver
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-06-21' and '2020-07-19'
and id=302) as log
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on log.cid=maps.cid
left join
(
    select orders.*,item_id
    from
    (select uid,substr(ordertime,0,10) as d,order_no,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-06-21' and '2020-07-19'
        and ctype=4
        and biz_type in(1,3)
        and mall_type=1
        and order_status in(1,2,3)
        and all_price>0)
    as orders
    join
    (
    select order_no,item_id
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item) as item
    on orders.order_no=item.order_no
) as order_item
on order_item.uid=maps.uid and log.d=order_item.d and order_item.item_id=
where
(log.st<order_item.st or order_item.order_no is null)
and
spu in
(
2127,
419,
1237,
405,
382,
1953,
651,
1974,
1765,
1951,
377,
401,
1917,
1233,
569,
1692,
717,
509,
1285,
1961,
390,
518,
1308,
1754,
1949)
group by spu
;


--小程序数据（7月10日开始）
--看到有车主秀详情页且可产生埋点数据的人数，下单数

select spu,
        count(distinct case when c_ver>=428000 then log.cid end) as visit,
       count(distinct case when c_ver>=428000 and order_no is not null then order_no end) as buyer
from
(select cid,st,d,get_json_object(args,'$.spu') as spu,c_ver
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-10' and '2020-07-19'
and id=302) as log
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on log.cid=maps.cid
left join
(
    select orders.*,item_id
    from
    (select uid,substr(ordertime,0,10) as d,order_no,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-10' and '2020-07-19'
        and ctype=4
        and biz_type in(1,3)
        and mall_type=1
        and order_status in(1,2,3)
        and all_price>0)
    as orders
    join
    (
    select order_no,item_id
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item) as item
    on orders.order_no=item.order_no
) as order_item
on order_item.uid=maps.uid and log.d=order_item.d and order_item.item_id=log.spu
where
(log.st<order_item.st or order_item.order_no is null)
and
spu in
(
2127,
419,
1237,
405,
382,
1953,
651,
1974,
1765,
1951,
377,
401,
1917,
1233,
569,
1692,
717,
509,
1285,
1961,
390,
518,
1308,
1754,
1949)
group by spu
;

-- 看到车主秀的人数，下单数
select spu,
        count(distinct log.cid) as visit,
       count(distinct order_no) as buyer
from
(select cid,st,d,get_json_object(args,'$.spu') as spu
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-10' and '2020-07-19'
and id=12576) as log
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on log.cid=maps.cid
left join
(
    select orders.*,item_id
    from
    (select uid,substr(ordertime,0,10) as d,order_no,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-10' and '2020-07-19'
        and biz_type in(1,3)
        and ctype=4
        and order_status in(1,2,3)
        and all_price>0)
    as orders
    join
    (
    select order_no,item_id
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item) as item
    on orders.order_no=item.order_no
) as order_item
on order_item.uid=maps.uid and log.d=order_item.d and order_item.item_id=log.spu
where
(log.st<order_item.st or order_item.order_no is null)
and
spu in
(
2127,
419,
1237,
405,
382,
1953,
651,
1974,
1765,
1951,
377,
401,
1917,
1233,
569,
1692,
717,
509,
1285,
1961,
390,
518,
1308,
1754,
1949)
group by spu
;

-- 看到全部车主秀的人数，下单数
select spu,
        count(distinct log.cid) as visit,
       count(distinct order_no) as buyer
from
(select cid,st,d,get_json_object(args,'$.spu') as spu
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-10' and '2020-07-19'
and id=12578) as log
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on log.cid=maps.cid
left join
(
    select orders.*,item_id
    from
    (select uid,substr(ordertime,0,10) as d,order_no,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-10' and '2020-07-19'
        and biz_type in(1,3)
        and ctype=4
        and mall_type=1
        and order_status in(1,2,3)
        and all_price>0)
    as orders
    join
    (
    select order_no,item_id
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item) as item
    on orders.order_no=item.order_no
) as order_item
on order_item.uid=maps.uid and log.d=order_item.d and order_item.item_id=log.spu
where
(log.st<order_item.st or order_item.order_no is null)
and
spu in
(
2127,
419,
1237,
405,
382,
1953,
651,
1974,
1765,
1951,
377,
401,
1917,
1233,
569,
1692,
717,
509,
1285,
1961,
390,
518,
1308,
1754,
1949)
group by spu
;

--点击全部车主秀的人数，下单数
select spu,
        count(distinct log.cid) as visit,
       count(distinct order_no) as buyer
from
(select cid,st,d,get_json_object(args,'$.spu') as spu
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-10' and '2020-07-19'
and id=12577) as log
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on log.cid=maps.cid
left join
(
    select orders.*,item_id
    from
    (select uid,substr(ordertime,0,10) as d,order_no,unix_timestamp(ordertime)*1000 as st
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-10' and '2020-07-19'
        and biz_type in(1,3)
        and ctype=4
        and mall_type=1
        and order_status in(1,2,3)
        and all_price>0)
    as orders
    join
    (
    select order_no,item_id
    from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item) as item
    on orders.order_no=item.order_no
) as order_item
on order_item.uid=maps.uid and log.d=order_item.d and  order_item.item_id=log.spu
where
(log.st<order_item.st or order_item.order_no is null)
and
spu in
(
2127,
419,
1237,
405,
382,
1953,
651,
1974,
1765,
1951,
377,
401,
1917,
1233,
569,
1692,
717,
509,
1285,
1961,
390,
518,
1308,
1754,
1949)
group by spu
;



--每个spu的车主秀停留时长
select get_json_object(args,'$.spu') as spu,avg((et-st)/60000) as times
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-10' and '2020-07-19'
and id=12579
group by get_json_object(args,'$.spu');

select *
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-10' and '2020-07-19'
and id=12579;

