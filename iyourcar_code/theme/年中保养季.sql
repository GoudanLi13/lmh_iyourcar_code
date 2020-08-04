--查询以下商品历史价格分别
select *
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item where all_price!=0 limit 10;

select item_id,price,count(items.order_no)
from
     (
        select distinct order_no
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where order_status in(1,2,3)
        and all_price>0) as orders
join
    (
        select item_id,order_no,round(item_price*item_num/100,0) as price
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
        where item_id in(987,1699,776,582,577,1949)
        ) items
on orders.order_no=items.order_no
group by item_id,price
;

-----年中保养季弹窗数据(看到弹窗的人，弹窗点击的人，当天有购物行为的人）
--各端弹窗曝光+点击人数
        select
        d,
        ctype,
        cname,
        count(distinct case when id in(11510,11509,11511,11512) then cid end) as `曝光人数`,
        count(distinct case when id in(11506,11505,11507,11508) then cid end) as `点击人数`
        from iyourcar_dw.dwd_all_action_hour_log
        where d between '2020-07-20' and '2020-07-20' and
          id in(11510,11509,11511,11512,11506,11505,11507,11508)
          and (
                (ctype in (1, 2) and cname = 'APP_SUV' and get_json_object(args, '$.gid') = '501#114')
                or
                (ctype = 4 and cname = 'WXAPP_YCYH_PLUS' and get_json_object(args, '$.gid') = '501#114')
                or
                (ctype = 4 and cname = 'WXAPP_YCQKJ' and get_json_object(args, '$.gid') = '501#115')
            )
        group by d, ctype, cname;

--各端下单人数+GMV
--app端
        select action.d,action.ctype,count(distinct action.uid ) as `下单人数`,sum(all_price/100) as `GMV` from
        (
            select distinct uid,d,ctype
            from iyourcar_dw.dwd_all_action_hour_log
            where id in (11505,11506)  and
            d between '2020-07-20' and '2020-07-20' and
            ctype in (1,2) and
            get_json_object(args,"$.gid") = '501#114'
        ) as action
        inner join
        (
            select substr(ordertime,0,10) as d,uid,ctype,all_price
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
            where substr(ordertime,0,10) between '2020-07-20' and '2020-07-20' and
                  ctype in (1,2) and
                  order_status in (1,2,3) and
                  biz_type in(1,3) and
                  all_price>0
        )as orders
        on action.uid = orders.uid and action.d = orders.d and action.ctype=orders.ctype
        group by action.d,action.ctype;

--小程序端
        select action.d,action.mall_type,count(distinct action.uid) as `下单人数`,sum(all_price/100) as `GMV` from
        (
            select distinct uid,d,case when cname='WXAPP_YCQKJ' then 2 else 1 end as mall_type
            from iyourcar_dw.dwd_all_action_hour_log
            where id in (11507,11508)  and
            d between '2020-07-20' and '2020-07-20' and
            ctype=4 and
            get_json_object(args,"$.gid") in('501#114','501#115')
        ) as action
        inner join
        (
            select substr(ordertime,0,10) as d,uid,mall_type,all_price
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
            where substr(ordertime,0,10) between '2020-07-20' and '2020-07-20' and
                  ctype=4 and
                  order_status in (1,2,3) and
                  all_price>0 and
                  biz_type in(1,3)
        )as orders
        on action.uid = orders.uid and action.d = orders.d and action.mall_type=orders.mall_type
        group by action.d,action.mall_type;



----点击push的人，买了东西的人，GMV
--点击push
select d,id,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-17' and '2020-07-17' and
id in(11762,12486) and
get_json_object(args,'$.push_id')=665027
group by d,id;

select *
from iyourcar_dw.dwd_all_action_hour_log
where d='2020-07-10'
and h=21
and id in(11762,12486);

--买东西 and GMV
select click_list.d,id,count(distinct click_list.uid),sum(order_list.price)/100 as `GMV`
       from
    (select d, id,uid
    from (select distinct d, id,cid as cid
          from iyourcar_dw.dwd_all_action_hour_log
          where d between '2020-07-17' and '2020-07-17'
            and id in(11762,12486)
            and get_json_object(args, '$.push_id') = 665027
          ) as cid_list
             join iyourcar_dw.dws_extend_day_cid_map_uid as uid_list
                  on cid_list.cid = uid_list.cid) as click_list
    join
        (
            select substr(ordertime,0,10) as d,uid,sum(all_price) as price
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
            where ctype in(1,2) and all_price>0 and order_status in(1,2,3) and biz_type in(1,3) and substr(ordertime,0,10) between '2020-07-17' and '2020-07-17'
            group by substr(ordertime,0,10),uid
            ) as order_list
    on click_list.d=order_list.d and click_list.uid=order_list.uid
    group by click_list.d,id;


-----扫了群空间二维码、下单的人、GMV,是否需要满足同一个session的条件
--('11339','11340','11828') 大型专题的页面e事件
select
        count(distinct open.cid) as `小程序广告图扫码人数`,
        count(distinct orders.uid) as `下单的人`,
        sum(orders.all_price) as `GMV`
from (select *
      from iyourcar_dw.dwd_all_action_hour_log
      where id=316
        and d='2020-07-16'
        and ctype = 4
        and get_json_object(args, "$.scene_id") in (1047, 1048, 1049)
     )
         as open
         inner join
     (select *
      from iyourcar_dw.dwd_all_action_hour_log
      where d='2020-07-16' and ctype = 4 and id in(11339,11338) and get_json_object(args,'$.redirect_target')=114) as mall
     on open.cid = mall.cid and open.session=mall.session
     left join iyourcar_dw.dws_extend_day_cid_map_uid as map_list
     on open.cid=map_list.cid
     left join
    (
        select uid,all_price
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where ctype=4 and mall_type=1 and order_status in(1,2,3) and all_price>0 and substr(ordertime,0,10)='2020-07-16'
        ) as orders
    on map_list.uid=orders.uid;

--banner的曝光点击人数、下单人数、GMV
--曝光
select d,ctype,cname,count(distinct cid) as `banner曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829,11341)  and d between '2020-07-20' and '2020-07-20' and ctype in (1,2,4)
and get_json_object(args,'$.redirect_target') in (114,115)
group by d,ctype,cname;

--点击、下单、GMV
select click_cid.d,
       click_cid.ctype,
       click_cid.cname,
       count(click_cid.cid)                                                              as `点击人数`,
       count(case when unix_timestamp(orders.ordertime) * 1000 > st then orders.uid end) as `购买人数`,
       sum(case when unix_timestamp(orders.ordertime) * 1000 > st then all_price end)    as GMV
from (select d, cid, ctype, cname, min(st) as st
      from iyourcar_dw.dwd_all_action_hour_log
      where d between '2020-07-20' and '2020-07-20'
        and id in (11340, 11339, 11828)
        and get_json_object(args, '$.redirect_target') in (114, 115)
      group by d, cid, ctype, cname) as click_cid
         left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on click_cid.cid=maps.cid
left join (
select ordertime,uid,all_price
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-07-20' and '2020-07-20'
    and all_price>0
    and order_status in(1,2,3)
    and biz_type in(1,3)
) as orders
on substr(orders.ordertime,0,10)=click_cid.d and orders.uid=maps.uid
group by click_cid.d,click_cid.ctype,click_cid.cname;

select *
from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info;


--以天为维度(活动页面访问人数，活动商品GMV，总计GMV，还缺下单人数，商品曝光人数，商品点击人数)
--活动商品GMV
select d,sum(all_price) as `GMV`,count(distinct orders.uid) as `活动商品下单人数`
from (select uid,substr(ordertime,0,10) as d,order_no
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10) between '2020-07-20' and '2020-07-20'
        and order_status in(1,2,3)
        and biz_type in(1,3)
        and all_price>0
    ) as orders
join
    (
        select uid,order_no,all_price
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
        where item_id in(2233,
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
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421,
2290,
2291
)) as item
on orders.order_no=item.order_no
group by d;

--活动期间总GMV
select substr(ordertime,0,10),sum(all_price/100)
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10) between '2020-07-20' and '2020-07-20'
        and order_status in(1,2,3)
        and biz_type in(1,3)
        and all_price>0
group by substr(ordertime,0,10);

--活动期间活动商品的详情页人数
select count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d='2020-07-09'
and id in(267,379,1024,302)
and get_json_object(args,'$.spu') in(
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
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421,2290,2291);

--活动期间活动页内商品的曝光人数
select d,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-20' and '2020-07-20'
and id in(11338,11829,11341,11828,11339,11340)
and get_json_object(args,'$.gid') in('206#665027','206#665032')
and get_json_object(args,'$.redirect_target') in(2233,
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
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421,2290,2291
    )
group by d;

--在大型专题页曝光


--商品点击人数
select d,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-20' and '2020-07-20'
and id in(11828,11339,11340)
and get_json_object(args,'$.gid') in('206#665027','206#665032')
and get_json_object(args,'$.redirect_target') in(2233,
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
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421,
2290,2291)
group by d;

select *
from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info where special_no in(416674,390556);

--活动页面访问人数
select d,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-20' and '2020-07-20'
and id in(11338,11829,11341,11828,11339,11340)
and get_json_object(args,'$.gid') in('206#665027','206#665032')
group by d;



--商品下单人数及GMV
--有车币商城
select t1.*,t2.`商品点击人数`,t6.`商品下单人数`,t6.`GMV` from
(
select
       d,
       get_json_object(args,"$.redirect_target") as `商品spu`,
       count(distinct cid) as `商品曝光人数`,
       count(distinct case when get_json_object(args,'$.gid')='206#665027' then cid end) as `活动页商品曝光人数`
from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11341)  and d between '2020-07-17' and '2020-07-19' and cname in('APP_SUV','WXAPP_YCYH_PLUS')
and  get_json_object(args,"$.redirect_target") in
 (2233,
     1420,
2234,
2236,
2238,
2241,
469,
446,
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
1623,
2127,
1953,
2180,
1765,
1237,
387,
687,2290,1989)
group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `商品点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11339,11340)  and d between '2020-07-17' and '2020-07-19' and cname in('APP_SUV','WXAPP_YCYH_PLUS')
and  get_json_object(args,"$.redirect_target") in
(
 1989,2233,
     1420,
2234,
2236,
2238,
2241,
469,
446,
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
1623,
2127,
1953,
2180,
1765,
1237,
387,
687,2290)
group by d,get_json_object(args,"$.redirect_target")
) as t2
on t1.d=t2.d and t1.`商品spu`= t2.`商品spu`
left join
    (
select orders.d as d,items.item_id as `商品spu`,count(distinct orders.uid) as `商品下单人数`,(sum(items.all_price))/100 as `GMV` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-07-17' and '2020-07-19'  and order_status  in (1,2,3) and all_price>0 and mall_type=1
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-07-17' and '2020-07-19' and item_id in
    (2233,1420,1989,
2234,
2236,
2238,
2241,
469,
446,
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
1623,
2127,
1953,
2180,
1765,
1237,
387,
687,2290)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t6
on  t2.`商品spu`= t6.`商品spu` and t6.d=t2.d;

--活动总结
--活动期间活动页内商品的曝光人数
select count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-07-20'
and id in(11338,11829,11341,11828,11339,11340)
and get_json_object(args,'$.gid') in('206#665027','206#665032')
and get_json_object(args,'$.redirect_target') in(2233,
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
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421,2290,2291
    );

--在大型专题页曝光


--商品点击人数
select count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-07-20'
and id in(11828,11339,11340)
and get_json_object(args,'$.gid') in('206#665027','206#665032')
and get_json_object(args,'$.redirect_target') in(2233,
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
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421,
2290,2291);


--活动页面访问人数
select count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-07-20'
and id in(11338,11829,11341,11828,11339,11340)
and get_json_object(args,'$.gid') in('206#665027','206#665032');


--活动商品GMV和下单人数
select sum(all_price) as `GMV`,count(distinct orders.uid) as `活动商品下单人数`
from (select uid,substr(ordertime,0,10) as d,order_no
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10) between '2020-07-09' and '2020-07-20'
        and order_status in(1,2,3)
        and biz_type in(1,3)
        and all_price>0
    ) as orders
join
    (
        select uid,order_no,all_price
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
        where item_id in(2233,
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
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421,
2290,
2291
)) as item
on orders.order_no=item.order_no;

--商品下单人数及GMV
--有车币商城
select t1.*,t2.`商品点击人数`,t6.`商品下单人数`,t6.`GMV` from
(
select
      get_json_object(args,"$.redirect_target") as `商品spu`,
       count(distinct cid) as `商品曝光人数`,
       count(distinct case when get_json_object(args,'$.gid')='206#665027' then cid end) as `活动页商品曝光人数`
from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11341)  and d between '2020-07-09' and '2020-07-20' and cname in('APP_SUV','WXAPP_YCYH_PLUS')
and  get_json_object(args,"$.redirect_target") in
 (731,
  1420,
  2233,
     1420,
2234,
2236,
2238,
2241,
469,
446,
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
1623,
2127,
1953,
2180,
1765,
1237,
387,
687,2290,1989)
group by get_json_object(args,"$.redirect_target")
) as t1
left join
(
select get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `商品点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11339,11340)  and d between '2020-07-09' and '2020-07-20' and cname in('APP_SUV','WXAPP_YCYH_PLUS')
and  get_json_object(args,"$.redirect_target") in
(731,
  1420,
 1989,2233,
     1420,
2234,
2236,
2238,
2241,
469,
446,
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
1623,
2127,
1953,
2180,
1765,
1237,
387,
687,2290)
group by get_json_object(args,"$.redirect_target")
) as t2
on t1.`商品spu`= t2.`商品spu`
left join
    (
select items.item_id as `商品spu`,count(distinct orders.uid) as `商品下单人数`,(sum(items.all_price))/100 as `GMV` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-07-09' and '2020-07-20'  and order_status  in (1,2,3) and all_price>0 and mall_type=1
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-07-09' and '2020-07-20' and item_id in
    (731,
  1420,2233,1420,1989,
2234,
2236,
2238,
2241,
469,
446,
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
1623,
2127,
1953,
2180,
1765,
1237,
387,
687,2290)
) as items
on orders.order_no = items.order_no
group by items.item_id
) as t6
on  t2.`商品spu`= t6.`商品spu`;

--访问过活动页面，看到过商品曝光的人有多少，贡献了多少GMV
select count(distinct uid),sum(all_price)
from
(select distinct orders.uid,orders.order_no,item_id,items.all_price
from iyourcar_dw.dws_extend_day_cid_map_uid as maps
join
(select cid,min(st) as st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-07-20'
and id in(11338,11829,11341,11828,11339,11340)
and get_json_object(args,'$.gid') in('206#665027','206#665032')
and get_json_object(args,'$.redirect_target') in(2233,731,732,
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
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421,2290,2291,1989,1988
    )
group by cid) as visit
on visit.cid=maps.cid
join
(
select *,unix_timestamp(ordertime)*1000 as st from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-07-09' and '2020-07-20'  and order_status  in (1,2,3) and all_price>0 and biz_type in(1,3)
) as orders
on orders.uid=maps.uid
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-07-09' and '2020-07-20' and item_id in
    (2233,731,732,
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
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421,2290,2291,1989,1988)
) as items
on orders.order_no = items.order_no
where orders.st>visit.st) as a
;

--计算访问过活动页的all_price
select count(distinct uid),sum(all_price)
from
(select distinct orders.uid,orders.order_no,all_price
from iyourcar_dw.dws_extend_day_cid_map_uid as maps
join
(select cid,min(st) as st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-07-20'
and id in(11338,11829,11341,11828,11339,11340)
and get_json_object(args,'$.gid') in('206#665027','206#665032')
and get_json_object(args,'$.redirect_target') in(2233,731,732,
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
1624,
2126,
1954,
2181,
1764,
1236,
749,
688,
1421,2290,2291,1989,1988
    )
group by cid) as visit
on visit.cid=maps.cid
join
(
select *,unix_timestamp(ordertime)*1000 as st from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-07-09' and '2020-07-20'  and order_status  in (1,2,3) and all_price>0 and biz_type in(1,3)
) as orders
on orders.uid=maps.uid
where orders.st>visit.st) as a
;

