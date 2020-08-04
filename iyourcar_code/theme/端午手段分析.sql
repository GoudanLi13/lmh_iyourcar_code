-----端午弹窗数据(看到弹窗的人，弹窗点击的人，当天有购物行为的人）

select d,count(distinct cid) as `APP弹窗曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11509,11510)  and d between '2020-06-25' and '2020-06-27'
and get_json_object(args,"$.gid") = "501#96"
group by d;

select count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d='2020-06-25' and h between 16 and 23 and ctype=4 and cname='WXAPP_YCYH_PLUS' and page_evt_id=62;

--各端弹窗曝光+点击人数
        select
        d,
        ctype,
        cname,
        count(distinct case when id in(11510,11509,11511,11512) then cid end) as `曝光人数`,
        count(distinct case when id in(11506,11505,11507,11508) then cid end) as `点击人数`
        from iyourcar_dw.dwd_all_action_hour_log
        where d between '2020-06-25' and '2020-06-27' and
          id in(11510,11509,11511,11512,11506,11505,11507,11508)
          and (
                (ctype in (1, 2) and cname = 'APP_SUV' and get_json_object(args, '$.gid') = '501#69')
                or
                (ctype = 4 and cname = 'WXAPP_YCYH_PLUS' and get_json_object(args, '$.gid') = '501#69')
                or
                (ctype = 4 and cname = 'WXAPP_YCQKJ' and get_json_object(args, '$.gid') = '501#70')
            )
        group by d, ctype, cname;

--各端下单人数+GMV
--app端
        select action.d,action.ctype,count(distinct action.uid ) as `下单人数`,sum(all_price/100) as `GMV` from
        (
            select distinct uid,d,ctype
            from iyourcar_dw.dwd_all_action_hour_log
            where id in (11505,11506)  and
            d between '2020-06-25' and '2020-06-27' and
            ctype in (1,2) and
            get_json_object(args,"$.gid") = '501#69'
        ) as action
        inner join
        (
            select substr(ordertime,0,10) as d,uid,ctype,all_price
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
            where substr(ordertime,0,10) between '2020-06-25' and '2020-06-27' and
                  ctype in (1,2) and
                  order_status in (1,2,3) and
                  order_type in(2,3,4) and
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
            d between '2020-06-25' and '2020-06-27' and
            ctype=4 and
            get_json_object(args,"$.gid") in('501#69','501#70')
        ) as action
        inner join
        (
            select substr(ordertime,0,10) as d,uid,mall_type,all_price
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
            where substr(ordertime,0,10) between '2020-06-25' and '2020-06-27' and
                  ctype=4 and
                  order_status in (1,2,3) and
                  order_type in(2,3,4) and
                  all_price>0
        )as orders
        on action.uid = orders.uid and action.d = orders.d and action.mall_type=orders.mall_type
        group by action.d,action.mall_type;

----点击push的人，买了东西的人，GMV
--点击push
select d,id,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-06-30' and
id in(11762,12486) and
get_json_object(args,'$.push_id')=638485
group by d,id;

--买东西 and GMV
select click_list.d,id,count(distinct click_list.uid),sum(order_list.price)/100 as `GMV`
       from
    (select d, id,uid
    from (select distinct d, id,cid as cid
          from iyourcar_dw.dwd_all_action_hour_log
          where d between '2020-07-09' and '2020-06-30'
            and id in(11762,12486)
            and get_json_object(args, '$.push_id') = 638485
          ) as cid_list
             join iyourcar_dw.dws_extend_day_cid_map_uid as uid_list
                  on cid_list.cid = uid_list.cid) as click_list
    join
        (
            select substr(ordertime,0,10) as d,uid,sum(all_price) as price
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
            where ctype in(1,2) and all_price>0 and order_status in(1,2,3) and substr(ordertime,0,10) between '2020-07-09' and '2020-06-30'
            group by substr(ordertime,0,10),uid
            ) as order_list
    on click_list.d=order_list.d and click_list.uid=order_list.uid
    group by click_list.d,id;

-----扫了群空间二维码、下单的人、GMV
--('11339','11340','11828') 大型专题的页面e事件
select
        count(distinct open.cid) as `小程序广告图扫码人数`,
        count(distinct orders.uid) as `下单的人`,
        sum(orders.all_price) as `GMV`
from (select *
      from iyourcar_dw.dwd_all_action_hour_log
      where id=316
        and d='2020-06-25'
        and ctype = 4
        and get_json_object(args, "$.scene_id") in (1047, 1048, 1049)
     )
         as open
         inner join
     (select *
      from iyourcar_dw.dwd_all_action_hour_log
      where d='2020-06-25' and ctype = 4 and id in(11339,11338) and get_json_object(args,'$.redirect_target')=69) as mall
     on open.cid = mall.cid
     left join iyourcar_dw.dws_extend_day_cid_map_uid as map_list
     on open.cid=map_list.cid
     left join
    (
        select uid,all_price
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where ctype=4 and mall_type=1 and order_status in(1,2,3) and all_price>0 and substr(ordertime,0,10)='2020-06-25'
        ) as orders
    on map_list.uid=orders.uid;

--banner的曝光点击人数、下单人数、GMV
--曝光
select d,ctype,cname,count(distinct cid) as `banner曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829,11341)  and d between '2020-06-25' and '2020-07-01' and ctype in (1,2,4)
and get_json_object(args,'$.redirect_target') in (69,70)
group by d,ctype,cname;

--点击、下单、GMV
select click_cid.d,click_cid.ctype,click_cid.cname,
    count(click_cid.cid) as `点击人数`,
    count(case when unix_timestamp(orders.ordertime)*1000>st then orders.uid end) as `购买人数`,
    sum(case when unix_timestamp(orders.ordertime)*1000>st then all_price end) as GMV
from (select d, cid, ctype,cname,min(st) as st
      from iyourcar_dw.dwd_all_action_hour_log
      where d between '2020-06-25' and '2020-07-01'
        and id in (11340, 11339, 11828)
        and get_json_object(args, '$.redirect_target') in (69, 70)
        group by d, cid, ctype,cname) as click_cid
left join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on click_cid.cid=maps.cid
left join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on substr(orders.ordertime,0,10)=click_cid.d and orders.uid=maps.uid
group by click_cid.d,click_cid.ctype,click_cid.cname;

--优惠券领取情况和使用情况
select `日期`,`卷id`,`具体的券`,`满多少金额`,`点击商城首页领劵的人数`,`领取总量`,`领取的人数`,`使用总量`,`使用的人数`,`用券的订单的最终付款金额均值`
from (
    select coupon_user.get_d as `日期`,
           coupon_info.id as `卷id`,
           coupon_info.name as `具体的券`,
           coupon_info.require_amount/100 as `满多少金额`,
           coupon_click.coupon_click_uv as `点击商城首页领劵的人数`,
           count( coupon_user.uid) as `领取总量`,
           count(distinct coupon_user.uid) as `领取的人数`,
           sum( case when coupon_user.use_time is not null then 1 else 0 end) as `使用总量`,
           count(distinct case when coupon_user.use_time is not null then coupon_user.uid end) as `使用的人数`,
           if(isnull(avg(all_price/100)),0,avg(all_price/100)) as `用券的订单的最终付款金额均值`
          from
               -- 优惠券使用用户
        (
            select uid,coupon_id,status,substr(createtime,0,10) as get_d,substr(use_time,0,10) as use_d,use_time,is_view
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_coupon_user
            where substr(createtime,0,10) between '2020-06-25' and '2020-07-01'
        ) as coupon_user
        inner join
        (
            select *
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_coupon_info
        ) as coupon_info
        on coupon_info.id = coupon_user.coupon_id
        inner join
        (
            select d,count(distinct uid) as coupon_click_uv,info.name
            from iyourcar_dw.dwd_all_action_hour_log
            inner join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_coupon_info as info
            on  split(get_json_object(args,"$.redirect_target"),'/')[5] = info.id
            where dwd_all_action_hour_log.id in ('11339','11828','11340') and d between '2020-06-25' and '2020-07-01'
            and get_json_object(args,"$.redirect_target") like  "%coupon%"
            group by d,info.name
        ) as coupon_click
        on coupon_click.name = coupon_info.name and coupon_user.get_d = coupon_click.d
        left join
             ( select *,substr(ordertime,0,10) as d
                from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
               where substr(ordertime,0,10) between '2020-06-25' and '2020-07-01' and biz_type in (1,3) and order_status in (1,2,3) and coupon_no is not null
              ) as orders
        on orders.uid = coupon_user.uid and orders.d = coupon_user.use_d
    group by  coupon_user.get_d,coupon_info.id,coupon_info.name , coupon_info.require_amount/100,coupon_click.coupon_click_uv
    ) as final_result
order by  `日期`,`具体的券`;


--这几天的客单价分布
select `客单价`,count(order_no)
from
(select orders.order_no as order_no,round(sum(all_price)/100,0) as `客单价`
from (
          select uid,order_no
          from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
          where substr(ordertime, 0, 10) between '2020-06-25' and '2020-07-01'
          and mall_type=1
          and order_status in(1,2,3)
          and all_price>0
          and coupon_no is null
      ) as orders
        join
      (
          select order_no,all_price
          from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
          where item_id not in(1989,1939,1935,1984,1988,1940,1936,1985) and
          item_id  in (
839,
401,
409,
469,
458,
643,
406,
1900,
1897,
1890,
1882,
1881,
1298,
446,
415,
509,
1956,
1899,
505,
501,
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
870,
471,
435,
475,
330,
358,
617,
391,
1766,
1710,
1699,
731,
582,
577,
615,
1990,
1974,
1961,
1953,
1951,
1949,
1911,
1907,
1778,
717,
696,
667,
625,
550,
486,
390,
1623,
1496,
1444,
1290,
1019,
791,
750,
382,
1690,
489,
356,
975,
820,
1237,
387,
399,
403,
408,
419,
421,
518,
524,
530,
532,
533,
536,
537,
540,
543,
669,
844,
932,
934,
1397,
1653,
1989,
1939,
1935,
1984,
2127,
2205)
          ) as orders_item
          on orders.order_no=orders_item.order_no
        group by orders.order_no,uid) as final_data
group by `客单价`;


select *
from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order where coupon_price !=0 limit 10;

[{"id":"11341","a":"s","page":"/large_topic/:id","view":"341697","st":1593518674036,"args":"{\"gid\":\"206#638485\",\"card_box_type\":1,\"redirect_type\":0}","sess":"and-34164-1593518636297","et":1593518674036}]
[{"a":"p","et":1593518904965,"ref_page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","st":1593518671018,"id":"242","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","sess":"and-34164-1593518636297"},{"a":"s","et":1593518904964,"st":1593518671019,"view":"com.youcheyihou.iyoursuv.ui.fragment.ShopListFragment","args":"{\"group_id\":\"83\"}","id":"515","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","sess":"and-34164-1593518636297"},{"redirect_target":"","redirect_type":"","wx_app_original_id":"","gid":"206#416674"},{"id":"11340","a":"e","page":"/large_topic/:id","st":1593518904726,"args":"{\"gid\":\"206#416674\",\"card_box_type\":5,\"redirect_type\":501,\"redirect_target\":\"69\"}","sess":"and-34164-1593518636297","view":342119}]
[{"a":"p","et":1593518987676,"ref_page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","st":1593518905150,"id":"242","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","sess":"and-34164-1593518636297"},{"a":"s","et":1593518987675,"st":1593518905037,"view":"com.youcheyihou.iyoursuv.ui.fragment.ShopListFragment","args":"{\"group_id\":\"69\"}","id":"515","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","sess":"and-34164-1593518636297"},{"id":"11340","a":"e","page":"/large_topic/:id","st":1593518987523,"args":"{\"gid\":\"206#638485\",\"card_box_type\":12,\"redirect_type\":1,\"redirect_target\":\"https://res.youcheyihou.com/auto_home_mobile/coupon/316\"}","sess":"and-34164-1593518636297","view":341698}]
--点击端午出行
[{"a":"s","et":1593520023205,"st":1593520018354,"view":"com.youcheyihou.iyoursuv.ui.fragment.ShopListFragment","args":"{\"group_id\":\"83\"}","id":"515","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","sess":"and-34164-1593518636297"}]
[{"a":"s","et":1593520121833,"st":1593520113340,"view":"com.youcheyihou.iyoursuv.ui.fragment.ShopListFragment","args":"{\"group_id\":\"83\"}","id":"515","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","sess":"and-34164-1593518636297"}]

select *
from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info;

[{"a":"s","et":1593520214865,"st":1593520121835,"view":"com.youcheyihou.iyoursuv.ui.fragment.ShopListFragment","args":"{\"group_id\":\"69\"}","id":"515","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","sess":"and-34164-1593518636297"}]

--商品下单人数及GMV
select t1.*,t2.`商品点击人数`,t6.`商品下单人数`,t6.`GMV` from
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `商品曝光人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11338,11829,11341)  and d between '2020-06-25' and '2020-07-01' and cname in('APP_SUV','WXAPP_YCYH_PLUS')
and  get_json_object(args,"$.redirect_target") in (1989,	1935,	1984,	1990,	1623,	1766,	651,	1907,	731,	987,	1171,	2109,	2121,	2111,	2115,	868,	2124,	401,	446,	1298,	406,	469,	399,	419,	1953,	408,	1237,	1939,	2127,	2205)
group by d,get_json_object(args,"$.redirect_target")
) as t1
left join
(
select d,get_json_object(args,"$.redirect_target") as `商品spu`, count(distinct cid) as `商品点击人数` from iyourcar_dw.dwd_all_action_hour_log
where id in (11339,11828,11340)  and d between '2020-06-25' and '2020-07-01' and cname in('APP_SUV','WXAPP_YCYH_PLUS')
and  get_json_object(args,"$.redirect_target") in (1989,	1935,	1984,	1990,	1623,	1766,	651,	1907,	731,	987,	1171,	2109,	2121,	2111,	2115,	868,	2124,	401,	446,	1298,	406,	469,	399,	419,	1953,	408,	1237,	1939,	2127,	2205)
group by d,get_json_object(args,"$.redirect_target")
) as t2
on t1.d=t2.d and t1.`商品spu`= t2.`商品spu`
left join
    (
select orders.d as d,items.item_id as `商品spu`,count(distinct orders.uid) as `商品下单人数`,(sum(items.all_price))/100 as `GMV` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-25' and '2020-07-01'   and order_status  in (1,2,3) and all_price>0 and mall_type=1
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-06-25' and '2020-07-01' and item_id in
		(1989,	1935,	1984,	1990,	1623,	1766,	651,	1907,	731,	987,	1171,	2109,	2121,	2111,	2115,	868,	2124,	401,	446,	1298,	406,	469,	399,	419,	1953,	408,	1237,	1939,	2127,	2205)
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t6
on  t2.`商品spu`= t6.`商品spu` and t6.d=t2.d;

--以天为维度(活动页面访问人数，活动商品GMV，总计GMV)
--活动商品GMV
select d,sum(all_price)
from (select uid,substr(ordertime,0,10) as d,order_no
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10) between '2020-06-25' and '2020-07-01'
        and order_status in(1,2,3)
        and all_price>0
    ) as orders
join
    (
        select uid,order_no,all_price
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
        where item_id in(
839,
401,
409,
469,
458,
643,
406,
1900,
1897,
1890,
1882,
1881,
1298,
446,
415,
509,
1956,
1899,
505,
501,
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
870,
471,
435,
475,
330,
358,
617,
391,
1766,
1710,
1699,
731,
582,
577,
615,
1990,
1974,
1961,
1953,
1951,
1949,
1911,
1907,
1778,
717,
696,
667,
625,
550,
486,
390,
1623,
1496,
1444,
1290,
1019,
791,
750,
382,
1690,
489,
356,
975,
820,
1237,
387,
399,
403,
408,
419,
421,
518,
524,
530,
532,
533,
536,
537,
540,
543,
669,
844,
932,
934,
1397,
1653,
1989,
1939,
1935,
1984,
2127,
2205)) as item
on orders.order_no=item.order_no
group by d;

select d,sum(all_price)
from (select substr(ordertime,0,10) as d,all_price
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10) between '2020-06-25' and '2020-07-01'
        and order_status in(1,2,3)
        and all_price>0) as a
group by d;

actions	[{"a":"s","et":1594352312631,"st":1594352291174,"view":"com.youcheyihou.iyoursuv.ui.fragment.ShopListFragment","args":"{\"group_id\":\"83\"}","id":"515","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","sess":"and-34164-1594352152107"}]
actions	[{"id":"11341","a":"s","page":"/large_topic/:id","view":"346242","st":1594352315666,"args":"{\"gid\":\"206#665027\",\"card_box_type\":1,\"redirect_type\":0}","sess":"and-34164-1594352152107","et":1594352315666},{"id":"11341","a":"s","page":"/large_topic/:id","view":"346241","st":1594352315654,"args":"{\"gid\":\"206#665027\",\"card_box_type\":1,\"redirect_type\":520,\"redirect_target\":\"6\"}","sess":"and-34164-1594352152107","et":1594352315654},{"id":"11341","a":"s","page":"/large_topic/:id","view":"346240","st":1594352315639,"args":"{\"gid\":\"206#665027\",\"card_box_type\":6,\"redirect_type\":511,\"redirect_target\":\"446\"}","sess":"and-34164-1594352152107","et":1594352315639},{"id":"11341","a":"s","page":"/large_topic/:id","view":"346239","st":1594352315625,"args":"{\"gid\":\"206#665027\",\"card_box_type\":6,\"redirect_type\":511,\"redirect_target\":\"469\"}","sess":"and-34164-1594352152107","et":1594352315625},{"id":"11341","a":"s","page":"/large_topic/:id","view":"346238","st":1594352315611,"args":"{\"gid\":\"206#665027\",\"card_box_type\":1,\"redirect_type\":511,\"redirect_target\":\"2241\"}","sess":"and-34164-1594352152107","et":1594352315611}]
actions	[{"id":"11336","a":"p","ref_page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity","page":"/large_topic/:id","st":1594352315913,"et":1594352315913,"args":"{\"gid\":\"206#665027\"}","sess":"and-34164-1594352152107"},{"id":"11341","a":"s","page":"/large_topic/:id","view":"346265","st":1594352315906,"args":"{\"gid\":\"206#665027\",\"card_box_type\":1,\"redirect_type\":0}","sess":"and-34164-1594352152107","et":1594352315906},{"id":"11341","a":"s","page":"/large_topic/:id","view":"346264","st":1594352315896,"args":"{\"gid\":\"206#665027\",\"card_box_type\":12,\"redirect_type\":511,\"redirect_target\":\"687\"}","sess":"and-34164-1594352152107","et":1594352315896},{"id":"11341","a":"s","page":"/large_topic/:id","view":"346263","st":1594352315887,"args":"{\"gid\":\"206#665027\",\"card_box_type\":12,\"redirect_type\":511,\"redirect_target\":\"387\"}","sess":"and-34164-1594352152107","et":1594352315887},{"id":"11341","a":"s","page":"/large_topic/:id","view":"346262","st":1594352315879,"args":"{\"gid\":\"206#665027\",\"card_box_type\":12,\"redirect_type\":511,\"redirect_target\":\"1237\"}","sess":"and-34164-1594352152107","et":1594352315879}]

actions	[{"a":"p","et":1594352573259,"ref_page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-NewsFragment","st":1594352290795,"id":"242","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","sess":"and-34164-1594352152107"},{"a":"s","et":1594352573258,"st":1594352291175,"view":"com.youcheyihou.iyoursuv.ui.fragment.ShopListFragment","args":"{\"group_id\":\"114\"}","id":"515","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-ShopAndWelfareFragment","sess":"and-34164-1594352152107"},{"a":"s","et":1594352573257,"st":1594352263191,"view":"","args":"{\"page_type\":\"条件选车\"}","id":"11941","page":"com.youcheyihou.iyoursuv.ui.activity.MainActivity-CarFragment","sess":"and-34164-1594352152107"},{"id":"11340","a":"e","page":"/large_topic/:id","st":1594352573160,"args":"{\"gid\":\"206#665027\",\"card_box_type\":1,\"redirect_type\":511,\"redirect_target\":\"1420\"}","sess":"and-34164-1594352152107","view":346233}]
--活动页面访问人数
select d,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-06-25' and '2020-07-01'
and id in(11338,11829,11341,11339,11828,11340)
and get_json_object(args,'$.gid') in('206#638485','206#638810')
group by d;




