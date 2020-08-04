with visit_mall as
         (
select distinct ctype,cname,cid,d from iyourcar_dw.dwd_all_action_hour_log log
join iyourcar_dw.dwd_all_action_day_event_group event_group
on log.id=event_group.event_id
where log.d between date_sub('2020-07-28',14) and  '2020-07-28' and event_group.event_group_id=20
)
select visit.d as`日期`,
       case when visit.mall_type = '1' then '有车币商城'
           when visit.mall_type = '2' then '声望商城'
               end as `商城类型`,
       case when visit.ctype = 'And' then '安卓' else visit.ctype end  as `平台`,visit.`新用户访问商城人数`,visit.`老用户访问商城人数`,
       (visit.`新用户访问商城人数`+visit.`老用户访问商城人数`)/global_uv.dau as `商城DAU占比`,
       mall_transformation_data.`商品曝光次数`,mall_transformation_data.`商品列表点击次数`,
       mall_transformation_data.`商品详情页次数`,mall_transformation_data.`商品加购次数`,
       mall_transformation_data.`商品下单次数`,mall_transformation_data.`商品有效下单次数`,
       mall_transformation_data.`商品无效下单次数`,
       mall_transformation_data.`商品列表点击次数` / mall_transformation_data.`商品曝光次数` as `点击率`,
       mall_transformation_data.`商品加购次数` / mall_transformation_data.`商品详情页次数` as `加购率` ,
       mall_transformation_data.`商品下单次数` / mall_transformation_data.`商品详情页次数` as `下单率`,
       mall_transformation_data.`商品有效下单次数` / mall_transformation_data.`商品下单次数`as `有效下单率`,
       mall_transformation_data.`利润` as `利润`
       from
(
select visit_mall.d ,
case when visit_mall.ctype = '1' then 'iOS'
     when visit_mall.ctype = '2' then 'And'
     when visit_mall.ctype = '4' then '小程序' end as ctype,
case when visit_mall.cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when visit_mall.cname  = 'WXAPP_YCQKJ' then '2' end as mall_type,
count(distinct case when new_users.cid is not null then visit_mall.cid end) as `新用户访问商城人数`,
count(distinct case when new_users.cid is null then visit_mall.cid end) as `老用户访问商城人数`
from
visit_mall
left join
(select * from tmp.mall_new_user_cid_all_ctype_cname where d between date_sub('2020-07-28',14) and  '2020-07-28') as new_users
on visit_mall.cid = new_users.cid and visit_mall.d = new_users.d
group by visit_mall.d,visit_mall.ctype,case when visit_mall.cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when visit_mall.cname  = 'WXAPP_YCQKJ' then '2' end
) as visit
inner join (
select t1.d,t1.`ctype`,t1.mall_type, t1.`商品曝光次数`,t2.`商品列表点击次数`,t3.`商品详情页次数`,t5.`商品加购次数`,t6.`商品下单次数`,t8.`商品有效下单次数`,t9.`商品无效下单次数`,t10.`利润` from
(
select d,
       case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end as ctype,
case when cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when cname  = 'WXAPP_YCQKJ' then '2' end as mall_type,
       count(cid) as `商品曝光次数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 24) as e_group
    on action.id = e_group.event_id
    inner join

    (select  item_id,item_name,row_number() over (partition by item_id order by substr(createtime,0,10) desc) as rank
    from
         iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
        ) as items
on get_json_object(args,"$.spu") = items.item_id
where d between date_sub('2020-07-28',14) and  '2020-07-28' and items.rank =1
group by d,
       case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end ,
case when cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when cname  = 'WXAPP_YCQKJ' then '2' end
) as t1
left join
(
select d,
       case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end as ctype,
case when cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when cname  = 'WXAPP_YCQKJ' then '2' end as mall_type,
       count(cid) as `商品列表点击次数` from
    (
        select d,ctype,cname,cid from iyourcar_dw.dwd_all_action_hour_log as action
        inner join
        (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 25) as e_group
        on action.id = e_group.event_id
        where d between date_sub('2020-07-28',14) and  '2020-07-28'
        union all
        select d,ctype,cname,cid  from
        (
             select cid,
                    get_json_object(args,"$.redirect_target") as spu,
                    split(get_json_object(args,"$.gid") ,'#')[1] as gid,
                    ctype,
                    cname,
                    d
             from iyourcar_dw.dwd_all_action_hour_log where id in ('11339','11340','11828')
            and d between date_sub('2020-07-28',14) and  '2020-07-28'
        ) as big_theme
       inner join
       (
           select special_no
           from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info where special_no is not null
       ) as theme_info
        on big_theme.gid = theme_info.special_no
        inner join
        (
            select id from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info
        ) as item_info
        on big_theme.spu = item_info.id
    ) as a

group by d,
       case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end ,
case when cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when cname  = 'WXAPP_YCQKJ' then '2' end
) as t2
on t1.d = t2.d and t1.ctype =t2.ctype and t1.mall_type =t2.mall_type
left join
(
    select d,
           case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end as ctype,
case when cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when cname  = 'WXAPP_YCQKJ' then '2' end as mall_type,
           count( cid) as  `商品详情页次数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 29) as e_group
    on action.id = e_group.event_id
            inner join
        (
            select id from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info
        ) as item_info
        on get_json_object(action.args,"$.spu") = item_info.id
    where action.d  between date_sub('2020-07-28',14) and  '2020-07-28'
    group by d,
       case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end ,
case when cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when cname  = 'WXAPP_YCQKJ' then '2' end
) as t3
on t2.d = t3.d and t2.ctype =t3.ctype and t2.mall_type =t3.mall_type
left join
(
    select d,
           case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end as ctype,
case when cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when cname  = 'WXAPP_YCQKJ' then '2' end as mall_type,
           count( cid) as  `商品加购次数` from iyourcar_dw.dwd_all_action_hour_log as action
    inner join
    (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id  = 26) as e_group
    on action.id = e_group.event_id
    left join
    (select id as sku,item_id as spu from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku)
        as sku_spu_map
    on sku_spu_map.sku  = get_json_object(action.args,"$.sku")
    where action.d  between date_sub('2020-07-28',14) and  '2020-07-28'
    group by d,
       case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end ,
case when cname in ('APP_SUV','WXAPP_YCYH_PLUS','有车以后+') then '1'
when cname  = 'WXAPP_YCQKJ' then '2' end
) as t5
on t3.d = t5.d and t3.ctype =t5.ctype and t3.mall_type =t5.mall_type
left join
(
select orders.d,
       case when ctype = '1' then 'iOS'
       when ctype = '2' then 'And'
       when ctype = '4' then '小程序' end as ctype,
       mall_type,
       count( orders.uid) as `商品下单次数` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10)  between date_sub('2020-07-28',14) and  '2020-07-28'   and order_status  in (1,2,3,5) and biz_type in (1,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10)  between date_sub('2020-07-28',14) and  '2020-07-28') as items
on orders.order_no = items.order_no
group by orders.d,
       case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end,
     mall_type
) as t6
on t1.d = t6.d and t1.ctype = t6.ctype and t1.mall_type = t6.mall_type
left join
(
select orders.d,
     case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end as ctype,
     mall_type,
     count( orders.uid) as `商品有效下单次数` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10)  between date_sub('2020-07-28',14) and  '2020-07-28'   and order_status  in (1,2,3) and biz_type in (1,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10)  between date_sub('2020-07-28',14) and  '2020-07-28') as items
on orders.order_no = items.order_no
group by orders.d,
       case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end,
     mall_type
) as t8
on t1.d = t8.d and t1.ctype =t8.ctype and t1.mall_type =t8.mall_type
left join
(
select orders.d,
     case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end as ctype,
     mall_type,
     count( orders.uid) as `商品无效下单次数` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10)  between date_sub('2020-07-28',14) and  '2020-07-28'   and order_status  in (4,5) and biz_type in (1,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10)  between date_sub('2020-07-28',14) and  '2020-07-28') as items
on orders.order_no = items.order_no
group by orders.d,
       case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end,
     mall_type
) as t9
on t1.d = t9.d and t1.ctype =t9.ctype and t1.mall_type =t9.mall_type
left join
(
  select d,
         case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end as ctype,
         mall_type,
 (sum(case when order_status in(1,2,3) then all_price end) - sum(case when order_status in(1,2,3) then cost end) + sum(case when order_status in(1,2,3) then postage_price end) + count(case when is_open_privilege_card = 1 and order_status in(1,2,3,5) and paytime is not null then 1 end) * 5) / 100 as `利润`
from (
         select orders.order_no,
                orders.all_price,
                orders.postage_price,
                is_open_privilege_card,
                order_status,
                paytime,
                ctype,
                mall_type,
                d,
                sum(item.cost_price * item.item_num) as cost
         from (select uid, order_no, all_price, postage_price, is_open_privilege_card, order_status,paytime,ctype,mall_type,substr(ordertime, 0, 10) as d
               from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
               where substr(ordertime, 0, 10) between date_sub('2020-07-28',14) and  '2020-07-28'
                 and all_price > 0
                    ) as orders
                  join
              iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item as item
              on item.order_no = orders.order_no
         group by orders.order_no, orders.all_price, orders.postage_price, is_open_privilege_card, order_status,paytime,ctype,mall_type,d ) as a
    group by d,
       case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end ,
    mall_type


  ) as t10
on t1.d = t10.d and t1.ctype =t10.ctype and t1.mall_type =t10.mall_type

) as mall_transformation_data
on visit.d = mall_transformation_data.d and visit.ctype = mall_transformation_data.ctype and visit.mall_type = mall_transformation_data.mall_type
inner join
(
select d,case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end as ctype,count(distinct cid) as dau from iyourcar_dw.dwd_all_action_day_active_cid
where d between date_sub('2020-07-28',14) and  '2020-07-28'
group by d,case when ctype = '1' then 'iOS'
     when ctype = '2' then 'And'
     when ctype = '4' then '小程序' end
) as global_uv
on visit.d = global_uv.d and visit.ctype = global_uv.ctype
order by `日期` desc;

