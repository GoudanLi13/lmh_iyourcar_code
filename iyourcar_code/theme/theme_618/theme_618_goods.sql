--整合
--APP端数据销售情况
--set mapreduce.job.queuename = dailyDay;
with item_info as (
    select id, name from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info where biz_type in (1,3)
)
select t_click.d,
       item_info.id as `商品id`,
        item_info.name as `商品名称`,
       t_click.`APP商品点击人数`,
       t_detail.`APP商品详情页人数`,
       t_detail_stay.`APP商品详情页人均停留时长`,
       if(isnull(t_cart.`APP商品加购人数`),0,t_cart.`APP商品加购人数`),
       if(isnull(t_order.`APP商品下单人数`),0,t_order.`APP商品下单人数`) from
(
        select
               d,
            actions.spu as `商品spu`,
            count(distinct  cid) as `APP商品点击人数`
                from
                 (
                    select action.*,get_json_object(args,"$.spu") as spu from
                    iyourcar_dw.dwd_all_action_hour_log  as action
                    inner join
                    iyourcar_dw.dwd_all_action_day_event_group as evt
                    on action.id = evt.action_id
                    where event_group_id = 25
                    and action.d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
                    union all
                    select *,get_json_object(args,"$.redirect_target") as spu from iyourcar_dw.dwd_all_action_hour_log where id in ('11340')
                    and get_json_object(args,"$.gid")= '206#549198'
                    and d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
                ) as actions
            group by d,actions.spu
) as t_click
    inner join item_info on t_click.`商品spu` = item_info.id
left join
(
           select
                  d,
            actions.spu as `商品spu`,
                count(distinct  cid) as `APP商品详情页人数`
                from
                 (
                    select action.*,get_json_object(args,"$.spu") as spu from
                    iyourcar_dw.dwd_all_action_hour_log  as action
                    inner join
                    iyourcar_dw.dwd_all_action_day_event_group as evt
                    on action.id = evt.action_id
                    where event_group_id = 29
                    and action.d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
                     ) as actions
            group by d,actions.spu
) as t_detail
on t_click.d = t_detail.d and t_click.`商品spu`= t_detail.`商品spu`
left join
(
        select
               d,
            actions.spu as `商品spu`,
               sum(et-st)/count(distinct cid) as  `APP商品详情页人均停留时长`
                from
                 (
                    select action.*,get_json_object(args,"$.spu") as spu from
                    iyourcar_dw.dwd_all_action_hour_log  as action
                    inner join
                    iyourcar_dw.dwd_all_action_day_event_group as evt
                    on action.id = evt.action_id
                    where event_group_id = 29
                    and action.d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
                     ) as actions
            group by d,actions.spu
) as t_detail_stay
on t_detail.d = t_detail_stay.d and t_detail.`商品spu`= t_detail_stay.`商品spu`
left join
(
        select
               d,
            actions.spu as `商品spu`,
            count(distinct cid) as `APP商品加购人数`
                from
                 (
                    select action.*,get_json_object(args,"$.spu") as spu from
                    iyourcar_dw.dwd_all_action_hour_log  as action
                    inner join
                    iyourcar_dw.dwd_all_action_day_event_group as evt
                    on action.id = evt.action_id
                    where event_group_id = 26
                    and action.d between '2020-06-01' and '2020-06-18' and ctype in (1,2)
                     ) as actions
            group by d,actions.spu
) as t_cart
on t_detail_stay.d = t_cart.d and t_detail_stay.`商品spu`= t_cart.`商品spu`
left join
(
select orders.d,items.item_id as `商品spu` ,count(distinct orders.uid) as `APP商品下单人数` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-18' and ctype in (1,2) and order_status  in (1,2,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-06-01' and '2020-06-18'
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t_order
on t_cart.d = t_order.d and t_cart.`商品spu`= t_order.`商品spu`
order by t_click.d,t_click.`APP商品点击人数` desc;
--小程序端销售情况
--整合
--APP端数据销售情况
--set mapreduce.job.queuename = dailyDay;
with item_info as (
    select id, name from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info where biz_type in (1,3)
)
select t_click.d,
       item_info.id as `商品id`,
        item_info.name as `商品名称`,
       t_click.`小程序商品点击人数`,
       t_detail.`小程序商品详情页人数`,
       t_detail_stay.`小程序商品详情页人均停留时长`,
       if(isnull(t_cart.`小程序商品加购人数`),0,t_cart.`小程序商品加购人数`),
       if(isnull(t_order.`小程序商品下单人数`),0,t_order.`小程序商品下单人数`) from
(
        select
               d,
            actions.spu as `商品spu`,
            count(distinct  cid) as `小程序商品点击人数`
                from
                 (
                    select action.*,get_json_object(args,"$.spu") as spu from
                    iyourcar_dw.dwd_all_action_hour_log  as action
                    inner join
                    iyourcar_dw.dwd_all_action_day_event_group as evt
                    on action.id = evt.action_id
                    where event_group_id = 25
                    and action.d between '2020-06-01' and '2020-06-18' and ctype  = 4
                    union all
                    select *,get_json_object(args,"$.redirect_target") as spu from iyourcar_dw.dwd_all_action_hour_log where id in ('11340')
                    and get_json_object(args,"$.group_id")= '206#549198'
                    and d between '2020-06-01' and '2020-06-18' and ctype  = 4
                ) as actions
            group by d,actions.spu
) as t_click
    inner join item_info on t_click.`商品spu` = item_info.id
left join
(
           select
                  d,
            actions.spu as `商品spu`,
                count(distinct  cid) as `小程序商品详情页人数`
                from
                 (
                    select action.*,get_json_object(args,"$.spu") as spu from
                    iyourcar_dw.dwd_all_action_hour_log  as action
                    inner join
                    iyourcar_dw.dwd_all_action_day_event_group as evt
                    on action.id = evt.action_id
                    where event_group_id = 29
                    and action.d between '2020-06-01' and '2020-06-18' and ctype  = 4
                     ) as actions
            group by d,actions.spu
) as t_detail
on t_click.d = t_detail.d and t_click.`商品spu`= t_detail.`商品spu`
left join
(
        select
               d,
            actions.spu as `商品spu`,
               sum(et-st)/count(distinct cid) as  `小程序商品详情页人均停留时长`
                from
                 (
                    select action.*,get_json_object(args,"$.spu") as spu from
                    iyourcar_dw.dwd_all_action_hour_log  as action
                    inner join
                    iyourcar_dw.dwd_all_action_day_event_group as evt
                    on action.id = evt.action_id
                    where event_group_id = 29
                    and action.d between '2020-06-01' and '2020-06-18' and ctype  = 4
                     ) as actions
            group by d,actions.spu
) as t_detail_stay
on t_detail.d = t_detail_stay.d and t_detail.`商品spu`= t_detail_stay.`商品spu`
left join
(
        select
               d,
            actions.spu as `商品spu`,
            count(distinct cid) as `小程序商品加购人数`
                from
                 (
                    select action.*,get_json_object(args,"$.spu") as spu from
                    iyourcar_dw.dwd_all_action_hour_log  as action
                    inner join
                    iyourcar_dw.dwd_all_action_day_event_group as evt
                    on action.id = evt.action_id
                    where event_group_id = 26
                    and action.d between '2020-06-01' and '2020-06-18' and ctype  = 4
                     ) as actions
            group by d,actions.spu
) as t_cart
on t_detail_stay.d = t_cart.d and t_detail_stay.`商品spu`= t_cart.`商品spu`
left join
(
select orders.d,items.item_id as `商品spu` ,count(distinct orders.uid) as `小程序商品下单人数` from
(
select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-06-01' and '2020-06-18' and ctype  = 4  and order_status  in (1,2,3) and biz_type in (1,3)
) as orders
inner join
(select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
where substr(createtime ,0,10) between '2020-06-01' and '2020-06-18'
) as items
on orders.order_no = items.order_no
group by orders.d,items.item_id
) as t_order
on t_cart.d = t_order.d and t_cart.`商品spu`= t_order.`商品spu`
order by t_click.`小程序商品点击人数` desc;