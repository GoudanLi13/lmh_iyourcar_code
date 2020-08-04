
--0707去掉月数据
drop table tmp.rpt_mall_operation_wx_week_and_month;
set hive.groupby.skewindata = false;
create table tmp.rpt_mall_operation_wx_week_and_month as
with order_tmp_7 as
         (
             select x.*,y.cost from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as x
                                        left join
                                    (
                                        select a.order_no,sum(b.cost_price*a.item_num) as cost
                                        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item a
                                                 left join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku b
                                                           on a.item_sku_id = b.id
                                        group by a.order_no
                                    )y
                                    on x.order_no=y.order_no
             where biz_type in (1,3) and substr(ordertime,1,10)<='2020-07-29' and substr(ordertime,1,10)>='2020-07-01'
         )

select action_data.`时间周期` as time_period
     ,order_data.`商城类型` as mall_type
     ,order_data.`平台` as platform
     ,case when order_data.`商城类型`='有车币商城' and order_data.`平台`='iOS' then `有车币商城ios访问人数`
           when order_data.`商城类型`='有车币商城' and order_data.`平台`='And' then `有车币商城And访问人数`
           when order_data.`商城类型`='有车币商城' and order_data.`平台`='App' then `有车币商城App访问人数`
           when order_data.`商城类型`='有车币商城' and order_data.`平台`='小程序' then `有车币商城小程序访问人数`
           when order_data.`商城类型`='有车币商城' and order_data.`平台`='总计' then `有车币商城总计访问人数`
           when order_data.`商城类型`='声望商城' and order_data.`平台`='总计' then `声望商城访问人数`
           when order_data.`商城类型`='总计' and order_data.`平台`='总计' then `访问合计` end as mall_visit_count
     ,order_data.`下单人数` as order_user_count
     ,order_data.`下单人数`/(case when order_data.`商城类型`='有车币商城' and order_data.`平台`='iOS' then `有车币商城iOS访问人数`
                              when order_data.`商城类型`='有车币商城' and order_data.`平台`='And' then `有车币商城And访问人数`
                              when order_data.`商城类型`='有车币商城' and order_data.`平台`='App' then `有车币商城App访问人数`
                              when order_data.`商城类型`='有车币商城' and order_data.`平台`='小程序' then `有车币商城小程序访问人数`
                              when order_data.`商城类型`='有车币商城' and order_data.`平台`='总计' then `有车币商城总计访问人数`
                              when order_data.`商城类型`='声望商城' and order_data.`平台`='总计' then `声望商城访问人数`
                              when order_data.`商城类型`='总计' and order_data.`平台`='总计' then `访问合计` end) as `conversion_rate`
     ,order_data.`订单数` as order_count
     ,order_data.`GMV`
     ,order_data.`GMV`/order_data.`下单人数` as `customer_price`
     ,order_data.`首单用户数` as first_order_user_count
     ,order_data.`首单用户数`/order_data.`下单人数` as `first_order_user_portion`
     ,order_data.`开卡用户数` as open_card_user_count
     ,(order_data.`GMV`-order_data.`成本` +order_data.`开卡用户数`*5 +order_data.`邮费`) as `all_profit`
     ,order_data.`GMV`-order_data.`成本` as `goods_profit`
     ,order_data.`开卡用户数`*5 as `open_card_profit`
     ,order_data.`邮费` as mail_profit
from
    (
-- 周数据
        select concat('2020-07-29','~','2020-07-01') as `时间周期`
             ,count(distinct case when ctype in (1)  and cname = 'APP_SUV' then cid end ) as `有车币商城iOS访问人数`
             ,count(distinct case when ctype in (2)  and cname = 'APP_SUV' then cid end ) as `有车币商城And访问人数`
             ,count(distinct case when ctype in (1,2)  and cname = 'APP_SUV' then cid end ) as `有车币商城App访问人数`
             ,count(distinct case when ctype = 4 and cname = 'WXAPP_YCYH_PLUS' then cid end ) as `有车币商城小程序访问人数`
             ,count(distinct case when ((cname='APP_SUV' and ctype in (1,2)) or (cname='WXAPP_YCYH_PLUS' and ctype=4 )) then cid end ) as `有车币商城总计访问人数`
             ,count(distinct case when ctype = 4 and cname = 'WXAPP_YCQKJ' then cid end ) as `声望商城访问人数`
             ,count(distinct cid) as `访问合计`
        from iyourcar_dw.dwd_all_action_hour_log
                 inner join (select event_id from iyourcar_dw.dwd_all_action_day_event_group where event_group_id = 20) as visit_mall
                            on dwd_all_action_hour_log.id = visit_mall.event_id
        where d<='2020-07-29'  and d>='2020-07-01'

    )action_data

        left join
    (
        --周数据
-- 有车币商城分平台
        select concat('2020-07-29','~','2020-07-01') as `时间周期`
             ,'有车币商城' as `商城类型`
             ,case when ctype =1  then 'iOS' when ctype = 2 then 'And' when ctype=4 then '小程序' end as `平台`
             ,count(distinct case when order_status in (1,2,3)  then uid end) as `下单人数`
             ,count(distinct case when order_status in (1,2,3)  then order_no end) as `订单数`
             ,sum(case when order_status in (1,2,3)  then all_price end)/100 as `GMV`
             ,count(distinct case when is_first_order  =1 and order_status in (1,2,3) then uid else null end) as `首单用户数`
             ,count(distinct case when is_open_privilege_card  =1 and order_status in (1,2,3,5)  and paytime is not null then uid else null end) as `开卡用户数`
             ,sum(case when order_status in (1,2,3) then postage_price end)/100 as `邮费`
             ,sum(case when order_status in (1,2,3) then cost end)/100 as `成本`
        from order_tmp_7
        where  mall_type=1
        group by case when ctype =1  then 'iOS' when ctype = 2 then 'And' when ctype=4 then '小程序' end
        having `平台` is not null

        union all
--有车币商城的App单独拿出来

        select concat('2020-07-29','~','2020-07-01') as `时间周期`
             ,'有车币商城' as `商城类型`
             , 'App' as `平台`
             ,count(distinct case when order_status in (1,2,3)  then uid end) as `下单人数`
             ,count(distinct case when order_status in (1,2,3)  then order_no end) as `订单数`
             ,sum(case when order_status in (1,2,3)  then all_price end)/100 as `GMV`
             ,count(distinct case when is_first_order  =1 and order_status in (1,2,3) then uid else null end) as `首单用户数`
             ,count(distinct case when is_open_privilege_card  =1 and order_status in (1,2,3,5)  and paytime is not null then uid else null end) as `开卡用户数`
             ,sum(case when order_status in (1,2,3) then postage_price end)/100 as `邮费`
             ,sum(case when order_status in (1,2,3) then cost end)/100 as `成本`
        from order_tmp_7
        where  mall_type=1 and ctype in (1,2)
        union all
-- 两个商城分平台
        select concat('2020-07-29','~','2020-07-01') as `时间周期`
             ,case mall_type when 1 then '有车币商城' when 2 then '声望商城' end as `商城类型`
             ,'总计' as `平台`
             ,count(distinct case when order_status in (1,2,3)  then uid end) as `下单人数`
             ,count(distinct case when order_status in (1,2,3)  then order_no end) as `订单数`
             ,sum(case when order_status in (1,2,3)  then all_price end)/100 as `GMV`
             ,count(distinct case when is_first_order  =1 and order_status in (1,2,3) then uid else null end) as `首单用户数`
             ,count(distinct case when is_open_privilege_card  =1 and order_status in (1,2,3,5)  and paytime is not null then uid else null end) as `开卡用户数`
             ,sum(case when order_status in (1,2,3) then postage_price end)/100 as `邮费`
             ,sum(case when order_status in (1,2,3) then cost end)/100 as `成本`
        from order_tmp_7
        group by case mall_type when 1 then '有车币商城' when 2 then '声望商城' end
        union all
-- 全局总计
        select concat('2020-07-29','~','2020-07-01') as `时间周期`
             ,'总计' as `商城类型`
             ,'总计' as `平台`
             ,count(distinct case when order_status in (1,2,3)  then uid end) as `下单人数`
             ,count(distinct case when order_status in (1,2,3)  then order_no end) as `订单数`
             ,sum(case when order_status in (1,2,3)  then all_price end)/100 as `GMV`
             ,count(distinct case when is_first_order  =1 and order_status in (1,2,3) then uid else null end) as `首单用户数`
             ,count(distinct case when is_open_privilege_card  =1 and order_status in (1,2,3,5)  and paytime is not null then uid else null end) as `开卡用户数`
             ,sum(case when order_status in (1,2,3) then postage_price end)/100 as `邮费`
             ,sum(case when order_status in (1,2,3) then cost end)/100 as `成本`
        from order_tmp_7
    )order_data
    on action_data.`时间周期`= order_data.`时间周期`;