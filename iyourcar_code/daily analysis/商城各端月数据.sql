--全体用户数据
--各端复购人数

    select month,mall_type,ctype,count(uid) as `复购人数` from
                                                                      (
        select
        month(ordertime) as month,
        orders.uid
        ,orders.mall_type
        ,orders.ctype
from
    (select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) between '2020-01-01' and '2020-07-31'
 and biz_type in (1,3) and order_status in (1,2,3) and all_price>0) as orders
        join iyourcar_dw.dws_extend_day_cid_map_uid as maps
        on maps.uid=orders.uid
        join (select * from tmp.mall_new_user_cid_all_ctype_cname where d between '2020-01-01' and '2020-07-31') as news
        on news.cid=maps.cid and news.ctype=orders.ctype and orders.mall_type=if(news.cname='WXAPP_YCQKJ',2,1) and month(news.d)=month(ordertime)
group by month(ordertime),orders.uid
        ,orders.mall_type
        ,orders.ctype
        having count(orders.uid) > 1
        ) as a
group by month,mall_type,ctype;

--新用户月数据
--part 1
drop table if exists tmp.rpt_mall_operation_wx_month;
set hive.groupby.skewindata = false;
create table tmp.rpt_mall_operation_wx_month as
with order_tmp_30 as
         (
             select x.*,y.cost from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as x
                                    join iyourcar_dw.dws_extend_day_cid_map_uid as maps
                                    on maps.uid=x.uid
                                    join (select * from tmp.mall_new_user_cid_all_ctype_cname where d between '2020-07-01' and '2020-07-31') as news
        on news.cid=maps.cid and news.ctype=x.ctype and x.mall_type=if(news.cname='WXAPP_YCQKJ',2,1)
                                        left join
                                    (
                                        select a.order_no,sum(b.cost_price*a.item_num) as cost
                                        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item a
                                                 left join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_sku b
                                                           on a.item_sku_id = b.id
                                        group by a.order_no
                                    )y
                                    on x.order_no=y.order_no

             where biz_type in (1,3) and substr(ordertime,1,10)>='2020-07-01' and substr(ordertime,1,10)<='2020-07-31' and all_price>0
         )
select action_data.`时间周期` as time_period
     ,order_data.`商城类型` as mall_type
     ,order_data.`平台` as platform
     ,case when order_data.`商城类型`='有车币商城' and order_data.`平台`='iOS' then `有车币商城ios访问人数`
           when order_data.`商城类型`='有车币商城' and order_data.`平台`='And' then `有车币商城And访问人数`

           when order_data.`商城类型`='有车币商城' and order_data.`平台`='小程序' then `有车币商城小程序访问人数`

           when order_data.`商城类型`='声望商城' and order_data.`平台`='总计' then `声望商城访问人数`
           end as mall_visit_count
     ,order_data.`下单人数` as order_user_count
     ,order_data.`下单人数`/(case when order_data.`商城类型`='有车币商城' and order_data.`平台`='iOS' then `有车币商城iOS访问人数`
                              when order_data.`商城类型`='有车币商城' and order_data.`平台`='And' then `有车币商城And访问人数`

                              when order_data.`商城类型`='有车币商城' and order_data.`平台`='小程序' then `有车币商城小程序访问人数`
                              when order_data.`商城类型`='声望商城' and order_data.`平台`='总计' then `声望商城访问人数`
                               end) as `conversion_rate`
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
--月数据
        select concat('2020-07-01','~','2020-07-31') as `时间周期`
             ,count(distinct case when ctype in (1)  and cname = 'APP_SUV' then cid end ) as `有车币商城iOS访问人数`
             ,count(distinct case when ctype in (2)  and cname = 'APP_SUV' then cid end ) as `有车币商城And访问人数`
             ,count(distinct case when ctype = 4 and cname = 'WXAPP_YCYH_PLUS' then cid end ) as `有车币商城小程序访问人数`
             ,count(distinct case when ctype = 4 and cname = 'WXAPP_YCQKJ' then cid end ) as `声望商城访问人数`
        from tmp.mall_new_user_cid_all_ctype_cname
        where d between '2020-07-01' and '2020-07-31'
    )action_data
        left join
    (
--月数据
-- 有车币商城分平台
        select concat('2020-07-01','~','2020-07-31') as `时间周期`
             ,'有车币商城' as `商城类型`
             ,case when ctype =1  then 'iOS' when ctype = 2 then 'And' when ctype=4 then '小程序' end as `平台`
             ,count(distinct case when order_status in (1,2,3)  then uid end) as `下单人数`
             ,count(distinct case when order_status in (1,2,3)  then order_no end) as `订单数`
             ,sum(case when order_status in (1,2,3)  then all_price end)/100 as `GMV`
             ,count(distinct case when is_first_order  =1 and order_status in (1,2,3) then uid else null end) as `首单用户数`
             ,count(distinct case when is_open_privilege_card  =1 and order_status in (1,2,3,5)  and paytime is not null then uid else null end) as `开卡用户数`
             ,sum(case when order_status in (1,2,3) then postage_price end)/100 as `邮费`
             ,sum(case when order_status in (1,2,3) then cost end)/100 as `成本`
        from order_tmp_30
        where  mall_type=1
        group by case when ctype =1  then 'iOS' when ctype = 2 then 'And' when ctype=4 then '小程序' end
        having `平台` is not null
        union all
--声望商城
        select concat('2020-07-01','~','2020-07-31') as `时间周期`
             ,'声望商城' as `商城类型`
             ,'总计' as `平台`
             ,count(distinct case when order_status in (1,2,3)  then uid end) as `下单人数`
             ,count(distinct case when order_status in (1,2,3)  then order_no end) as `订单数`
             ,sum(case when order_status in (1,2,3)  then all_price end)/100 as `GMV`
             ,count(distinct case when is_first_order  =1 and order_status in (1,2,3) then uid else null end) as `首单用户数`
             ,count(distinct case when is_open_privilege_card  =1 and order_status in (1,2,3,5)  and paytime is not null then uid else null end) as `开卡用户数`
             ,sum(case when order_status in (1,2,3) then postage_price end)/100 as `邮费`
             ,sum(case when order_status in (1,2,3) then cost end)/100 as `成本`
        from order_tmp_30
        where  mall_type=2
    )order_data
    on action_data.`时间周期`= order_data.`时间周期`;

