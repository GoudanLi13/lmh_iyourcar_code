select `日期`,`具体的券`,`满多少金额`,`点击商城首页领劵的人数`,`领取总量`,`领取的人数`,`使用总量`,`使用的人数`,`用券的订单的最终付款金额均值`
from (
    select coupon_user.get_d as `日期`,
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
            where substr(createtime,0,10) between '2020-06-16' and '2020-06-18'
        ) as coupon_user
        inner join
        (
            select *
            from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_coupon_info
            where status = 1
        ) as coupon_info
        on coupon_info.id = coupon_user.coupon_id
        inner join
        (
            select d,count(distinct uid) as coupon_click_uv,info.name
            from iyourcar_dw.dwd_all_action_hour_log
            inner join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_coupon_info as info
            on  split(get_json_object(args,"$.redirect_target"),'/')[5] = info.id
            where dwd_all_action_hour_log.id in ('11339','11828','11340') and d between '2020-06-16' and '2020-06-18'
            and get_json_object(args,"$.redirect_target") like  "%coupon%"
            group by d,info.name
        ) as coupon_click
        on coupon_click.name = coupon_info.name and coupon_user.get_d = coupon_click.d
        left join
             ( select *,substr(ordertime,0,10) as d
                from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
               where substr(ordertime,0,10) between '2020-06-16' and '2020-06-18' and biz_type in (1,3) and order_status in (1,2,3) and coupon_no is not null
              ) as orders
        on orders.uid = coupon_user.uid and orders.d = coupon_user.use_d
    group by  coupon_user.get_d,coupon_info.name , coupon_info.require_amount/100,coupon_click.coupon_click_uv
    ) as final_result
order by  `日期`,`具体的券`;


select `具体的券`,`满多少金额`,`看到券的人数`,`领取总量`,`领取的人数`,`使用总量`,`使用的人数`,`用券的订单的最终付款金额均值`
from (
    select
           coupon_info.name as `具体的券`,
           coupon_info.require_amount/100 as `满多少金额`,
           coupon_click.coupon_click_uv as `看到券的人数`,
           count( coupon_user.uid) as `领取总量`,
           count(distinct coupon_user.uid) as `领取的人数`,
           sum( case when coupon_user.use_time is not null then 1 else 0 end) as `使用总量`,
           count(distinct case when coupon_user.use_time is not null then coupon_user.uid end) as `使用的人数`,
           if(isnull(avg(all_price/100)),0,avg(all_price/100)) as `用券的订单的最终付款金额均值`
          from
               -- 优惠券使用用户
        (select uid,coupon_id,status,substr(createtime,0,10) as get_d,substr(use_time,0,10) as use_d,use_time,is_view
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_coupon_user
        where substr(createtime,0,10) between '2020-06-05' and '2020-06-08') as coupon_user
        inner join
              (select * from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_coupon_info where status = 1) as coupon_info
                on coupon_info.id = coupon_user.coupon_id
            inner join
            (select count(distinct uid) as coupon_click_uv,info.name from iyourcar_dw.dwd_all_action_hour_log
  inner join iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_coupon_info as info
      on  split(get_json_object(args,"$.redirect_target"),'/')[5] = info.id
    where dwd_all_action_hour_log.id in ('11339','11828','11340') and d between '2020-06-05' and '2020-06-08'
        and get_json_object(args,"$.redirect_target") like  "%coupon%"
        group by info.name
        ) as coupon_click
            on coupon_click.name = coupon_info.name
    left join
             ( select *,substr(ordertime,0,10) as d from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
              where substr(ordertime,0,10) between '2020-06-05' and '2020-06-08' and biz_type in (1,3) and order_status in (1,2,3) and coupon_no is not null
              ) as orders   on orders.uid = coupon_user.uid
    group by  coupon_info.name ,
           coupon_info.require_amount/100,coupon_click.coupon_click_uv
         ) as final_result
order by  `具体的券`;

select from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order