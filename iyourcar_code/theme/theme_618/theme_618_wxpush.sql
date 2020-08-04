select sum(all_price)/100 from

     (
         select distinct goods_detail.uid,goods_detail.d from
         (
        select distinct uid,cid,session,d from iyourcar_dw.dwd_all_action_hour_log
        where d>='2020-06-01' and d<='2020-06-21' and id=302 and get_json_object(args,'$.spu') is not null
     ) as goods_detail
    inner join
    (
        select distinct cid,session,d from iyourcar_dw.dwd_all_action_hour_log
        where d >= '2020-06-01' and d<='2020-06-21' and id=316 and get_json_object(args,'$.wx_page') like '_\_%'
    ) as from_wx
    on from_wx.session = goods_detail.session and from_wx.cid = goods_detail.cid and from_wx.d = goods_detail.d) as from_wx_user
    inner join
         (select  uid,substr(ordertime,0,10) as d,all_price from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
             where substr(ordertime,0,10) >= '2020-06-01' and substr(ordertime,0,10)<='2020-06-21'
             and order_status in (1,2,3) and biz_type in (1,3)
             ) as orders
    on orders.uid = from_wx_user.uid and orders.d = from_wx_user.d