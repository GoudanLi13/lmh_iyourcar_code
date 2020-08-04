--查询周五到1周日有多少人点击

select get_json_object(args,'$.model_id'),count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-17' and '2020-07-19'
and id=316
group by get_json_object(args,'$.model_id');

select *
from
(select log_a.*,row_number() over (partition by log_a.cid,log_a.d,log_a.session order by log_a.st) as rank
from
(select cid,d,session,st,id,ctype,cname
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-17' and '2020-07-19'
) as log_a
join
(select cid,d,session,st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-17' and '2020-07-19'
and id=316
and get_json_object(args,'$.model_id')=0) as log
on log_a.session=log.session and log_a.cid=log.cid and log.d=log_a.d
where log.st<log_a.st) as a
where rank=1
;

--区分用户类型
--0.0广告用户
drop table if exists tmp.wx_yhyc_user;
create table if not exists tmp.wx_yhyc_user as
select unionid,substr(from_unixtime(cast(subscribe_time as int)),0,10) as d,subscribe
from iyourcar_dw.stage_all_service_day_iyourcar_wechat_user_wechat_user
where appid='wx29051ba13add54c6'
and substr(from_unixtime(cast(subscribe_time as int)),0,10) between '2020-07-30' and '2020-08-03'
and origin=1;



select *
from tmp.wx_yhyc_user;

--0.新增广告用户总数
select d,count(unionid),sum(subscribe)
from tmp.wx_yhyc_user
group by d;


--1.有从优惠养车工作号进来的广告用户
select visit.d,count(distinct visit.cid)
from
tmp.wx_yhyc_user as wx
join iyourcar_dw.dws_prop_day_user as users
    on wx.unionid=users.wechat_unionid
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.uid=users.uid
join
(select cid,d
from iyourcar_dw.dwd_all_action_hour_log
where d ='2020-08-03'
and id=316
and ( get_json_object(args,'$.model_id') in(
    101,102) or split(get_json_object(args,'$.wx_page'),'_')[0]=4)) as visit
on visit.cid=maps.cid
group by visit.d;


--查询8月2日进入小程序的人是什么时候关注的
select wx.*
from
(
    select unionid,substr(from_unixtime(cast(subscribe_time as int)),0,10) as d
from iyourcar_dw.stage_all_service_day_iyourcar_wechat_user_wechat_user
where appid='wx29051ba13add54c6'
    ) as wx
join iyourcar_dw.dws_prop_day_user as users
    on wx.unionid=users.wechat_unionid
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.uid=users.uid
join
(select distinct cid,d
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-08-02' and '2020-08-02'
and id=316
and ( get_json_object(args,'$.model_id') in(
    '1#2',
'1#3',
'1#4',
'1#5',
'1#6',
'1#7',
'1#8') or split(get_json_object(args,'$.wx_page'),'_')[2]=4)) as visit
on visit.cid=maps.cid;


--2.从公众号进来的广告用户中的新用户
select visit.d,count(distinct visit.cid)
from
tmp.wx_yhyc_user as wx
join iyourcar_dw.dws_prop_day_user as users
    on wx.unionid=users.wechat_unionid
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.uid=users.uid
join
(select cid,d
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-30' and '2020-08-02'
and id=316
and ( get_json_object(args,'$.model_id') in(
    '1#2',
'1#3',
'1#4',
'1#5',
'1#6',
'1#7',
'1#8') or split(get_json_object(args,'$.wx_page'),'_')[0]=4)) as visit
on visit.cid=maps.cid
join (select * from tmp.mall_new_user_cid_all_ctype_cname where d between '2020-07-30' and '2020-08-02' and cname='WXAPP_YCYH_PLUS')as new_user
on visit.cid=new_user.cid
group by visit.d;

--总体带来的GMV
select sum(all_price)
from
(select unionid
from iyourcar_dw.stage_all_service_day_iyourcar_wechat_user_wechat_user
where appid='wx29051ba13add54c6'
and substr(from_unixtime(cast(subscribe_time as int)),0,10) between '2020-07-30' and '2020-07-30'
and origin=1) as wx
join iyourcar_dw.dws_prop_day_user as users
    on wx.unionid=users.wechat_unionid
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.uid=users.uid
join
(select distinct cid,d
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-30' and '2020-07-30'
and id=316
and ( get_json_object(args,'$.model_id') in(
    '1#2',
'1#3',
'1#4',
'1#5',
'1#6',
'1#7',
'1#8') or split(get_json_object(args,'$.wx_page'),'_')[2]=4)) as visit
on visit.cid=maps.cid
join ( select substr(ordertime,0,10) as d,uid,order_no,all_price
      from
    iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-30' and '2020-07-30'
    and biz_type in(1,3)
    and all_price>0
    and order_status in(1,2,3)
    and mall_type=1
    and ctype=4
    )as orders
on orders.uid=maps.uid and orders.d=visit.d;

--新用户带来的GMV
select sum(all_price)
from
(select unionid
from iyourcar_dw.stage_all_service_day_iyourcar_wechat_user_wechat_user
where appid='wx29051ba13add54c6'
and substr(from_unixtime(cast(subscribe_time as int)),0,10) between '2020-07-30' and '2020-07-30'
and origin=1) as wx
join iyourcar_dw.dws_prop_day_user as users
    on wx.unionid=users.wechat_unionid
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.uid=users.uid
join
(select distinct cid,d
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-30' and '2020-07-30'
and id=316
and ( get_json_object(args,'$.model_id') in(
    '1#2',
'1#3',
'1#4',
'1#5',
'1#6',
'1#7',
'1#8') or split(get_json_object(args,'$.wx_page'),'_')[2]=4)) as visit
on visit.cid=maps.cid
join (select * from tmp.mall_new_user_cid_all_ctype_cname where d between '2020-07-30' and '2020-07-30' and cname='WXAPP_YCYH_PLUS')as new_user
on visit.cid=new_user.cid
join ( select substr(ordertime,0,10) as d,uid,order_no,all_price
      from
    iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) between '2020-07-30' and '2020-07-30'
    and biz_type in(1,3)
    and all_price>0
    and order_status in(1,2,3)
    and mall_type=1
    and ctype=4
    )as orders
on orders.uid=maps.uid and orders.d=visit.d;

select *
from
(select unionid
from iyourcar_dw.stage_all_service_day_iyourcar_wechat_user_wechat_user
where appid='wx29051ba13add54c6'
and substr(from_unixtime(cast(subscribe_time as int)),0,10) between '2020-07-30' and '2020-07-30'
and origin=1) as wx
join iyourcar_dw.dws_prop_day_user as users
    on wx.unionid=users.wechat_unionid;

--aiflow汇总
with wx_user_uid as
    (
        select wx.d,users.uid,maps.cid
        from
        tmp.wx_yhyc_user as wx
join iyourcar_dw.dws_prop_day_user as users
    on wx.unionid=users.wechat_unionid
join iyourcar_dw.dws_extend_day_cid_map_uid as maps
on maps.uid=users.uid
    )

select a.*,b.`累计黑金卡实体卡人数`
from
(select
       visit.d,
       count(distinct visit.cid) as `当日进入商城用户数`,
       count(distinct orders.uid) as `当日消费用户数`,
       if(sum(all_price) is null,0,sum(all_price)/100) as `总GMV`,
       count(distinct new_user.cid) as `当日进入商城人数（新用户）`,
       count(distinct case when new_user.cid is not null then orders.uid end) as `当日消费用户数（新用户）`,
       if(sum(case when new_user.cid is not null then all_price end) is null,0,sum(case when new_user.cid is not null then all_price end)/100) as `新用户GMV`
from
wx_user_uid
join
(select distinct cid,d
from iyourcar_dw.dwd_all_action_hour_log
where d ='2020-08-03'
and id=316
and ( get_json_object(args,'$.model_id') in(
    101,102
    ) or split(get_json_object(args,'$.wx_page'),'_')[0]=4)) as visit
on visit.cid=wx_user_uid.cid
left join (select * from tmp.mall_new_user_cid_all_ctype_cname where d ='2020-08-03' and cname='WXAPP_YCYH_PLUS')as new_user
on visit.cid=new_user.cid
left join ( select uid,order_no,all_price
      from
    iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
    where substr(ordertime,0,10) ='2020-08-03'
    and biz_type in(1,3)
    and all_price>0
    and order_status in(1,2,3)
    and mall_type=1
    and ctype=4
    )as orders
on orders.uid=wx_user_uid.uid
group by visit.d) as a
join
(
    select '2020-08-03' as d,count(vcard.uid) as `累计黑金卡实体卡人数`
    from wx_user_uid
    join iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_user as vcard
    on wx_user_uid.uid=vcard.uid
    where member_type=1
    and substr(member_time,0,10)>=wx_user_uid.d
    and is_member!=0
    ) as b
on a.d=b.d
;

select distinct cid,d
from iyourcar_dw.dwd_all_action_hour_log
where d ='2020-08-03'
and id=316
and ( get_json_object(args,'$.model_id') in(
    101) );