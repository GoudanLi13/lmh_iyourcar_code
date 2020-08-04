--安卓4.26的使用人数
select d,c_ver,count(distinct cid) as `最新版的使用人数`
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-06-23' and '2020-07-01'  and ctype=2 and cname='APP_SUV'
group by d,c_ver;

--过去7天购物的人中有多少人在1个月内用过ios和安卓(c_ver>423000)
select count(order_user.uid),count(app_user.uid)
from
    (
        select distinct uid
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime, 0, 10) between '2020-06-19' and '2020-06-25'
          and order_status in (1, 2, 3)
          and order_type in (2, 3, 4)
    ) as order_user
left join
    (
        select distinct uid
        from iyourcar_dw.dwd_all_action_hour_log
        where d between '2020-06-01' and '2020-06-25'
        and cname='APP_SUV'
        and ((ctype=1) or (ctype=2 and c_ver>=423000))
        ) as app_user
on order_user.uid=app_user.uid;

--按过去几天有多少人点击ios的商城push
select d,count(distinct cid) from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-06-24' and '2020-06-27' and id=11762 and get_json_object(args,'$.push_id')=638485
group by d;

--建表
create table if not exists tmp.lmh_mall_push_user_list
(
    uid string comment '用户id',
    order_d string comment '购物日期',
    push_d string comment '推送发送日期',
    is_first_order smallint comment '是否首单用户',
    push_id bigint comment '推送id',
    spu_id bigint comment '推送的商品id'
)
row format delimited
fields terminated by ',';

select * from tmp.rpt_official_comparative_analysis;
select * from tmp.rpt_official_comparative_analysis_card;

select * from tmp.lmh_mall_push_user_list;

--uid提取sql
select *,row_number() over(partition by d,is_first_order order by uid) as ranks
from (select uid, d, is_first_order, row_number() over (partition by uid order by d desc,is_first_order desc) as num
      from (select DISTINCT uid, d, is_first_order
            from (select uid, substr(ordertime, 0, 10) as d, is_first_order, order_no
                  from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
                  where (
                          substr(ordertime, 0, 10) between '2020-06-10' and '2020-06-16'
                          or
                          substr(ordertime, 0, 10) between '2020-06-24' and '2020-06-30'
                      )
                    and all_price > 0
                    and order_status in (1, 2, 3)) as uid_list
                     left join
                 (
                     select distinct order_no
                     from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order_item
                     where item_id in (1285, 390)
                 ) as item
                 on uid_list.order_no = item.order_no
            where item.order_no is null) as a
    ) as b
where num = 1
;

select name from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_item_info where id=1285;

--查看本次发送的人里面多少人访问过ios app或安卓426(push发送了给多少人)
drop table tmp.lmh_mall_push_list_active;
create table if not exists tmp.lmh_mall_push_list_active as
select push_uid.*
from tmp.lmh_mall_push_user_list as push_uid
join
    (
        select distinct uid
        from iyourcar_dw.dwd_all_action_hour_log
        where d between '2020-06-01' and '2020-07-01' and cname='APP_SUV' and
        ((ctype=2 and c_ver=426000) or ctype=1)
        ) as log
on log.uid=push_uid.uid;


        select log.ctype,count(distinct log.uid) from
        tmp.lmh_mall_push_list_active as push
        join
        (select distinct uid,ctype
        from iyourcar_dw.dwd_all_action_hour_log
        where d between '2020-06-01' and '2020-07-01' and cname='APP_SUV' and
        ((ctype=2 and c_ver=426000) or ctype=1)) as log
        on push.uid=log.uid
        group by log.ctype;



select count(*) from tmp.lmh_mall_push_list_active;

--发送的人有多少在当天点击了push\购买了商品(按天数来分）
select
log.push_id,push_user.order_d,push_user.is_first_order,
count(push_user.uid) as `点击人数`,
sum(orders.gmv) as `支付金额`
from tmp.lmh_mall_push_list_active as push_user
join
(
    select cid,uid,get_json_object(args,'$.redirect_target') as push_id
    from iyourcar_dw.dwd_all_action_hour_log
    where d='2020-07-01'
    and ctype in(1,2)
    and cname='APP_SUV'
    and id in(11762,12486)
    and get_json_object(args,'$.redirect_target') in(648452,648453)
    ) as log
on push_user.uid=log.uid
left join
    (
        select uid,sum(all_price)/100 as gmv
        from iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
        where substr(ordertime,0,10)='2020-07-01'
        and order_status in(1,2,3)
        and all_price>0
        group by uid
        ) as orders
on orders.uid=push_user.uid
group by log.push_id,push_user.order_d,push_user.is_first_order;

select cid,uid,args
from iyourcar_dw.dwd_all_action_hour_log
where d='2020-07-01' and h between 20 and 23 and id in(11762);


select *
from iyourcar_dw.stage_all_service_day_iyourcar_mall_item_mall_group_info;

--每天多少新用户从微信公众号进来

select log.d,sum(all_price/100)
from
iyourcar_dw.dws_extend_day_cid_map_uid as maps
join
(select distinct d,cid
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-08' and '2020-07-14'
and cname='WXAPP_YCYH_PLUS'
and ctype=4
and id=316
and get_json_object(args,'$.scene_id')=1035) as log
on maps.cid=log.cid
join iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on maps.uid=orders.uid and ctype=4 and mall_type=1 and substr(ordertime,0,10)=log.d
group by log.d;


select * from tmp.mall_new_user_cid_all_ctype_cname where d='2020-07-13';

--每天有多少人通过砖叔优选进来
select d,count(distinct cid)
from
(select wx.*,id,row_number() over (partition by wx.d,wx.session,wx.cid order by first_page.st) as rank
from
(select d,cid,session,st
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-08' and '2020-07-14'
and cname='WXAPP_YCYH_PLUS'
and ctype=4
and id=316
and get_json_object(args,'$.scene_id')=1035) as wx
join
(
select d,cid,session,st,id
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-08' and '2020-07-14'
and cname='WXAPP_YCYH_PLUS'
and ctype=4
and id=293
and a='p'
) as first_page
on wx.d=first_page.d and wx.session=first_page.session and wx.cid=first_page.cid
where first_page.st>wx.st) as a
where rank=1
group by d
;

--每天有多少人从公众号进来且有商城行为
select wx.d,count(distinct wx.cid)
from
(select distinct d,cid
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-08' and '2020-07-14'
and cname='WXAPP_YCYH_PLUS'
and ctype=4
and id=316
and get_json_object(args,'$.scene_id')=1035) as wx
join
(
    select distinct d,cid
    from (select cid,id,d from iyourcar_dw.dwd_all_action_hour_log
    where d between '2020-07-08' and '2020-07-14'
    and cname='WXAPP_YCYH_PLUS'
    and ctype=4
          )as log
join
     (select event_id
     from iyourcar_dw.dwd_all_action_day_event_group
      where event_group_id = 20
     ) as visit_mall
on log.id=visit_mall.event_id
    ) as mall
on wx.cid=mall.cid and wx.d=mall.d
group by wx.d;

--
select count(origin)
from iyourcar_dw.stage_all_service_day_iyourcar_wechat_user_wechat_user
where appid='wx29051ba13add54c6' and subscribe=1;

select *
from iyourcar_dw.dwd_all_action_hour_log
where d between '2020-07-09' and '2020-07-14'
and id=12579;

--7月29日有多少人点击了有商品关联的帖子
select id,count(distinct cid)
from iyourcar_dw.dwd_all_action_hour_log
where d='2020-07-27'
and id in(11762,12486)
and get_json_object(args,'$.push_id')=5236397
group by id;

