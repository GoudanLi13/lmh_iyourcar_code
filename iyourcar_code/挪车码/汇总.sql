-- 累计激活人数
-- 整体
select count(distinct uid) as `总激活人数`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code
where active_time is not null and substr(active_time,0,10) between '2019-11-15' and '2020-05-31';

-- 输出第一批挪车码激活情况数据
select count(distinct uid) as `第一批激活人数`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code
where uuid not like "1116%" and uuid not like "0217%" and active_time is not null and substr(active_time,0,10) between '2020-02-09' and '2020-05-31';

-- 输出1115-0209第一批挪车码激活情况数据
select count(distinct uid) as `1115-0209第一、二批激活人数`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code
where uuid not like "0217%"
and active_time is not null and substr(active_time,0,10) between '2019-11-15' and '2020-02-08';

-- 输出第二批挪车码激活情况数据
select count(distinct uid) as `第二批激活人数`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code
where uuid like "1116%" and active_time is not null and substr(active_time,0,10) between '2020-02-09' and '2020-05-31';

-- 输出第三批挪车码激活情况数据
select count(distinct uid) as `第三批激活人数`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code
where uuid like "0217%" and active_time is not null and substr(active_time,0,10) between '2020-03-01' and '2020-05-31';


-- 输出挪车码激活-APP新用户数据
select count(distinct pcode.uid) as `总激活人数APP新用户` from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2019-11-15'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-05-31';


-- 输出1115-0209第一、二批挪车码激活情况数据
select count(distinct pcode.uid) as `1115-0209第一、二批激活人数APP新用户`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d between '2019-11-15' and '2020-02-08'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-02-08'
and uuid not like "0217%";


--第一批
-- 输出挪车码激活-APP新用户数据
select count(distinct pcode.uid) as `第一批-激活人数-APP新用户`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2020-02-09'
and pcode.uuid not like "1116%" and pcode.uuid not like "0217%"
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-05-31';
--第二批
-- 输出挪车码激活-APP新用户数据
select count(distinct pcode.uid) as `第二批-激活人数-APP新用户` from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2020-02-09'
and pcode.uuid  like "1116%"
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-05-31';
--第三批
-- 输出挪车码激活-APP新用户数据
select count(distinct pcode.uid) as `第三批-激活人数-APP新用户` from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2020-03-01'
and pcode.uuid  like "0217%"
and substr(pcode.active_time,0,10) between '2020-03-01' and '2020-05-31';

-- 输出挪车码激活-小程序新用户数据
select count(distinct pcode.uid) as `总激活人数-小程序新用户` from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_wxapp_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2019-11-15'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-05-31';

-- 输出1115-0209第一、二批挪车码激活情况数据
select count(distinct pcode.uid) as `1115-0209第一、二批激活人数小程序新用户`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_wxapp_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d between '2019-11-15' and '2020-02-08'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-02-08'
and uuid not like "0217%"

--第一批
-- 输出挪车码激活-小程序新用户数据
select count(distinct pcode.uid) as `第一批激活人数-小程序新用户` from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_wxapp_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2020-02-09'
and pcode.uuid not like "1116%" and pcode.uuid not like "0217%"
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-05-31'


--第二批
-- 输出挪车码激活-小程序新用户数据
select count(distinct pcode.uid) as `第二批-激活人数-小程序新用户` from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_wxapp_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2020-02-09'
and pcode.uuid  like "1116%"
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-05-31'

--第三批
-- 输出挪车码激活-小程序新用户数据
select count(distinct pcode.uid) as `第三批-激活人数-小程序新用户` from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_wxapp_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2020-03-01'
and pcode.uuid  like "0217%"
and substr(pcode.active_time,0,10) between '2020-03-01' and '2020-05-31'



select count(distinct pcode.uid) as `总小程序新用户-下单人数统计`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_wxapp_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2019-11-15'
and orders.order_status in (1,2,3)
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2019-11-15'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-05-31'


select count(distinct pcode.uid) as `总-APP新用户-下单人数统计`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2019-11-15'
and orders.order_status in (1,2,3)
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2019-11-15'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-05-31'




--GMV

-- 输出挪车码激活-所有用户数据
select sum(all_price)/100 as `all_GMV`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and substr(pcode.active_time,0,10) <= '2020-05-31'
and orders.order_status in (1,2,3)
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2019-11-15'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-05-31'


select count(distinct pcode.uid) as `all_GMV_uid_count`   from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and substr(pcode.active_time,0,10) <= '2020-05-31'
and orders.order_status in (1,2,3)
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2019-11-15'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-05-31'


-- 输出1115-0209挪车码激活人数GMV数据
select sum(all_price)/100 as `mixed_all_GMV`  from
(select * from
iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code
where active_time is not null and substr(active_time,0,10) between '2019-11-15' and '2020-02-08'
)
as pcode
inner join
(select * from
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) <= '2020-05-31' and substr(ordertime,0,10) >= '2019-11-15'
and order_status in (1,2,3)
)as orders
on pcode.uid = orders.uid

select count(distinct pcode.uid) as `mixed_all_GMV_uid_count`  from
(select * from
iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code
where active_time is not null and substr(active_time,0,10) between '2019-11-15' and '2020-02-08'
)
as pcode
inner join
(select * from
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order
where substr(ordertime,0,10) <= '2020-05-31' and substr(ordertime,0,10) >= '2019-11-15'
and order_status in (1,2,3)
)as orders
on pcode.uid = orders.uid

--第一批
-- 输出挪车码激活-所有用户数据
select sum(all_price)/100 as `1_all_GMV`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null
and orders.order_status in (1,2,3) and pcode.uuid not like "1116%" and pcode.uuid not like "0217%"
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2020-02-09'
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-05-31'


select count(distinct pcode.uid) as `1_all_GMV_uid_count`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null
and orders.order_status in (1,2,3) and pcode.uuid not like "1116%" and pcode.uuid not like "0217%"
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2020-02-09'
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-05-31'



--第二批
-- 输出挪车码激活-所有用户数据
select sum(all_price)/100 as `2_all_GMV`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null
and orders.order_status in (1,2,3)
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2020-02-09'
and pcode.uuid  like "1116%"
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-05-31'




--第三批
-- 输出挪车码激活-所有用户数据
select sum(all_price)/100 as `3_all_GMV`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null
and orders.order_status in (1,2,3) and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2020-03-01'
and pcode.uuid  like "0217%"
and substr(pcode.active_time,0,10) between '2020-03-01' and '2020-05-31'






--APP新用户
--GMV
-- 输出挪车码GMV-APP新用户数据
select sum(all_price)/100 as `all_new_GMV`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2019-11-15'
and orders.order_status in (1,2,3)
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2019-11-15'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-05-31';


--人数
select count(distinct pcode.uid)  as `all_new_GMV_uid_count`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2019-11-15'
and orders.order_status in (1,2,3)
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2019-11-15'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-05-31';

-- 输出挪车码GMV-app新用户数据

select sum(all_price)/100 as `1115-0208_all_new_GMV`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-02-08' and app_active.d >='2019-11-15'
and orders.order_status in (1,2,3)
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2019-11-15'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-02-08';


select count(distinct pcode.uid) as `1115-0208_all_new_GMV_uid_count`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-02-08' and app_active.d >='2019-11-15'
and orders.order_status in (1,2,3)
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2019-11-15'
and substr(pcode.active_time,0,10) between '2019-11-15' and '2020-02-08';


--第一批
-- 输出挪车码GMV-APP新用户数据
select sum(all_price)/100 as `1_new_GMV`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2020-02-09'
and orders.order_status in (1,2,3) and pcode.uuid not like "1116%" and pcode.uuid not like "0217%"
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2020-02-09'
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-05-31';


select count(distinct pcode.uid) as `1_new_GMV_uid_count`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2020-02-09'
and orders.order_status in (1,2,3) and pcode.uuid not like "1116%" and pcode.uuid not like "0217%"
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2020-02-09'
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-05-31';



--第二批
-- 输出挪车码GMV-APP新用户数据



select sum(all_price)/100 as `2_new_GMV`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
inner join
iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
on pcode.uid = orders.uid
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-05-31' and app_active.d >='2020-02-09'
and orders.order_status in (1,2,3) and pcode.uuid  like "1116%"
and  substr(orders.ordertime,0,10) <= '2020-05-31' and substr(orders.ordertime,0,10) >= '2020-02-09'
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-05-31';


----跑到了这里！！！！！！
select count(distinct pcode.uid) as `2_new_GMV_uid_count`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_app_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2020-02-09'
  and orders.order_status in (1, 2, 3)
  and pcode.uuid like "1116%"
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2020-02-09'
  and substr(pcode.active_time, 0, 10) between '2020-02-09' and '2020-06-30';


--第三批
-- 输出挪车码GMV-APP新用户数据
select sum(all_price) / 100 as `3_new_GMV`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_app_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2020-03-01'
  and orders.order_status in (1, 2, 3)
  and pcode.uuid like "0217%"
  and orders.order_status in (1, 2, 3)
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2020-03-01'
  and substr(pcode.active_time, 0, 10) between '2020-03-01' and '2020-06-30';


select count(distinct pcode.uid) as `3_new_GMV_uid_count`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_app_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2020-03-01'
  and orders.order_status in (1, 2, 3)
  and pcode.uuid like "0217%"
  and orders.order_status in (1, 2, 3)
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2020-03-01'
  and substr(pcode.active_time, 0, 10) between '2020-03-01' and '2020-06-30';



--小程序新用户
--GMV
-- 输出挪车码GMV-小程序新用户数据
select sum(all_price) / 100 as `all_new_GMV_wxapp`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_wxapp_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2019-11-15'
  and orders.order_status in (1, 2, 3)
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2019-11-15'
  and substr(pcode.active_time, 0, 10) between '2019-11-15' and '2020-06-30';


select sum(all_price) / 100 as `all_new_GMV_wxapp`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_wxapp_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2019-11-15'
  and orders.order_status in (1, 2, 3)
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2019-11-15'
  and substr(pcode.active_time, 0, 10) between '2019-11-15' and '2020-06-30';


--小程序新用户
--GMV
-- 输出挪车码GMV-小程序新用户数据
select sum(all_price) / 100 as `0115-0208_all_new_GMV_wxapp`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_wxapp_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-02-08'
  and app_active.d >= '2019-11-15'
  and orders.order_status in (1, 2, 3)
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2019-11-15'
  and substr(pcode.active_time, 0, 10) between '2019-11-15' and '2020-02-08';

select count(distinct pcode.uid) as `0115-0208_all_new_GMV_wxapp_uid_count`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_wxapp_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-02-08'
  and app_active.d >= '2019-11-15'
  and orders.order_status in (1, 2, 3)
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2019-11-15'
  and substr(pcode.active_time, 0, 10) between '2019-11-15' and '2020-02-08';
--第一批
-- 输出挪车码GMV-小程序新用户数据
select sum(all_price) / 100 as `1_new_GMV_wxapp`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_wxapp_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2020-02-09'
  and orders.order_status in (1, 2, 3)
  and pcode.uuid not like "1116%"
  and pcode.uuid not like "0217%"
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2020-02-09'
  and substr(pcode.active_time, 0, 10) between '2020-02-09' and '2020-06-30';

select count(distinct pcode.uid) as `1_new_GMV_wxapp_uid_count`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_wxapp_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2020-02-09'
  and orders.order_status in (1, 2, 3)
  and pcode.uuid not like "1116%"
  and pcode.uuid not like "0217%"
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2020-02-09'
  and substr(pcode.active_time, 0, 10) between '2020-02-09' and '2020-06-30';

--第二批
-- 输出挪车码GMV-小程序新用户数据
select sum(all_price) / 100 as `2_new_GMV_wxapp`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_wxapp_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2020-02-09'
  and orders.order_status in (1, 2, 3)
  and pcode.uuid like "1116%"
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2020-02-09'
  and substr(pcode.active_time, 0, 10) between '2020-02-09' and '2020-06-30';

select count(distinct pcode.uid) as `2_new_GMV_wxapp_uid_count`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_wxapp_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2020-02-09'
  and orders.order_status in (1, 2, 3)
  and pcode.uuid like "1116%"
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2020-02-09'
  and substr(pcode.active_time, 0, 10) between '2020-02-09' and '2020-06-30';
--第三批
-- 输出挪车码GMV-小程序新用户数据
select sum(all_price) / 100 as `3_new_GMV_wxapp`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_wxapp_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2020-03-01'
  and orders.order_status in (1, 2, 3)
  and pcode.uuid like "0217%"
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2020-03-01'
  and substr(pcode.active_time, 0, 10) between '2020-03-01' and '2020-06-30';

select count(distinct pcode.uid) as `3_new_GMV_wxapp_uid_count`
from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
         inner join
     tmp.dwd_ycyh_user_day_wxapp_active as app_active
     on pcode.uid = app_active.uid and substr(pcode.active_time, 0, 10) = app_active.d
         inner join
     iyourcar_dw.stage_all_service_day_iyourcar_mall_order_mall_score_order as orders
     on pcode.uid = orders.uid
where pcode.active_time is not null
  and app_active.action_status = 1
  and app_active.d <= '2020-06-30'
  and app_active.d >= '2020-03-01'
  and orders.order_status in (1, 2, 3)
  and pcode.uuid like "0217%"
  and substr(orders.ordertime, 0, 10) <= '2020-06-30'
  and substr(orders.ordertime, 0, 10) >= '2020-03-01'
  and substr(pcode.active_time, 0, 10) between '2020-03-01' and '2020-06-30';