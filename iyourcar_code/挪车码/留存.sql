--第一批
-- 输出挪车码激活-APP新用户数据
select count(distinct pcode.uid) as `第一批-激活人数-APP新用户`  from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-06-30' and app_active.d >='2020-02-09'
and pcode.uuid not like "1116%" and pcode.uuid not like "0217%"
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-06-30';

--第二批
-- 输出挪车码激活-APP新用户数据
select count(distinct pcode.uid) as `第二批-激活人数-APP新用户` from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-06-30' and app_active.d >='2020-02-09'
and pcode.uuid  like "1116%"
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-06-30';
--第三批
-- 输出挪车码激活-APP新用户数据
select count(distinct pcode.uid) as `第三批-激活人数-APP新用户` from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-06-30' and app_active.d >='2020-03-01'
and pcode.uuid  like "0217%"
and substr(pcode.active_time,0,10) between '2020-03-01' and '2020-06-30';


-- 第一批
with app_visit as (
select distinct app_active.uid,app_active.d from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-06-30' and app_active.d >='2020-02-09'
and pcode.uuid not like "1116%" and pcode.uuid not like "0217%"
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-06-30'
),
app_active_old_user as (
select * from tmp.dwd_ycyh_user_day_app_active where d >= '2020-02-09'
)

select app_visit.d,count(uid) as count  ,'当天访问'  from
--日活跃用户表（记录第一天登录）
app_visit

group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'一日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 1
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'二日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 2
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'三日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 3
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'五日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 5
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'七日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 7
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'十五日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 15
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'三十日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 30
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'六十日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 60
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
;

-- 第二批
with app_visit as (
select distinct app_active.uid,app_active.d from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-06-30' and app_active.d >='2020-02-09'
and pcode.uuid  like "1116%"
and substr(pcode.active_time,0,10) between '2020-02-09' and '2020-06-30'
),
app_active_old_user as (
select * from tmp.dwd_ycyh_user_day_app_active where action_status = 2 and d >= '2020-02-09'
)

select app_visit.d,count(uid) as count  ,'当天访问'  from
--日活跃用户表（记录第一天登录）
app_visit

group by app_visit.d
union all

select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'一日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join

(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 1
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'二日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 2
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'三日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 3
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'五日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 5
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'七日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 7
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'十五日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 15
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'三十日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 30
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'六十日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 60
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
;

-- 第三批
with app_visit as (
select distinct app_active.uid,app_active.d from iyourcar_dw.stage_all_service_day_iyourcar_activity_privilege_park_code as pcode
inner join
tmp.dwd_ycyh_user_day_app_active  as app_active
on pcode.uid = app_active.uid and substr(pcode.active_time,0,10) = app_active.d
where pcode.active_time is not null and app_active.action_status = 1 and app_active.d <='2020-06-30' and app_active.d >='2020-03-01'
and pcode.uuid  like "0217%"
and substr(pcode.active_time,0,10) between '2020-03-01' and '2020-06-30'
),
app_active_old_user as (
select * from tmp.dwd_ycyh_user_day_app_active where  d >= '2020-03-01'
)

select app_visit.d,count(uid) as count  ,'当天访问'  from
--日活跃用户表（记录第一天登录）
app_visit


group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'一日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 1
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'二日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 2
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'三日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 3
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'五日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 5
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'七日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 7
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'十五日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 15
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'三十日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 30
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d
union all
select app_visit.d,sum(if(isnull(c.uid),0,1)) as count  ,'六十日留'  from
--日活跃用户表（记录第一天登录）
app_visit
left join
(select distinct a.uid,a.d from app_visit as a
inner join
--日活跃用户表（记录第61天登录）
app_active_old_user as b
on a.uid = b.uid where datediff(b.d,a.d) = 60
) as c
on app_visit.uid = c.uid and app_visit.d = c.d
group by app_visit.d;