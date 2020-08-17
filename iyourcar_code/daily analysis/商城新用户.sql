with t_user as
(select distinct ctype,cid,cname from iyourcar_dw.dwd_all_action_hour_log log
join iyourcar_dw.dwd_all_action_day_event_group event_group
on log.id=event_group.event_id
where log.d = '2020-02-29' and event_group.event_group_id=20
)
-- 历史用户
, t_user_his as
(select distinct ctype,cid,cname from iyourcar_dw.dwd_all_action_hour_log log
join iyourcar_dw.dwd_all_action_day_event_group event_group
on log.id=event_group.event_id
where log.d>=date_sub('2020-02-29',180) and log.d<'2020-02-29' and event_group.event_group_id=20
)

insert overwrite table tmp.mall_new_user_cid_all_ctype_cname partition (d = '2020-02-29')
select t_user.cid,t_user.ctype,t_user.cname from t_user left join t_user_his
on t_user.ctype=t_user_his.ctype and t_user.cid=t_user_his.cid and t_user.cname=t_user_his.cname
where t_user_his.cid is null ;

select d,count(*)
from tmp.mall_new_user_cid_all_ctype_cname
group by d;

---动态分区

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;


create table tmp.lmh_tmp_new_users_0817 as
    with t_user as
(select distinct ctype,cid,cname,d from iyourcar_dw.dwd_all_action_hour_log log
join iyourcar_dw.dwd_all_action_day_event_group event_group
on log.id=event_group.event_id
where log.d between '2020-02-29' and '2020-02-29' and event_group.event_group_id=20
)
-- 历史用户
   , t_user_his as
(select distinct ctype,cid,cname,d from iyourcar_dw.dwd_all_action_hour_log log
join iyourcar_dw.dwd_all_action_day_event_group event_group
on log.id=event_group.event_id
where log.d>=date_sub('2020-02-29',180) and log.d<'2020-02-29' and event_group.event_group_id=20
)
select distinct t_user.cid,t_user.ctype,t_user.cname,t_user.d from t_user left join t_user_his
on t_user.ctype=t_user_his.ctype and t_user.cid=t_user_his.cid and t_user.cname=t_user_his.cname
where t_user_his.cid is null
or t_user_his.d<date_sub(t_user.d,180);


insert into table  tmp.mall_new_user_cid_all_ctype_cname
partition (d)
distribute by d;


select d,count(*)
from tmp.mall_new_user_cid_all_ctype_cname
group by d;