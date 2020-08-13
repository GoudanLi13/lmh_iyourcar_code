--0812 0814交付
--1.4个车系的7月新增帖子数，历史帖子数

select
from iyourcar_dw.stage_all_service_day_iyourcar_platform_post

select *
from iyourcar_dw.dws_prop_day_car_series
where name like '%C-HR%';


select * from iyourcar_dw.dws_prop_day_content;

select if(array_contains(car_series_ids,'1618'),1,0) from iyourcar_dw.dws_prop_day_content where content_id='1#17217';

select * from iyourcar_dw.stage_all_service_day_iyourcar_platform_post_ref_car;

--车系7月新增帖子数
select count(distinct content_id)
from iyourcar_dw.dws_prop_day_content
where substr(create_time,0,10) between '2020-07-01' and '2020-07-31'
and array_contains(car_series_ids,'1787')
and kind=2;

select count(content_id)
from iyourcar_dw.dws_prop_day_content
where array_contains(car_series_ids,'1787')
and kind=2;

--2.4个车系的精华帖子数
select count(distinct content_id)
from
(select content_id,original_id
from iyourcar_dw.dws_prop_day_content
where array_contains(car_series_ids,'1787')
and kind=2) as content
join iyourcar_dw.stage_all_service_day_iyourcar_platform_post as post
on content.original_id=post.id
where post.fine_level in(2,3)
;

--4.4个车系帖子评论数总和
select sum(count_comment)
from iyourcar_dw.dws_prop_day_content
where array_contains(car_series_ids,'1787')
and kind=2;