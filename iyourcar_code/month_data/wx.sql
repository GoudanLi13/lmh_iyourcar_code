select `时间周期`,
`商城类型`,
`平台`,
`新用户访问商城人数`,
`老用户访问商城人数`,
`访问商城人数`,
`新用户下单人数`,
`老用户下单人数`,
`新用户下单人数`/`新用户访问商城人数` as `新用户下单转化率`,
`老用户下单人数`/`老用户访问商城人数` as `老用户下单转化率`,
`下单人数`/`访问商城人数`,
`下单量`,
`客单价`,
`gmv`,
`利润`,
`利润率`,
`转化率`,
`复购人数`,
`复购率` from tmp.rpt_mall_global_data_month order by
(case `商城类型` when '有车币商城' then 1
    when '声望商城' then 2
    when '总计'then 3 end )
asc
,
(case `平台` when '安卓' then 1
    when 'iOS' then 2
    when 'App'then 3
    when '小程序' then 4
    when '总计' then 5
    end );