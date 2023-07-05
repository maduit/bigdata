-- 1.数据仓库构建
-- 1.1创建ods库
create database if not exists ods_didi;
-- 1.2创建dw数据库
create database if not exists dw_didi;
-- 1.3创建app数据库
create database if not exists app_didi;
use ods_didi;
-- 2.在ods层创建表
-- 2.1创建订单结构表
-- 创建用户订单表结构
create table if not exists ods_didi.t_user_order
(
    orderId         string comment '订单id',
    telephone       string comment '打车用户手机',
    lng             string comment '用户发起打车的经度',
    lat             string comment '用户发起打车的纬度',
    province        string comment '所在省份',
    city            string comment '所在城市',
    es_money        double comment '预估打车费用',
    gender          string comment '用户信息 - 性别',
    profession      string comment '用户信息 - 行业',
    age_range       string comment '年龄段（70后、80后、...）',
    tip             double comment '小费',
    subscribe       int comment '是否预约（0 - 非预约、1 - 预约）',
    sub_time        string comment '预约时间',
    is_agent        int comment '是否代叫（0 - 本人、1 - 代叫）',
    agent_telephone string comment '预约人手机',
    order_time      string comment '订单时间'
) partitioned by (dt string comment '时间分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
-- ods创建取消订单表
create table if not exists ods_didi.t_user_cancel_order
(
    orderId        string comment '订单ID',
    cstm_telephone string comment '客户联系电话',
    lng            string comment '取消订单的经度',
    lat            string comment '取消订单的纬度',
    province       string comment '所在省份',
    city           string comment '所在城市',
    es_distance    double comment '预估距离',
    gender         string comment '性别',
    profession     string comment '行业',
    age_range      string comment '年龄段',
    reason         int comment '取消订单原因（1 - 选择了其他交通方式、2 - 与司机达成一致，取消订单、3 - 投诉司机没来接我、4 - 已不需要用车、5 - 无理由取消订单）',
    cancel_time    string comment '取消时间'
) partitioned by (dt string comment '时间分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
-- ods创建订单表支付表
create table if not exists ods_didi.t_user_pay_order
(
    id                         string comment '支付订单ID',
    orderId                    string comment '订单ID',
    lng                        string comment '目的地的经度（支付地址）',
    lat                        string comment '目的地的纬度（支付地址）',
    province                   string comment '省份',
    city                       string comment '城市',
    total_money                double comment '车费总价',
    real_pay_money             double comment '实际支付总额',
    passenger_additional_money double comment '乘客额外加价',
    base_money                 double comment '车费合计',
    has_coupon                 int comment '是否使用优惠券（0 - 不使用、1 - 使用）',
    coupon_total               double comment '优惠券合计',
    pay_way                    int comment '支付方式（0 - 微信支付、1 - 支付宝支付、3 - QQ钱包支付、4 - 一网通银行卡支付）',
    mileage                    double comment '里程（单位公里）',
    pay_time                   string comment '支付时间'
) partitioned by (dt string comment '时间分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
-- ods创建用户评价表
create table if not exists ods_didi.t_user_evaluate
(
    id                  string comment '评价日志唯一ID',
    orderId             string comment '订单ID',
    passenger_telephone string comment '用户电话',
    passenger_province  string comment '用户所在省份',
    passenger_city      string comment '用户所在城市',
    eva_level           int comment '评价等级（1 - 一颗星、... 5 - 五星）',
    eva_time            string comment '评价时间'
) partitioned by (dt string comment '时间分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
-- 创建数据仓库 导入数据
load data local inpath '/export/data/didi/order.csv' into table ods_didi.t_user_order partition (dt = '2020-04-12');
load data local inpath '/export/data/didi/cancel_order.csv' into table ods_didi.t_user_cancel_order partition (dt = '2020-04-12');
load data local inpath '/export/data/didi/pay.csv' into table ods_didi.t_user_pay_order partition (dt = '2020-04-12');
load data local inpath '/export/data/didi/evaluate.csv' into table ods_didi.t_user_evaluate partition (dt = '2020-04-12');
truncate table dw_didi.t_user_pay_order;
-- 3.在dw层进行数据预处理use dw_didi;
-- 创建宽表语句
create table if not exists dw_didi.t_user_order_wide
(
    orderId          string comment '订单id',
    telephone        string comment '打车用户手机',
    lng              string comment '用户发起打车的经度',
    lat              string comment '用户发起打车的纬度',
    province         string comment '所在省份',
    city             string comment '所在城市',
    es_money         double comment '预估打车费用',
    gender           string comment '用户信息 - 性别',
    profession       string comment '用户信息 - 行业',
    age_range        string comment '年龄段（70后、80后、...）',
    tip              double comment '小费',
    subscribe        int comment '是否预约（0 - 非预约、1 - 预约）',
    subscribe_name   string comment '是否预约名称',
    sub_time         string comment '预约时间',
    is_agent         int comment '是否代叫（0 - 本人、1 - 代叫）',
    is_agent_name    string comment '是否代缴名称',
    agent_telephone  string comment '预约人手机',
    order_time       string comment '订单时间',
    order_date       string comment '订单时间，yyyy-MM-dd',
    order_year       string comment '年',
    order_month      string comment '月',
    order_day        string comment '日',
    order_hour       string comment '小时',
    order_time_range string comment '时间段'
) partitioned by (dt string comment '2020-04-12')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
-- 预处理sql语句 用户订单处理
insert overwrite table dw_didi.t_user_order_wide partition (dt = '2020-04-12')
select orderid,
       telephone,
       lng,
       lat,
       province,
       city,
       es_money,
       gender,
       profession,
       age_range,
       tip,
       subscribe,
       if(nvl(subscribe, 0) = 0, '非预约', '预约')                                                 as subscribe_name,
       case
           when subscribe = 0 or (subscribe is null) then '非预约'
           when subscribe = 1
               then '预约' end                                                                     as subscribe_name,
       date_format(sub_time, 'yyyy-MM-dd')                                                         as sub_time,
       is_agent,
       case when is_agent = 0 or (subscribe is null) then '本人' when is_agent = 1 then '代叫' end as is_agent_name,
       agent_telephone,
       substr(order_time, 1, 4)                                                                    as year,
       date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')                               as order_time,
       date_format(order_time, 'yyyy-MM-dd')                                                       as order_data,
       year(date_format(order_time, 'yyyy-MM-dd'))                                                 as order_year,
       month(date_format(order_time, 'yyyy-MM-dd'))                                                as order_month,
       day(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss'))                          as order_day,
       hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss'))                         as order_hour,
       case
           when hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) >= 1 and
                hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) < 5 then '凌晨'
           when hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) >= 5 and
                hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) < 8 then '早上'
           when hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) >= 8 and
                hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) < 11 then '上午'
           when hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) >= 11 and
                hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) < 13 then '中午'
           when hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) >= 13 and
                hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) < 17 then '下午'
           when hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) >= 17 and
                hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) < 19 then '晚上'
           when hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) >= 19 and
                hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) < 20 then '半夜'
           when hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) >= 20 and
                hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) < 24 then '深夜'
           when hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) >= 0 and
                hour(date_format(concat(order_time, ':00'), 'yyyy-MM-dd HH:mm:ss')) < 1 then '凌晨' end,
       date_format(order_time, 'yyyy-MM-dd HH:mm:ss')
from ods_didi.t_user_orderwhere length(order_time) >= 8  and dt = '2020-04-12';
create table if not exists dw_didi.t_user_cancel_order
(
    orderId     string,
    Profession  string,
    age_range   string,
    Reason      string,
    cancel_time string
) partitioned by (dt string comment '2020-04-12')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
insert overwrite table dw_didi.t_user_cancel_order partition (dt = '2020-04-12')
select orderId, profession, age_range, reason, cancel_timefrom ods_didi.t_user_cancel_orderwhere dt = '2020-04-12';
create table if not exists dw_didi.t_user_pay_order
(
    id             string comment '支付订单ID',
    orderId        string comment '订单ID',
    real_pay_money double comment '实际支付总额',
    has_coupon     int comment '是否使用优惠券（0 - 不使用、1 - 使用）',
    pay_way        int comment '支付方式（0-微信支付、1-支付宝支付、3-QQ钱包支付、4- 一网通银行卡支付）',
    mileage        double comment '里程（单位公里）',
    pay_time       string comment '支付时间'
) partitioned by (dt string comment '2020-04-12') ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
insert overwrite table dw_didi.t_user_pay_order partition (dt = '2020-04-12')
select id,
       orderId,
       real_pay_money,
       has_coupon,
       pay_way,
       mileage,
       pay_timefrom ods_didi.t_user_pay_orderwhere dt = '2020-04-12';
create table if not exists dw_didi.t_user_evaluate
(
    id        string comment '评价日志唯一ID',
    orderId   string comment '订单ID',
    eva_level int comment '评价等级（1 - 一颗星、... 5 - 五星）',
    eva_time  string comment '评价时间'
) partitioned by (dt string comment '时间分区') ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
insert overwrite table dw_didi.t_user_evaluate partition (dt = '2020-04-12')
select id, orderId, eva_level, eva_timefrom ods_didi.t_user_evaluatewhere dt = '2020-04-12';
-- 4.数据处理
-- 4.1 总订单笔数分析
-- 4.1.1计算4.12的总订单笔数分析
select max(dt) as `时间`, count(orderid) `订单总笔数`
from dw_didi.t_user_order_widewhere dt = '2020-04-12';
--建表
create table if not exists app_didi.t_order_total
(
    date_val string comment '日期(yyyy-MM-dd)',
    count    int comment '订单笔数'
) partitioned by (month string comment '按月分区yyyy-MM')
    row format delimited fields terminated by ',';
--加载数据据
insert into table app_didi.t_order_total partition (month = '2020-04')
select max(dt) as `时间`, count(orderid) as `订单时间`
from dw_didi.t_user_order_widewhere dt = '2020-04-12';
truncate table app_didi.t_order_total;
-- 4.2 预约订单/非预约订单占比分析-- sum,avg,max,min-- 预约单/总单*100%
select count(*) as cnt_totalfrom dw_didi.t_user_order_widewhere dt = '2020-04-12';
select max(order_date)       as `日期`,
       subscribe_name        as `是否预约`,
       count(subscribe_name) as cntfrom dw_didi.t_user_order_widewhere dt = '2020-04-12'
group by subscribe_name;
--左连接
select *
from (select max(order_date) as `日期`, subscribe_name as `是否预约`, count(subscribe_name) as cnt
      from dw_didi.t_user_order_wide
      where dt = '2020-04-12'
      group by subscribe_name) t1
         left join (select count(*) as cnt_total from dw_didi.t_user_order_wide where dt = '2020-04-12') t2;
-- 隐式内连接
select `日期`, `是否预约`, concat(round(cnt / cnt_total * 100, 2), '%') as `百分比`
from (select max(order_date) as `日期`,
subscribe_name as `是否预约`,
count(subscribe_name) as cnt
      from dw_didi.t_user_order_wide
      where dt = '2020-04-12'
      group by subscribe_name) t1,
     (select count(*) as cnt_total 
     from dw_didi.t_user_order_wide where dt = '2020-04-12') t2;
--开窗函数
select order_date     as                                        `日期`,
       subscribe_name as                                        `是否预约`,
       count(subscribe_name) over (partition by subscribe_name) cnt,
       count() over ()                                          cnt_totalfrom dw_didi.t_user_order_widewhere dt = '2020-04-12';
-- group by subscribe_name;
--开窗函数2
select max(`日期`) as `日期`, `是否预约`, concat(round(max(cnt) / max(cnt_total) * 100, 2), '%') `百分比`
from (select order_date     as                                        `日期`,
             subscribe_name as                                        `是否预约`,
             count(subscribe_name) over (partition by subscribe_name) cnt,
             count() over ()                                          cnt_total
      from dw_didi.t_user_order_wide
      where dt = '2020-04-12') tgroup by `是否预约`;
--方法三
select `日期`, `是否预约`, concat(round((cnt / sum(cnt) over ()) * 100, 2), '%') `百分比`
from (select max(order_date) as `日期`, subscribe_name as `是否预约`, count(subscribe_name) as cnt
      from dw_didi.t_user_order_wide
      where dt = '2020-04-12'
      group by subscribe_name) t;
create table if not exists app_didi.t_order_subscribe_percent
(
    date_val       string comment '日期',
    subscribe_name string comment '是否预约',
    percent_val    string comment '百分比'
) partitioned by (month string comment '年月yyyy-MM') row format delimited fields terminated by ',';
select `日期`,
       `是否预约`,
       cnt / sum(cnt) over () *
       100from(select max(order_date)       as `日期`, subscribe_name as `是否预约`, count(subscribe_name) as cnt      from dw_didi.t_user_order_wide      where dt = '2020-04-12'      group by subscribe_name) t;
insert overwrite table app_didi.t_order_subscribe_percent partition (month = '2020-04')
select `日期`, `是否预约`, concat(round((cnt / sum(cnt) over ()) * 100, 2), '%') `百分比`
from (select max(order_date) as `日期`, subscribe_name as `是否预约`, count(subscribe_name) as cnt
      from dw_didi.t_user_order_wide
      where dt = '2020-04-12'
      group by subscribe_name) t;
-- 4.3不同时段订单的个数
create table if not exists app_didi.t_order_timerange_total
(
    datetime  string comment '日期',
    timerange string comment '时间段',
    count     int comment '订单数量'
) partitioned by (month string comment '年月，yyyy-MM')
    row format delimited fields terminated by ',';
--sql
select max(dt), order_time_range, count(*) as order_cntfrom dw_didi.t_user_order_widewhere dt = '2020-04-12'
group by order_time_range;
--加载数据
insert overwrite table app_didi.t_order_timerange_total partition (month = '2020-04')
select max(dt), order_time_range, count(*) as order_cntfrom dw_didi.t_user_order_widewhere dt = '2020-04-12'
group by order_time_range;
--4.4不同年龄段、时段订单个数
select max(dt),
       age_range,
       order_time_range,
       count(*) as order_cntfrom dw_didi.t_user_order_widewhere dt = '2020-04-12'
group by age_range, order_time_range;
create table if not exists app_didi.t_order_age_and_time_range_total
(
    datetime         string comment '日期',
    age_range        string comment '年龄段',
    order_time_range string comment '时段',
    count            int comment '订单数量'
) partitioned by (month string comment '年月，yyyy-MM') row format delimited fields terminated by ',';
insert overwrite table app_didi.t_order_age_and_time_range_total partition (month = '2020-04')
select max(dt),
       age_range,
       order_time_range,
       count(*) as order_cntfrom dw_didi.t_user_order_widewhere dt = '2020-04-12'
group by age_range, order_time_range;
--4.4不同地域订单个数
select province, count(*) as order_cntfrom dw_didi.t_user_order_widewhere dt = '2020-04-12'
group by province;
--建表
create table if not exists app_didi.t_order_province_total
(
    datetime string comment '日期',
    province string comment '省份',
    count    int comment '订单数量'
) partitioned by (month string comment '年月，yyyy-MM') row format delimited fields terminated by ',';
insert overwrite table app_didi.t_order_province_total partition (month = '2020-04')
select '2020-04-12', province, count(*) as order_cntfrom dw_didi.t_user_order_widewhere dt = '2020-04-12'
group by province;
-- 4.5求订单客户职业排名top5
-- 第一步 ：按职业分组求客户数量
select max(dt), profession, count(orderId)
from dw_didi.t_user_order_widewhere dt = '2020-04-12'
group by profession;
-- 第二部 排名
select dt1, profession, cnt, row_number() over (order by cnt desc )
from (select max(dt) as dt1, profession, count(orderId) cnt
      from dw_didi.t_user_order_wide
      where dt = '2020-04-12'
      group by profession) t;
-- 取前五
select *
from (select dt1, profession, cnt, row_number() over (order by cnt desc ) as rk
      from (select max(dt) as dt1, profession, count(orderId) cnt
            from dw_didi.t_user_order_wide
            where dt = '2020-04-12'
            group by profession) t1) t2where rk <= 5;
with t1 as (select max(dt) dt1, profession, count(orderId) cnt
            from dw_didi.t_user_order_wide
            where dt = '2020-04-12'
            group by profession),
     t2 as (select dt1, profession, cnt, row_number() over (order by cnt desc ) as rk from t1)
select *
from t2where rk<=5;
select *
from (select t.profession, t.cnt, rank() over (order by t.cnt desc ) as rk
      from (select profession, count(*) as cnt
            from dw_didi.t_user_order_wide
            group by profession) t) ttwhere tt.rk <= 5;
--建表
create table if not exists app_didi.t_order_profession_total_topn
(
    profession string comment '职业',
    Order_cnt  int comment '订单数量',
    rk         int comment '排名'
) partitioned by (month string comment '年月，yyyy-MM') row format delimited fields terminated by ',';
--加载数据
insert overwrite table app_didi.t_order_profession_total_topn partition (month = '2020-04')
select *
from (select t.profession, t.cnt, rank() over (order by t.cnt desc ) as rk
      from (select profession, count(*) as cnt
            from dw_didi.t_user_order_wide
            group by profession) t) ttwhere tt.rk <= 5;
--4.6用户订单取消占比
select '2020-04-12'                                                date_val,
       concat(round(t1.total_cnt / t2.total_cnt * 100, 2), '%') as cancel_order_percentfrom (select count(orderid) as total_cnt      from ods_didi.t_user_cancel_order      where dt = '2020-04-12') t1        , (select count(orderid) as total_cnt
                                                                                                                                                                                                                  from dw_didi.t_user_order_wide
                                                                                                                                                                                                                  where dt = '2020-04-12') t2;
--创建表
create table if not exists app_didi.t_order_cancel_order_percent
(
    datetime             string comment '日期',
    cancel_order_percent string comment '百分比'
) partitioned by (month string comment '年月，yyyy-MM') row format delimited fields terminated by ',';
--加载数据
insert overwrite table app_didi.t_order_cancel_order_percent partition (month = '2020-04')
select '2020-04-12'                                                date_val,
       concat(round(t1.total_cnt / t2.total_cnt * 100, 2), '%') as percent_valfrom (select count(*) total_cnt from ods_didi.t_user_cancel_order where dt = '2020-04-12') t1        , (select count(*) total_cnt
                                                                                                                                                                                      from dw_didi.t_user_order_wide
                                                                                                                                                                                      where dt = '2020-04-12') t2;
-- 4.8统计用户取消订单原因top1
with t1 as (select reason, profession, count(reason) over () cnt
            from dw_didi.t_user_cancel_order
            where dt = '2020-04-12'),
     t2 as (select reason, profession, cnt, row_number() over (order by cnt desc) as rk from t1)
select *
from t2where rk<=5;
insert overwrite table app_didi.t_order_cancel_reason partition (month = '2020-04')
select *
from (select t.profession, t.cnt, rank() over (order by t.cnt desc ) as rk
      from (select profession, count(*) as cnt
            from dw_didi.t_user_cancel_order
            group by profession) t) ttwhere tt.rk <= 5;
--建表
create table if not exists app_didi.t_order_cancel_reason
(
    profession string comment '职业',
    cancel_cnt int comment '订单数量',
    rk         int comment '排名'
) partitioned by (month string comment '年月，yyyy-MM') row format delimited fields terminated by ',';
--统计每个省订单量最高的城市top3
select city, count(city)
from dw_didi.t_user_order_widewhere length(city) > 0group by  city;
--建表
insert overwrite table app_didi.t_order_city partition (month = '2020-04')
select *
from (select t.city, t.cnt, rank() over (order by t.cnt desc ) as rk
      from (select city, count(city) as cnt
            from dw_didi.t_user_order_wide
            where length(city) > 0
            group by city) t) ttwhere tt.rk <= 3;
create table if not exists app_didi.t_order_city
(
    city      string comment '城市',
    order_cnt int comment '订单数量',
    rk        int comment '排名'
) partitioned by (month string comment '年月，yyyy-MM')
    row format delimited fields terminated by ',';
--统计订单支付中使用优惠券的百分比
create table if not exists app_didi.t_order_dicount
(
    isdicount string comment '是否使用优惠券',
    order_cnt string comment '百分比'
) partitioned by (month string comment '年月，yyyy-MM') row format delimited fields terminated by ',';
insert overwrite table app_didi.t_order_dicount partition (month = '2020-04')
select `是否使用优惠券`, concat(round((cnt / sum(cnt) over ()) * 100, 2), '%') `百分比`
from (select has_coupon as `是否使用优惠券`, count(has_coupon) as cnt
      from dw_didi.t_user_pay_order
      where has_coupon != 17
      group by has_coupon) t;
--统计用户五星级好评的百分比
create table if not exists app_didi.t_order_five_start
(
    fivestart string comment '是否是5',
    order_cnt string comment '百分比'
) partitioned by (month string comment '年月，yyyy-MM') row format delimited fields terminated by ',';
insert overwrite table app_didi.t_order_five_start partition (month = '2020-04')
select `是否是5`, concat(round((cnt / sum(cnt) over ()) * 100, 2), '%') `百分比`
from (select eva_level as `是否是5`, count(eva_level) as cnt
      from dw_didi.t_user_evaluate
      where length(eva_level) > 0
      group by eva_level) t;
