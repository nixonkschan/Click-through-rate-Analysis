################################################################################################################
#                                                                                                              #
#                                        MID_TERM CTR PROJECT                                                  #
#                                             NIXON CHAN                                                       #
#                                                                                                              #
################################################################################################################


#create database for CRT project and establish the connection
create database if not exists ctr;

use ctr;
############## CREATE TABLE #########################
################# VIEWS #############################
drop table if exists views;
create table views (
    view_time datetime,
    txn_datetime datetime not null,
    user_id varchar(10) COLLATE utf8mb4_bin not null,
    store_id varchar(100),
    ad_id varchar(10),
    primary key (view_time, txn_datetime, user_id)
);

show index from views;

################# TRANSACTIONS ###################

show processlist;

drop table if exists transactions;

#Due to the computer capability, I have to remove the primary key for loading
# In this case, txt_datetime and user_id should be set as the primary key
create table transactions (
    user_id varchar(10) COLLATE utf8mb4_bin not null,
    txn_datetime datetime not null,
    pay_amt float,
    pay_type varchar(10),
    card_type varchar(10),
    store_id varchar(100),
    network varchar(10),
    industry int,
    gender char(10),
    address varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    #primary key(txn_datetime, user_id)
);

show index from transactions;

############### CLICKS TABLE ####################

drop table if exists clicks;

create table clicks (
    click_datetime datetime,
    txn_datetime datetime not null,
    user_id varchar(10) COLLATE utf8mb4_bin not null,
    store_id varchar(100),
    ad_id varchar(10),
    primary key(click_datetime, txn_datetime, user_id)
);

show index from clicks;

###################### AD_INFO TABLE #########################
drop table if exists ad_info;

create table ad_info(
    row_id int,
    ad_id varchar(6) not null,
    ad_loc int,
    ad_label varchar(10),
    begin_datetime datetime,
    end_datetime datetime,
    pic_url varchar(500),
    ad_url varchar(500),
    ad_desc_url varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    ad_copy varchar(255),
    min_money float,
    store_id varchar(1000),
    order_num varchar(50),
    user_id varchar(1000) COLLATE utf8mb4_bin,
    city_id varchar(500),
    idu_category varchar(10),
    click_hide varchar(1),
    price float,
    sys varchar(50),
    network varchar(50),
    user_gender varchar(10),
    pay_type varchar(10),
    primary key(ad_id)
);

show index from ad_info;

################### LOADING DATA #######################

SET GLOBAL LOCAL_INFILE = 1;

############LOADING VIEWS ######################
truncate views;


load data local infile 'D:/DS BootCamp/Mid-Term/aug-view-01-09.csv' into table views
fields terminated by ","
enclosed by '"'
lines terminated by "\n"
(@view_time, @txn_datetime, user_id, store_id, ad_id)
SET view_time = STR_TO_DATE(REPLACE(@view_time, '"', ''), '%Y-%m-%d %H:%i:%s'),
    txn_datetime  = STR_TO_DATE(REPLACE(@txn_datetime, '"', ''), '%Y-%m-%d %H:%i:%s')
;

load data local infile 'D:/DS BootCamp/Mid-Term/aug-view-10.csv' into table views
fields terminated by ","
enclosed by '"'
lines terminated by "\n"
(@view_time, @txn_datetime, user_id, store_id, ad_id)
SET view_time = STR_TO_DATE(REPLACE(@view_time, '"', ''), '%Y-%m-%d %H:%i:%s'),
    txn_datetime  = STR_TO_DATE(REPLACE(@txn_datetime, '"', ''), '%Y-%m-%d %H:%i:%s')
;

load data local infile 'D:/DS BootCamp/Mid-Term/aug-view-11-31.csv' into table views
fields terminated by ","
enclosed by '"'
lines terminated by "\n"
(@view_time, @txn_datetime, user_id, store_id, ad_id)
SET view_time = STR_TO_DATE(REPLACE(@view_time, '"', ''), '%Y-%m-%d %H:%i:%s'),
    txn_datetime  = STR_TO_DATE(REPLACE(@txn_datetime, '"', ''), '%Y-%m-%d %H:%i:%s')
;

############LOADING CLICKS ######################

truncate clicks;
load data local infile 'D:/DS BootCamp/Mid-Term/aug-click-01-09.csv' into table clicks
fields terminated by ","
enclosed by '"'
lines terminated by "\n"
(@click_datetime,@txn_datetime,user_id,store_id,ad_id)
set click_datetime = STR_TO_DATE(REPLACE(@click_datetime, '"', ''), '%Y-%m-%d %H:%i:%s'),
    txn_datetime = STR_TO_DATE(REPLACE(@txn_datetime, '"', ''), '%Y-%m-%d %H:%i:%s')
;

load data local infile 'D:/DS BootCamp/Mid-Term/aug-click-10.csv' into table clicks
fields terminated by ","
enclosed by '"'
lines terminated by "\n"
(@click_datetime,@txn_datetime,user_id,store_id,ad_id)
set click_datetime = STR_TO_DATE(REPLACE(@click_datetime, '"', ''), '%Y-%m-%d %H:%i:%s'),
    txn_datetime = STR_TO_DATE(REPLACE(@txn_datetime, '"', ''), '%Y-%m-%d %H:%i:%s')
;


load data local infile 'D:/DS BootCamp/Mid-Term/aug-click-11-31.csv' into table clicks
fields terminated by ","
enclosed by '"'
lines terminated by "\n"
(@click_datetime,@txn_datetime,user_id,store_id,ad_id)
set click_datetime = STR_TO_DATE(REPLACE(@click_datetime, '"', ''), '%Y-%m-%d %H:%i:%s'),
    txn_datetime = STR_TO_DATE(REPLACE(@txn_datetime, '"', ''), '%Y-%m-%d %H:%i:%s')
;


############LOADING  TRANSACTIONS ######################

truncate transactions;

load data local infile 'D:/DS BootCamp/Mid-Term/trans_4.csv' into table transactions
fields terminated by ","
enclosed by '"'
lines terminated by "\n"
(user_id, @txn_datetime, pay_amt, pay_type, card_type, store_id, network, industry, gender, @address, @truncated_add)
# Separator,"," was found in address column when loading in
SET
address = case
    when not isnull(@truncated_add)
    then
        concat(concat(@address, ','), @truncated_add)
    else
        @address
    end,
txn_datetime  = STR_TO_DATE(REPLACE(@txn_datetime, '"', ''), '%Y-%m-%d %H:%i:%s');

select count(*) from transactions;


load data local infile 'D:/DS BootCamp/Mid-Term/trans_5.csv' into table transactions
fields terminated by ","
enclosed by '"'
lines terminated by "\n"
(user_id, txn_datetime, pay_amt, pay_type, card_type, store_id, network, industry, gender, @address, @truncated_add)
# Separator,"," was found in address column when loading in
SET address = case
    when not isnull(@truncated_add)
    then
        concat(concat(@address, ','), @truncated_add)
    else
        @address
    end,
txn_datetime  = STR_TO_DATE(REPLACE(@txn_datetime, '"', ''), '%Y-%m-%d %H:%i:%s');


load data local infile 'D:/DS BootCamp/Mid-Term/trans_6.csv' into table transactions
fields terminated by ","
enclosed by '"'
lines terminated by "\n"
(user_id, txn_datetime, pay_amt, pay_type, card_type, store_id, network, industry, gender, @address, @truncated_add)
# Separator,"," was found in address column when loading in
SET address = case
    when not isnull(@truncated_add)
    then
        concat(concat(@address, ','), @truncated_add)
    else
        @address
    end,
txn_datetime  = STR_TO_DATE(REPLACE(@txn_datetime, '"', ''), '%Y-%m-%d %H:%i:%s')
;


############LOADING  AD_INFO ######################

truncate ad_info;

load data local infile 'D:/DS BootCamp/Mid-Term/aug-ad-info-with-tags.csv' into table ad_info
fields terminated by ","
enclosed by '"'
lines terminated by "\n"
(row_id, ad_id, ad_loc, ad_label, @begin_datetime, @end_datetime, pic_url, ad_url, ad_desc_url,
 ad_copy, min_money, store_id, order_num, user_id, city_id, idu_category, click_hide,
 price, sys, network, user_gender, pay_type)
set begin_datetime = STR_TO_DATE(REPLACE(@begin_datetime, '"', ''), '%Y-%m-%d %H:%i:%s'),
    end_datetime = STR_TO_DATE(REPLACE(@end_datetime, '"', ''), '%Y-%m-%d %H:%i:%s')
;

############# CHECKING LOADED DATA #################
select * from views limit 20 ;
select count(*) from views;

select * from clicks limit 20 ;
select count(*) from clicks;

select * from transactions limit 20 ;
select count(*) from transactions;

select * from ad_info;
select count(*) from ad_info;

#######################################################################################################################

# Exploring the data of 2017-08-03 and join transaction and view table to explore the result
select count(*) from transactions t
where t.txn_datetime between '2017-08-03 00:00:00' and '2017-08-03 23:59:59';

select count(*) from transactions t left join views v using(user_id, txn_datetime)
where t.txn_datetime between '2017-08-03 00:00:00' and '2017-08-03 23:59:59';

select * from transactions t left join views v using(user_id, txn_datetime)
where t.txn_datetime between '2017-08-03 00:00:00' and '2017-08-03 23:59:59'
order by user_id, txn_datetime;

########################################################################
# Due to the capability of my computer, I have to extract the
# data of 2017-08-23 and store in another table for further manipulation
################# VIEWS SAMPLE  (2017-08-03) ###########################

# Create tables for storing the data of views (between '2017-08-03 00:00:00' and '2017-08-03 23:59:59')
drop table if exists views_sample;
create table views_sample (
    view_time datetime,
    txn_datetime datetime not null,
    user_id varchar(10) COLLATE utf8mb4_bin not null,
    store_id varchar(100),
    ad_id varchar(10),
    primary key (view_time, txn_datetime, user_id)
);

show index from views_sample;

# Extract and store data into views_sample
insert into views_sample
select * from views where txn_datetime between '2017-08-03 00:00:00' and '2017-08-03 23:59:59';

select * from views_sample limit 10;

################# TRANSACTIONS SAMPLE  (2017-08-03) ###################

drop table if exists txn_sample;

# Create tables for storing the data of transactions (between '2017-08-03 00:00:00' and '2017-08-03 23:59:59')
create table txn_sample (
    user_id varchar(10) COLLATE utf8mb4_bin not null,
    txn_datetime datetime not null,
    pay_amt float,
    pay_type varchar(10),
    card_type varchar(10),
    store_id varchar(100),
    network varchar(10),
    industry int,
    gender char(10),
    address varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    #primary key(txn_datetime, user_id)
);
show index from txn_sample;

# Extract and store data into txn_sample
insert into txn_sample
select * from transactions where txn_datetime between '2017-08-03 00:00:00' and '2017-08-03 23:59:59';

select * from txn_sample limit 10;

########## CLICKS SAMPLE (2017-08-03) ####################

# Create tables for storing the data of clicks (between '2017-08-03 00:00:00' and '2017-08-03 23:59:59')
drop table if exists clicks_sample;

create table clicks_sample (
    click_datetime datetime,
    txn_datetime datetime not null,
    user_id varchar(10) COLLATE utf8mb4_bin not null,
    store_id varchar(100),
    ad_id varchar(10),
    primary key(click_datetime, txn_datetime, user_id)
);

# Extract and store data into clicks_sample
insert into clicks_sample
select * from clicks where txn_datetime between '2017-08-03 00:00:00' and '2017-08-03 23:59:59';

select * from clicks_sample limit 10;

#####################################################################################
############# MANIPULATION THE VIEWS_SAMPLE AND CREATE NEW FEATURE ##################
#####################################################################################

# Bring next view_time to the same row for calculating the time difference and take the average later.
# Also, count the view time within the same partition and assign a row number for filtering later.
###################################################################################################

# Bring next view_time to the same row for calculating the time difference later.
select  *,
        lead(view_time, 1) over (partition by user_id, txn_datetime order by view_time) as next_view,
        count(view_time) over (partition by user_id, txn_datetime) as tot_view_time,
        row_number() over (partition by user_id, txn_datetime order by view_time) as rnum
from views_sample
order by user_id, txn_datetime;

# Calculate the time different between each views and calculate the average (second)
select *, avg(case when next_view is not null and view_time is not null then timestampdiff(second, view_time, next_view) end)
        over (partition by user_id, txn_datetime)as avg_view_time
from (
            # Bring next view_time to the same row for calculating the time difference later.
            select  *,
                lead(view_time, 1) over (partition by user_id, txn_datetime order by view_time) as next_view,
                count(view_time) over (partition by user_id, txn_datetime) as tot_view_time,
                row_number() over (partition by user_id, txn_datetime order by view_time) as rnum
                from views_sample
                order by user_id, txn_datetime
     ) as sub;

# Filter out the first row only
select *
from (
        # Calculate the time different each view and calculate the average (second)
        select *, avg(case when next_view is not null and view_time is not null then timestampdiff(second, view_time, next_view) end)
        over (partition by user_id, txn_datetime)as avg_view_time
            from (
                        # Bring next view_time to the same row for calculating the time difference later.
                        select  *,
                            lead(view_time, 1) over (partition by user_id, txn_datetime order by view_time) as next_view,
                            count(view_time) over (partition by user_id, txn_datetime) as tot_view_time,
                            row_number() over (partition by user_id, txn_datetime order by view_time) as rnum
                            from views_sample
                            order by user_id, txn_datetime
                 ) as sub
     )as sub2
where rnum=1;

drop table if exists views_w_feature;

# Create table to store the views data with new features
create table views_w_feature (
    view_time datetime,
    txn_datetime datetime not null,
    user_id varchar(10) COLLATE utf8mb4_bin not null,
    store_id varchar(100),
    ad_id varchar(10),
    tot_view_time int,
    avg_view_time int,
    primary key (view_time, txn_datetime, user_id)
);

truncate views_w_feature;

# Insert the dataset with new features into views_w_feature table
insert into views_w_feature (view_time, txn_datetime, user_id, store_id, ad_id, tot_view_time, avg_view_time)
    (
        # Filter out the first row only
        select view_time, txn_datetime, user_id, store_id, ad_id, tot_view_time, avg_view_time
            from (
                     # Calculate the time different each view and calculate the average (second)
                    select *, avg(case when next_view is not null and view_time is not null
                                    then timestampdiff(second, view_time, next_view)
                                    end)
                    over (partition by user_id, txn_datetime)as avg_view_time
                        from (
                                    # Bring next view_time to the same row for calculating the time difference later.
                                    select  *,
                                        lead(view_time, 1) over (partition by user_id, txn_datetime order by view_time) as next_view,
                                        count(view_time) over (partition by user_id, txn_datetime) as tot_view_time,
                                        row_number() over (partition by user_id, txn_datetime order by view_time) as rnum
                                        from views_sample
                                        order by user_id, txn_datetime
                             ) as sub
                 )as sub2
            where rnum=1
    );

select count(*) from views_w_feature;
# no of record: 903619

##############################################################################################
###################### MANIPULATION THE CLICKS_SAMPLE AND TARGET LABEL #######################
##############################################################################################

# Count the number of click and assign row number, so only one row of the partition will be filtered out
select *,
       count(click_datetime) over (partition by user_id, txn_datetime) as tot_click,
       row_number() over (partition by user_id, txn_datetime) as rnum
from clicks_sample;


# Add a target label, clicked, for ML model
select *, case when tot_click >=1 then 1 else 0 end as clicked
    from (
            # Count the number of click and assign row number, so only one row of the partition will be filtered out
            select *,
                   count(click_datetime) over (partition by user_id, txn_datetime) as tot_click,
                   row_number() over (partition by user_id, txn_datetime) as rnum
            from clicks_sample
         ) as sub
    where rnum = 1;

# Create table to store the clicks data with new features
drop table if exists clicks_w_feature;

create table clicks_w_feature (
    txn_datetime datetime not null,
    user_id varchar(10) COLLATE utf8mb4_bin not null,
    store_id varchar(100),
    ad_id varchar(10),
    clicked tinyint,
    primary key(txn_datetime, user_id)
);


# Insert the dataset with new features into clicks_w_feature
truncate clicks_w_feature;

insert into clicks_w_feature (txn_datetime, user_id, store_id, ad_id, clicked)
    (
        # Add a target label, clicked, for ML model
        select txn_datetime, user_id, store_id, ad_id, case when tot_clicks >=1 then 1 else 0 end as clicked
            from (
                    # Count the number of click and assign row number, so only one row of the partition will be filtered out
                    select *,
                           count(click_datetime) over (partition by user_id, txn_datetime) as tot_clicks,
                           row_number() over (partition by user_id, txn_datetime) as rnum
                    from clicks_sample
                 ) as sub
            where rnum = 1
    );


select * from clicks_w_feature limit 500;
select count(*) from clicks_w_feature;

# total no of record: 109160

##############################################################################################
###################### MANIPULATION THE AD_INFO AND CREATE NEW FEATURE #######################
##############################################################################################

drop table if exists ad_info_w_feature;

create table ad_info_w_feature(
    ad_id varchar(6) not null,
    ad_loc int,
    ad_label varchar(10),
    ad_duration bigint,
    ad_no_city smallint,
    primary key(ad_id)
);

# Calculate the ad duration and select some features out from the list
# The reason for removing the feature is the high missing rate.
select *,
       # Calculate the time duration of the ad
       case     when begin_datetime is not null and end_datetime is not null
                then
                    timestampdiff(second, begin_datetime, end_datetime)
                else
                    0 end as ad_duration,
        # Calculate the no of city in city_id column
       case     when city_id = '' or city_id is null
                then 0
                else LENGTH(city_id) - LENGTH(REPLACE(city_id, '|', '')) + 1 end as ad_no_city
from ad_info;

truncate  ad_info_w_feature;

insert into ad_info_w_feature (ad_id, ad_loc, ad_label, ad_duration, ad_no_city)
(
 select ad_id, ad_loc, ad_label,
        # Calculate the time duration of the ad
        case    when begin_datetime is not null and end_datetime is not null
                then
                    timestampdiff(second, begin_datetime, end_datetime)
                else
                    0 end as ad_duration,
        # Calculate the no of city in city_id column
        case     when city_id = '' or city_id is null
                then 0
                else LENGTH(city_id) - LENGTH(REPLACE(city_id, '|', '')) + 1 end as ad_no_city
    from ad_info
);

select * from ad_info_w_feature;
select count(*) from ad_info_w_feature;

#No of records: 728


###################################################################################################################
##############  JOIN TRANSACTIONS WITH VIEWS_W_FEATURE, CLICKS_W_FEATURE AND AD_INFO_W_FEATURE TABLE  #############
###################################################################################################################

# Explore the date by join them together
select count(*) from txn_sample;

select t.*, v.*, c.*, a.* from txn_sample t left join views_w_feature v  using (user_id, txn_datetime)
                          left join clicks_w_feature c using (user_id, txn_datetime)
                          left join ad_info a on c.ad_id =a.ad_id;


select count(*) from txn_sample t left join views_w_feature v  using (user_id, txn_datetime)
                          left join clicks_w_feature c using (user_id, txn_datetime)
                          left join ad_info a on c.ad_id =a.ad_id;


# Create table to store the final result
drop table if exists CTR_sample;

create table CTR_sample (
    txn_hour tinyint unsigned,
    t_pay_amt float,
    t_pay_type varchar(10),
    t_card_type varchar(10),
    t_network varchar(10),
    t_industry int,
    t_gender char(10),
    txn_view_time int,
    tot_view_count int,
    avg_view_time int,
    clicked tinyint,
    ad_duration bigint,
    ad_no_city smallint
);


truncate CTR_sample;

# Insert and store the result into a table, at the same time, calculate the time differenet between the transaction and view time
# Remarks: 1) Column will be discarded due to too many missing value
#                                                        (Ad_info :     ad_desc_url, ad_copy, min_money, order_num,
#                                                                       user_id, idu_category,click_hide, price,
#                                                                       sys, network, user_gender, pay_type
#          2) Column dropped to meaningless string or ID:
#                                                       (Ad_info :      row_id, ad_id, ad_loc, ad_label, pic_url,
#                                                                       ad_url, ad_desc_url)
#                                                       (Clicks :       user_id, store_id, ad_id)
#                                                       (Views :        user_id, store_id, ad_id)
#                                                       (Transaction:   user_id, store_id, address)

insert into CTR_sample
(   txn_hour, t_pay_amt, t_pay_type, t_card_type, t_network, t_industry,
    t_gender, txn_view_time, tot_view_count, avg_view_time,
    clicked, ad_duration, ad_no_city)
    (
        # Join all the transactions with views_w_feature, clicks_w_feature and ad_info together
        select  hour(t.txn_datetime), t.pay_amt, t.pay_type, t.card_type,  t.network, t.industry,
                t.gender, timestampdiff(second, txn_datetime, view_time) as txn_view_time, v.tot_view_time, v.avg_view_time,
                c.clicked, a.ad_duration, a.ad_no_city
            from txn_sample t left join views_w_feature v  using (user_id, txn_datetime)
            left join clicks_w_feature c using (user_id, txn_datetime)
            left join ad_info_w_feature a on c.ad_id =a.ad_id
    );


##########################################################
# Update values after merging all the tables
##########################################################
# Update value of "clicked" after merging all the tables.
# Since all clicked record, clicked=1, shall have merged,
# therefore, all records with 'clicked is null' shall be set to 0

update CTR_sample set clicked=0 where clicked is null;

# Update value of "tot_view_count" after merging all the tables.
# Since all case with number of view greater than 1 shall have merged,
# therefore, all records with 'tot_clicks is null' shall be set to 0
update CTR_sample set tot_view_count=0 where tot_view_count is null;

select * from CTR_sample limit 1000;
select count(*) from CTR_sample;

####################### Answer #########################

# What are the total transactions, views, and clicks in your final table (after Joins/unions for all or selected days )?
# Answer : 1,254,153 no of record


# and what are the start and end Datetime of the dataset?
# Answer : only between 2017-08-03 00:00:00 and 2017-08-03 23:59:59

#Add labels to your table (clicked or not)
# Answer : Target label named as 'clicked' was added in table, 'CTR_sample'


# Save the results to a table/view (use for the ML part)
# Answer : The final result was saved to 'CTR_sample' table.

# 0,2800,4JBo,DEBIT,unknown,1219,male,10,2,17,0,,
# 0,7600,4JBo,CREDIT,4g,1000,male,5,1,,0,,
# 0,4000,zO8g,CREDIT,unknown,1204,male,5,1,,0,,
# 0,1000,4JBo,DEBIT,4g,1000,male,7,1,,0,,
# 0,700,4JBo,DEBIT,wifi,1203,female,6,1,,0,,







