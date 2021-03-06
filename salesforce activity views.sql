
--Last Modified: 20170615 - changing views to Fivetran tables for hourly refresh!

drop view work_revopt.salesforce_activity;
create view work_revopt.salesforce_activity as (

select
id activity_id
,'Task' as activity_type
,owner_id
,created_by_id
,last_modified_by_id
,created_date created_date_ts
,activity_date
,cast(activity_date as timestamp) activity_date_ts
,cast(completed_date_time_c as date) completed_date
,completed_date_time_c completed_date_ts
,account_id 
,case when left(who_id, 3) = '00Q' then who_id else null end lead_id
,case when left(who_id,3) = '003' then who_id else null end contact_id
,case when left(what_id,3) = '006' then what_id else null end opportunity_id
,type
,task_subtype sub_type
,case when subject like '%Email: >>%' then 'OB' 
          when subject like '%[Out]%' then 'OB'
    when subject like '%Email: <<%' then 'IB'
    when subject like '%[In]%' then 'IB'
    else 'NA' end email_direction
    
,case when lower(subject) like '%outreach%' then 1 else 0 end outreach_flag --basic old Outreach flag (for all Outreach use)
--,case when lower(subject) like '%outreach gmail%' then 1 else 0 end outreach_gmail_flag --one off emails sent through Outreach plugin through Gmail
--,case when lower(subject) like '%outreach%' and lower(subject) like '%mktg%' then 1 else 0 end outreach_mkting_flag --marketing Outreach sequences
--,case when lower(subject) like '%outreach%' and lower(subject) not like '%mktg%' and lower(subject) not like '%gmail%' then 1 else 0 end outreach_rep_seq_flag --marketing Outreach sequences

,case when lower(subject) like '%email: << %' or lower(subject) like '%email: >> %' then 1 else 0 end groove_flag  
,case when is_closed = 'true' then 1 else 0 end completed_task
,zeroifnull(case when MEETING_CHECKBOX_C = 'true' then 1 else 0 end) meeting_checkbox
,zeroifnull(case when QUALIFICATIONS_CHECKBOX_DEL_C = 'true' then 1 else 0 end) qualifications_checkbox
,subject

--from salesforce.task
from fivetran_db.prod_salesforce.task

where is_deleted = 'false'

union all

select
id activity_id
,'Event' as activity_type
,owner_id
,created_by_id
,last_modified_by_id
,created_date created_date_ts
,activity_date
,activity_date_time::timestamp activity_date_ts
,case when MEETING_CHECKBOX_C = 'true' then activity_date
        else cast(completed_date_time_c as date) end completed_date
,case when MEETING_CHECKBOX_C = 'true' then activity_date_time
        else completed_date_time_c end completed_date_ts
,account_id 
,case when left(who_id, 3) = '00Q' then who_id else null end lead_id
,case when left(who_id,3) = '003' then who_id else null end contact_id
,case when left(what_id,3) = '006' then what_id else null end opportunity_id
,type
,event_type_c sub_type
,'NA' as email_direction
,case when lower(subject) like '%outreach%' then 1 else 0 end outreach_flag
,case when lower(subject) like '%email: << %' or lower(subject) like '%email: >> %' then 1 else 0 end groove_flag  
,0 completed_task
,zeroifnull(case when MEETING_CHECKBOX_C = 'true' then 1 else 0 end) meeting_checkbox
,zeroifnull(case when QUALIFICATIONS_CHECKBOX_DEL_C = 'true' then 1 else 0 end) qualifications_checkbox
,subject

--from salesforce.event
from fivetran_db.prod_salesforce.event

where is_deleted = 'false'

);


drop view work_revopt.salesforce_completed_activity;
create view work_revopt.salesforce_completed_activity as ( 
select
activity_id
,owner_id person_id
,account_id
,lead_id
,contact_id
,opportunity_id
,activity_type
,coalesce(completed_date, cast(created_date_ts as date)) completed_date
,coalesce(completed_date_ts, created_date_ts) completed_date_ts
,type
,sub_type
,email_direction
,outreach_flag
,subject
,completed_task
,meeting_checkbox
,qualifications_checkbox

from work_revopt.salesforce_activity 

where activity_date <= current_date
and (
        meeting_checkbox = 1
        or completed_task = 1
     )
     
union all

select
activity_id
,created_by_id person_id
,account_id
,lead_id
,contact_id
,opportunity_id
,'Meeting Set' activity_type
,coalesce(completed_date, cast(created_date_ts as date)) completed_date
,coalesce(completed_date_ts, created_date_ts) completed_date_ts
,type
,sub_type
,email_direction
,outreach_flag
,subject
,completed_task
,meeting_checkbox
,qualifications_checkbox

from work_revopt.salesforce_activity 

where activity_date <= current_date
and activity_type = 'Event' and meeting_checkbox = 1
)
;




drop view work_revopt.salesforce_account_activity;
create view work_revopt.salesforce_account_activity as (
select
account_id
,max(completed_date) last_activity
,count(case when completed_date between dateadd(day,-30,current_date) and current_date then activity_id else null end) act_cnt_30
,count(case when completed_date between dateadd(day,-90,current_date) and current_date then activity_id else null end) act_cnt_90
,count(case when completed_date between dateadd(day,-180,current_date) and current_date then activity_id else null end) act_cnt_180

from (
select
account_id
,activity_id
,completed_date
 
from 
work_revopt.salesforce_completed_activity
where completed_date is not null
and person_id in
        (
        select distinct id
        from work_revopt.salesforce_user_detail
        where ns_department_full_name like 'Sales%'
        and ns_department_full_name not like 'Sales : 206%'
        )
      
) a
group by 1);
