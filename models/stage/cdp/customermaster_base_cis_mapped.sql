{{
    config(
        alias = 'customermaster_base_cis_mapped'

    )
}}
 
with cust46 as (
        select 
          a.cust_no 
         ,b.xref as ltd_46_cust_no
        from
        {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_INFO_VIEW_US') }}  a
        join  {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_XREF_US') }}   b
          on a.cust_no = b.cust_no
          and b.xref_type = 'LTD_CUST'
          and b.xref_no = '1'  )

, cust68 as (
        select 
          a.cust_no          
         ,b.xref as ltd_68_cust_no
        from
          {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_INFO_VIEW_US') }}  a
        join {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_XREF_US') }}  b
          on a.cust_no = b.cust_no
          and b.xref_type = 'LTD_CUST'
          and b.xref_no = '68'  )   
        

select 
'0100' as TUCSALESG
,'CIS' as SOURSYSTEM
,'A' as OBJVERS
,cis_cust.cust_no as CIS_CUST_NO
,cis_cust.cust_name as CIS_CUST_NAME
,cust46.ltd_46_cust_no
,cust68.ltd_68_cust_no
,mcust_name
,mcust_no
,division_desc as industry
,bill_to_cust_addr as address
,bill_to_cust_city as city
,bill_to_cust_state as state
,bill_to_cust_country as country
,bill_to_cust_zip as postal_cd
,bill_to_contact_phone as phone_no
,is_discontinued as deletion_ind
--,is_restricted
,cust_type_descr
,division_desc
from  {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_INFO_VIEW_US') }}  as cis_cust

left join cust46
  on cis_cust.cust_no = cust46.cust_no 
left join cust68
  on cis_cust.cust_no = cust68.cust_no