{{
     config(
         alias = 'sales_order_listing_full',
         materialized = 'table'

     )
 }}




with matl as (
select *    
from {{ ref('material_master') }} 
--from US_DATAPRACTICE.CORE.MATERIAL_MASTER
),

custtbl as (
    select *
    from {{ ref('customer_master') }}
    --from US_DATAPRACTICE.CORE.CUSTOMER_MASTER
  ),

cust as (
select distinct     
    cust.tnecusnu as TUCCUSTSS, 
    cust.tncbpcust
--from US_DATAPRACTICE.CDP.CUSTOMERMASTER_BASE_68  cust
from {{ source('us_cdp', 'CUSTOMERMASTER_BASE_68') }}  cust
join (select tnsaleorg,tnecusnu, max(createdon) createdon
      --from US_DATAPRACTICE.CDP.CUSTOMERMASTER_BASE_68 
      from {{ source('us_cdp', 'CUSTOMERMASTER_BASE_68') }} 
      where tnsaleorg = '1001'
      group by tnsaleorg,tnecusnu
      ) as max_cust
  on cust.tnecusnu = max_cust.tnecusnu
  and cust.tnsaleorg = max_cust.tnsaleorg
  and cust.createdon = max_cust.createdon
)
  
select
md5(concat(t1."/BIC/TUCDOCNUR", t1."/BIC/TUCSORDIX", t1.SOURSYSTEM)) as PKEY_SALES_ORDER_LISTING,
md5(t1."/BIC/TUCPROFIR") as FKEY_PROFIT_CENTER_MASTER,
md5(concat(t1.SOURSYSTEM, t1."/BIC/TUCSALESG", t1."/BIC/TUCDIVISN", t1."/BIC/TUCDISTRN", t1."/BIC/TUCSOLDTO")) as FKEY_CUSTOMER_MASTER,
md5(concat(t1.SOURSYSTEM,t1."/BIC/TUCMATERL", t1."/BIC/TUCSALESG", t1."/BIC/TUCDISTRN")) as FKEY_MATERIAL_MASTER,
t1."SOURSYSTEM" as SOURSYSTEM,
'US01' as COMPANY_CODE,
'US01' as SALES_ORG,
t1."/BIC/TUCDOCNUR" as SALES_DOC,
t1."/BIC/TUCSORDIX" as SALES_DOC_ITEM,
t1."/BIC/TUCDISTRN" as DISTR_CHANNEL,
t1."/BIC/TUCDIVISN" as DIVISION,
t1."/BIC/TUCDOCTYX" as SALES_DOC_TYPE,
t1."/BIC/TUCBSARK" as METHOD_ORDER_TAKING,
t1."/BIC/TUCSOLDTO" as RESELLER_ID,
t1."/BIC/TUCSOLDTO" as RESELLER_ID_46,
custtbl.RESELLER_ID_COMBINED as RESELLER_ID_COMBINED ,
cust.TNCBPCUST as RESELLER_ID_68,
custtbl.GROUPKEY as GROUPKEY,
t7."/BIC/TUCNAMEE" as RESELLER_NAME,
t7."/BIC/TUCACCNTP" as ACCNT_TYPE,
t1."/BIC/TUCCUSTG2" as CUST_GRP2,
t1."/BIC/TUCCUSTH1" as SUPER_SALES_AREA,
t1."/BIC/TUCCUSTH2" as SALES_AREA,
t1."/BIC/TUCCUSTH3" as SALES_EXECUTIVE,
t1."/BIC/TUCITUSER" as INTOUCH_ID,
t1."/BIC/TUDOERDAT" as SALES_ORDER_DATE,
t1."/BIC/TUDBILLDE" as BILL_DATE,
t1."/BIC/TUCMATERL" as MATERIAL_ID,
t1."/BIC/TUCMANUMR" as MFR_PART_NBR,
t1."/BIC/TUCPLANTT" as PLANT,
t1."/BIC/TUCPROFIR" as PROFIT_CENTER,
matl.GLBL_MFR as GLBL_MFR,
matl.PROD_FAMILY as PROD_FAMILY,
matl.PROD_CLASS as PROD_CLASS,
matl.PROD_SUBCLASS as PROD_SUBCLASS,
NULL as SBU_HRCHY_L0,
pc.SBU_HRCHY_L1 as SBU_HRCHY_L1,
pc.SBU_HRCHY_L2 as SBU_HRCHY_L2,
pc.SBU_HRCHY_L3 as SBU_HRCHY_L3,
pc.SBU_HRCHY_L4 as SBU_HRCHY_L4,
pc.SBU_HRCHY_L1_TXT as SBU_HRCHY_L1_TXT,
pc.SBU_HRCHY_L2_TXT as SBU_HRCHY_L2_TXT,
pc.SBU_HRCHY_L3_TXT as SBU_HRCHY_L3_TXT,
pc.SBU_HRCHY_L4_TXT as SBU_HRCHY_L4_TXT,
NULL as TM1_HRCHY_L1,
NULL as TM1_HRCHY_L2,
NULL as TM1_HRCHY_L3,
NULL as TM1_HRCHY_L4,
t1."/BIC/TUCREFERC" as REF_DOC_NBR,
t1."/BIC/TUCREFERM" as REF_DOC_ITEM,
t1."/BIC/TUCBSTNK" as CUST_PURCHASE_ORDER,
t1."/BIC/TUCREJECT" as REJECTION_STATUS,
t1."/BIC/TUCREASOJ" as REJECTION_REASON,
t1."/BIC/TUCDELBLK" as DELIVERY_BLOCK,
t1."/BIC/TUCLSSTA" as ORDER_STATUS,
t1."/BIC/TUCOVERAT" as OVERALL_STATUS,
t1."/BIC/TUCCUSTG3" as FREIGHT_COLUMN,
t1."/BIC/TUCPMNTTS" as PAYMENT_TERMS,
NULL as VOUCHER_VENDOR_ID,
NULL as VOUCHER_NBR,
NULL as VOUCHER_VALUE_DOC,
NULL as CUST_REBATE_FLAG,
t1."/BIC/TUKSB25L" as NSP_LC,
t1."/BIC/TUKSB49L" as SALES_ADJ_PROFIT_LC,
t1."/BIC/TUKSUB5L" as FRONT_END_PROFIT_LC,
t1."/BIC/TUKSUB5E" as FRONT_END_PROFIT_EUR,
t1."/BIC/TUKSUB6L" as NET_SALES_PROFIT_LC,
t1."/BIC/TUKSUB6E" as NET_SALES_PROFIT_EUR,
t1."/BIC/TUDCREATN" as CREATION_DATE,
t1."/BIC/TUCCREATY" as CREATED_BY,
bklg."/BIC/TUCUVALL" as HEADER_INCOMPL_STATUS,
bklg."/BIC/TUCSTSITM" as ITEM_INCOMPL_STATUS,
bklg."/BIC/TUCGBSTA" as ITEM_DELIVERY_STATUS,
case when bklg."/BIC/TUCDOCNUR" is not null 
  then TRUE 
  else FALSE 
end as OPEN_SO_FLAG,
nvl(bklg."/BIC/TUKOSOQTY",0) as SO_OPEN_QTY,
nvl(bklg."/BIC/TUKOSOVE",0) as SO_OPEN_NSP_EUR,
nvl(bklg."/BIC/TUKOSOVL",0) as SO_OPEN_NSP_LC,
SYSDATE() as UPDATE_DATE_UTC
from {{ source('us_cdp_bw_46','TUSOIO002') }} t1
--from ANALYTICS.EDW_SAP_BW_US_46.TUSOIO002 t1
Left join {{ source('us_cdp_bw_46','TUBKIO002') }} bklg
--left join ANALYTICS.EDW_SAP_BW_US_46.TUBKIO002 bklg
  on t1."/BIC/TUCDOCNUR" = bklg."/BIC/TUCDOCNUR"
  and t1."/BIC/TUCSORDIX" = bklg."/BIC/TUCSORDIX"
  
Left join {{ source('us_cdp_bw_46','TUCCUSTOR') }}   as t7
--left join ANALYTICS.EDW_SAP_BW_US_46.TUCCUSTOR as t7
   on t1."/BIC/TUCSOLDTO" = t7."/BIC/TUCCUSTOR" 
  and t7.SOURSYSTEM = 'A3'
  and t7."/BIC/TUCACCNTP" <> 'ZIC'
Left join cust
  on t1."/BIC/TUCSOLDTO" = cust.TUCCUSTSS  
Left join {{ ref('profit_center_master') }}  pc
--left join US_DATAPRACTICE.CORE.PROFIT_CENTER_MASTER  pc
  on t1."/BIC/TUCPROFIR" = pc.PROFIT_CENTER  
Left join matl
  on ltrim(t1."/BIC/TUCMATERL",'0') = ltrim(matl.material_id,'0')
  and t1.soursystem = matl.soursystem
  and t1."/BIC/TUCSALESG" = matl.sales_org  
  and matl.distr_channel = '00'   
left join custtbl 
  on t1.soursystem = custtbl.soursystem
  and t1."/BIC/TUCSALESG" = custtbl.legacy_sales_org
  and t1."/BIC/TUCSOLDTO" = custtbl.reseller_id
join {{ source('us_cdp_bw_46','TUCITEMCG') }} itemcg
--join ANALYTICS.EDW_SAP_BW_US_46.TUCITEMCG itemcg
  on t1."/BIC/TUCITEMCG" = itemcg."/BIC/TUCITEMCG"
  and itemcg."/BIC/TUCITEMCA" in('1', '3', '', NULL)
join {{ source('us_cdp_bw_46','TUCKTGRM') }} ktgrm
--join ANALYTICS.EDW_SAP_BW_US_46.TUCKTGRM ktgrm
  on t1."/BIC/TUCKTGRM" = ktgrm."/BIC/TUCKTGRM" 
  and ktgrm."/BIC/TUCKTGGR" = 'NSB'
join {{ source('us_cdp_bw_46','TUCDOCTYX') }} sd_typ
--join ANALYTICS.EDW_SAP_BW_US_46.TUCDOCTYX sd_typ
  on t1."/BIC/TUCDOCTYX" = sd_typ."/BIC/TUCDOCTYX"
  and sd_typ."/BIC/TUCDOCTP1" in('1','2','5', NULL, '')
where t1."/BIC/TUCACCNTN"  in('01', '', NULL)
  and t1."/BIC/TUCREASOJ" in('Z5', 'Y9', '', NULL)
  and t1."/BIC/TUCDIVISN" = '10'
  and t1."/BIC/TUCCOMPCE" = '0100'


-- 6.8
union all

select 
md5(concat(sd."/BIC/TND_NUMB", sd.S_ORD_ITEM, sd.SOURSYSTEM)) as PKEY_SALES_ORDER_LISTING,
md5(sd."/BIC/TNPROFITC") as FKEY_PROFIT_CENTER_MASTER,
md5(concat(sd.SOURSYSTEM, sd."/BIC/TNSALEORG", '00', '01', sd."/BIC/TNSOLDTO")) as FKEY_CUSTOMER_MASTER,
md5(concat(sd.SOURSYSTEM,sd."/BIC/TNMATERIL", sd."/BIC/TNSALEORG", '01')) as FKEY_MATERIAL_MASTER,
sd.SOURSYSTEM as SOURSYSTEM,
'US01' as COMPANY_CODE,
'US01' as SALES_ORG,
sd."/BIC/TND_NUMB" as SALES_DOC,
sd.S_ORD_ITEM as SALES_DOC_ITEM,
sd."/BIC/TNDISTNCH" as DISTR_CHANNEL,
sd."/BIC/TNDIVISIO" as DIVISION,
sd."/BIC/TNDOC_TYP" as SALES_DOC_TYPE,
sd."/BIC/TNBSARK" as METHOD_ORDER_TAKING,
sd."/BIC/TNSOLDTO" as RESELLER_ID,
custtbl.reseller_id_46 as RESELLER_ID_46,
custtbl.reseller_id_combined as RESELLER_ID_COMBINED,
sd."/BIC/TNSOLDTO" as RESELLER_ID_68,
custtbl.groupkey as GROUPKEY,
custtbl.reseller_name as RESELLER_NAME,
custtbl.customer_accnt_group as ACCNT_TYPE,
custtbl.cust_grp_2 as CUST_GRP2,
custtbl.super_sales_area as SUPER_SALES_AREA,
custtbl.sales_area as SALES_AREA,
custtbl.sales_executive as SALES_EXECUTIVE,
NULL as INTOUCH_ID,
sd."/BIC/TNCCRD_ON" as SALES_ORDER_DATE,
sd.BILL_DATE as BILL_DATE,
sd."/BIC/TNMATERIL" as MATERIAL_ID,
matl.MFR_PART_NBR as MFR_PART_NBR,
sd."/BIC/TNPLANT" as PLANT,
sd."/BIC/TNPROFITC" as PROFIT_CENTER,
matl.GLBL_MFR as GLBL_MFR,
matl.prod_family as PROD_FAMILY,
matl.prod_class as PROD_CLASS,
matl.prod_subclass as PROD_SUBCLASS,
NULL as SBU_HRCHY_L0,
NULL as SBU_HRCHY_L1,
NULL as SBU_HRCHY_L2,
NULL as SBU_HRCHY_L3,
NULL as SBU_HRCHY_L4,
NULL as SBU_HRCHY_L1_TXT,
NULL as SBU_HRCHY_L2_TXT,
NULL as SBU_HRCHY_L3_TXT,
NULL as SBU_HRCHY_L4_TXT,
NULL as TM1_HRCHY_L1,
NULL as TM1_HRCHY_L2,
NULL as TM1_HRCHY_L3,
NULL as TM1_HRCHY_L4,
sd."/BIC/TNREFER_D" as REF_DOC_NBR,
sd."/BIC/TNREFER_I" as REF_DOC_ITEM,
sd."/BIC/TNPOCUST" as CUST_PURCHASE_ORDER,
sd."/BIC/TNREJ_ST" as REJECTION_STATUS,
sd."/BIC/TNRSN_REJ" as REJECTION_REASON,
sd."/BIC/TNDEL_BLK" as DELIVERY_BLOCK,
sd.STS_DEL as ORDER_STATUS,
sd.STS_ITM as OVERALL_STATUS,
NULL as FREIGHT_COLUMN,
NULL as PAYMENT_TERMS,
NULL as VOUCHER_VENDOR_ID,
NULL as VOUCHER_NBR,
NULL as VOUCHER_VALUE_DOC,
NULL as CUST_REBATE_FLAG,
sd."/BIC/TNXRSLDC" as NSP_LC,
sd."/BIC/TNSAMLC" as SALES_ADJ_PROFIT_LC,
sd."/BIC/TNGPDC" as FRONT_END_PROFIT_LC,
sd."/BIC/TNGPUSD" as FRONT_END_PROFIT_EUR,
sd."/BIC/TNGPDC" as NET_SALES_PROFIT_LC,
sd."/BIC/TNGPUSD" as NET_SALES_PROFIT_EUR,
sd.CREATEDON as CREATION_DATE,
sd.CREATEDBY as CREATED_BY,
bklg."/BIC/TNOVRL_ST" as HEADER_INCOMPL_STATUS,
bklg."/BIC/TNLFSTA" as ITEM_INCOMPL_STATUS,
bklg."/BIC/TNLFGSA" as ITEM_DELIVERY_STATUS,
case when bklg."/BIC/TND_NUMB" is not null 
   then TRUE 
   else FALSE 
end as OPEN_SO_FLAG,
nvl(bklg."/BIC/TNOMENG",0) as SO_OPEN_QTY,
nvl(bklg."/BIC/TNXRSLDC",0) as SO_OPEN_NSP_EUR,
nvl(bklg."/BIC/TNXRSLDC",0) as SO_OPEN_NSP_LC,
SYSDATE() as UPDATE_DATE_UTC
from {{ source('us_cdp_bw_68','TNSOIO02') }} sd
--from ANALYTICS.EDW_SAP_BW_US_68.TNSOIO02 sd
Left join {{ source('us_cdp_bw_68','TNBCIO01') }} bklg
--left join ANALYTICS.EDW_SAP_BW_US_68.TNBCIO01 bklg
  on sd."/BIC/TND_NUMB" = bklg."/BIC/TND_NUMB"
  and sd.s_ord_item = bklg.s_ord_item
  and bklg."/BIC/AATRANTYP" = '004163'
left join matl
  on sd."/BIC/TNMATERIL" = matl.material_id
  and sd.soursystem = matl.soursystem
  and sd."/BIC/TNSALEORG" = matl.sales_org
left join custtbl
  on sd."/BIC/TNSOLDTO" = custtbl.reseller_id
  and sd.soursystem = custtbl.soursystem
  and sd."/BIC/TNSALEORG" = custtbl.legacy_sales_org
where sd."/BIC/TNSALEORG" in('1001')
  
union all

select 
md5(concat('CIS_US', lpad(to_char(inv.ORDER_NO), 38, '0'), lpad(to_char(inv.ORDER_LINE_NO),38,'0'), to_char(inv.ORDER_TYPE))) as PKEY_SALES_ORDER_LISTING,
md5(concat('CIS US', to_char(matl.vpl_no))) as FKEY_PROFIT_CENTER_MASTER,
md5(concat('CIS_US', to_char(inv.cust_no))) as FKEY_CUSTOMER_MASTER,
md5(concat('CIS_US', to_char(matl.sku_no))) as FKEY_MATERIAL_MASTER,
'CIS_US' as SOURSYSTEM,
'US01' as COMPANY_CODE,
'US01' as SALES_ORG,
to_char(inv.order_no) as SALES_DOC,
to_char(inv.order_line_no) as SALES_DOC_ITEM,
NULL as DISTR_CHANNEL,
NULL as DIVISION,
to_char(inv.ORDER_TYPE) as SALES_DOC_TYPE,
to_char(inv.from_ref_type) as METHOD_ORDER_TAKING,
to_char(inv.CUST_NO) as RESELLER,
NULL as RESELLER_ID_46,
custtbl.RESELLER_ID_COMBINED as RESELLER_ID_COMBINED,
NULL as RESELLER_ID_68,
custtbl.GROUPKEY as GROUPKEY,
to_char(inv.CUST_NAME) as RESELLER_NAME,
NULL as ACCNT_TYPE,
NULL as CUST_GRP2,
to_char(cust.division) as SUPER_SALES_AREA,
to_char(cust.cust_type) as SALES_AREA,
to_char(cust.sales_terr) as SALES_EXECUTIVE,
NULL as INTOUCH_ID,
to_char(inv.order_entry_datetime, 'YYYY-MM-DD') as SALES_ORDER_DATE,
inv.date_flag as BILL_DATE,
to_char(matl.sku_no) as MATERIAL_ID,
cte_matl.MFR_PART_NBR as MFR_PART_NBR,
to_char(inv.from_loc_no) as PLANT,
to_char(pc.LEVEL_4_NAME) as PROFIT_CENTER,
cte_matl.GLBL_MFR as GLBL_MFR,
cte_matl.prod_family as PROD_FAMILY,
cte_matl.prod_class as PROD_CLASS,
cte_matl.prod_subclass as PROD_SUBCLASS,
NULL as SBU_HRCHY_L0,
NULL as SBU_HRCHY_L1,
NULL as SBU_HRCHY_L2,
NULL as SBU_HRCHY_L3,
NULL as SBU_HRCHY_L4,
NULL as SBU_HRCHY_L1_TXT,
NULL as SBU_HRCHY_L2_TXT,
NULL as SBU_HRCHY_L3_TXT,
NULL as SBU_HRCHY_L4_TXT,
to_char(pc.SOLUTION_CODE) as TM1_HRCHY_L1,
to_char(pc.LEVEL_1_SEG_ID) as TM1_HRCHY_L2,
to_char(pc.LEVEL_2_SEG_ID) as TM1_HRCHY_L3,
to_char(pc.LEVEL_3_SEG_ID) as TM1_HRCHY_L4,
NULL as REF_DOC_NBR,
NULL as REF_DOC_ITEM,
NULL as CUST_PURCHASE_ORDER,
NULL as REJECTION_STATUS,
NULL as REJECTION_REASON,
NULL as DELIVERY_BLOCK,
NULL as ORDER_STATUS,
NULL as OVERALL_STATUS,
NULL as FREIGHT_COLUMN,
to_char(inv.terms) as PAYMENT_TERMS,
NULL as VOUCHER_VENDOR_ID,
NULL as VOUCHER_NBR,
NULL as VOUCHER_VALUE_DOC,
NULL as CUST_REBATE_FLAG,
to_char(inv.net_u_price*inv.ship_qty)  as NSP_LC,
NULL as SALES_ADJ_PROFIT_LC,
to_char(inv.net_u_price*inv.ship_qty) - to_char(inv.u_cost*inv.ship_qty) as FRONT_END_PROFIT_LC,
to_char(inv.net_u_price*inv.ship_qty) - to_char(inv.u_cost*inv.ship_qty) as FRONT_END_PROFIT_EUR,
to_char(inv.net_u_price*inv.ship_qty) - to_char(inv.u_cost*inv.ship_qty) as NET_SALES_PROFIT_LC,
to_char(inv.net_u_price*inv.ship_qty) - to_char(inv.u_cost*inv.ship_qty) as NET_SALES_PROFIT_EUR,
to_char(inv.order_entry_datetime, 'YYYY-MM-DD') as CREATION_DATE,
NULL as CREATED_BY,
NULL as HEADER_INCOMPL_STATUS,
NULL as ITEM_INCOMPL_STATUS,
NULL as ITEM_DELIVERY_STATUS,
NULL as OPEN_SO_FLAG,
0 as SO_OPEN_QTY,
0 as SO_OPEN_NSP_EUR,
0 as SO_OPEN_NSP_LC,
SYSDATE() as UPDATE_DATE_UTC
from {{ source('us_cdp_cis_us','DWD_DISTY_COMMON_SALES_DETAIL_DI_US') }}   inv
--from ANALYTICS.EDW_CIS_US.DWD_DISTY_COMMON_SALES_DETAIL_DI_US   inv
left join {{ source('us_cdp_cis_us','DM_PUB_PART_INFO_VIEW_US') }}   matl
--left join ANALYTICS.EDW_CIS_US.DM_PUB_PART_INFO_VIEW_US   matl
  on inv.sku_no = matl.sku_no 
left join {{ source('us_cdp_cis_us','DIM_PUB_BIZ_SEGMENT_HIERARCHY_US') }}  pc
--left join ANALYTICS.EDW_CIS_US.DIM_PUB_BIZ_SEGMENT_HIERARCHY_US  pc
  on matl.vpl_no = pc.vpl_no
left join {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_INFO_VIEW_US') }}  cust
--left join ANALYTICS.EDW_CIS_US.DIM_PUB_CUSTOMER_INFO_VIEW_US  cust
 on inv.cust_no = cust.cust_no
left join matl as cte_matl
  on to_char(matl.sku_no) = cte_matl.material_id
  and cte_matl.sales_org = 'CIS_US'
  and cte_matl.soursystem = 'CIS_US'
left join custtbl 
  on to_char(inv.cust_no) =  custtbl.reseller_id
  and custtbl.soursystem = 'CIS_US' 
where inv.order_type in('1', '125')


