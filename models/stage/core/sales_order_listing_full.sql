{{
      config(
          alias = 'sales_order_listing_full',
          materialized = 'table'

      )
 }}




with matl as (
select *    
from {{ ref('material_master') }} 
--from US_DATAPRACTICE_DEV.CORE.MATERIAL_MASTER
),

custtbl as (
    select *
    from {{ ref('customer_master') }}
    --from US_DATAPRACTICE_DEV.CORE.CUSTOMER_MASTER
  ),

cust as (
select distinct     
    cust.tnecusnu as TUCCUSTSS, 
    cust.tncbpcust, cust.tnsaleorg
--from US_DATAPRACTICE.CDP.CUSTOMERMASTER_BASE_68  cust
from {{ source('us_cdp', 'CUSTOMERMASTER_BASE_68') }}  cust
join (select tnsaleorg,tnecusnu, max(createdon) createdon
      --from US_DATAPRACTICE.CDP.CUSTOMERMASTER_BASE_68 
      from {{ source('us_cdp', 'CUSTOMERMASTER_BASE_68') }} 
      where tnsaleorg in( '1001','1002')
      group by tnsaleorg,tnecusnu
      ) as max_cust
  on cust.tnecusnu = max_cust.tnecusnu
  and cust.tnsaleorg = max_cust.tnsaleorg
  and cust.createdon = max_cust.createdon
),

bklg_68 as (select "/BIC/TND_NUMB", 
                    S_ORD_ITEM, 
                    case when "/BIC/TNOVRL_ST" = 'B'
                      then 'A'
                      else "/BIC/TNOVRL_ST"
                    end as "/BIC/TNOVRL_ST",
                    "/BIC/TNLFSTA", 
                    "/BIC/TNLFGSA", 
                    sum("/BIC/TNOMENG") "/BIC/TNOMENG",
                    sum("/BIC/TNXRSLDC") "/BIC/TNXRSLDC"
            from {{ source('us_cdp_bw_68','TNBCIO01') }}
             --from ANALYTICS.EDW_SAP_BW_US_68.TNBCIO01 
             where "/BIC/AATRANTYP" = '004163'
               and "/BIC/TNOVRL_ST" <> 'C'              
             group by all
        )


        
select 
md5(concat(t1."/BIC/TUCDOCNUR", t1."/BIC/TUCSORDIX", t1.SOURSYSTEM)) as PKEY_SALES_ORDER_LISTING,
md5(t1."/BIC/TUCPROFIR") as FKEY_PROFIT_CENTER_MASTER,
--md5(concat(t1.SOURSYSTEM, 'US01', t1."/BIC/TUCDIVISN", t1."/BIC/TUCDISTRN", t1."/BIC/TUCSOLDTO")) as FKEY_CUSTOMER_MASTER,
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
custtbl.reseller_id_46 as RESELLER_ID_46,
custtbl.reseller_id_combined as RESELLER_ID_COMBINED,
custtbl.reseller_id_68 as RESELLER_ID_68,
custtbl.groupkey as GROUPKEY,
custtbl.reseller_name as RESELLER_NAME,
custtbl.customer_accnt_group as ACCNT_TYPE,
custtbl.cust_grp_2 as CUST_GRP2,
custtbl.super_sales_area as SUPER_SALES_AREA,
custtbl.sales_area as SALES_AREA,
custtbl.sales_executive as SALES_EXECUTIVE,
t1."/BIC/TUCITUSER" as INTOUCH_ID,

CASE
  WHEN t1."/BIC/TUDOERDAT" IS NULL OR t1."/BIC/TUDOERDAT" = '' OR t1."/BIC/TUDOERDAT" = '00000000' THEN NULL
  ELSE TO_DATE(TO_CHAR(TO_DATE(t1."/BIC/TUDOERDAT", 'YYYYMMDD'), 'YYYY-MM-DD'))
END AS SALES_ORDER_DATE,

CASE
  WHEN t1."/BIC/TUDBILLDE" IS NULL OR t1."/BIC/TUDBILLDE" = '' OR t1."/BIC/TUDBILLDE" = '00000000' THEN NULL
  ELSE TO_DATE(TO_CHAR(TO_DATE(t1."/BIC/TUDBILLDE", 'YYYYMMDD'), 'YYYY-MM-DD'))
END AS BILL_DATE,

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

CASE
  WHEN t1."/BIC/TUDCREATN" IS NULL OR t1."/BIC/TUDCREATN" = '' OR t1."/BIC/TUDCREATN" = '00000000' THEN NULL
  ELSE TO_DATE(TO_CHAR(TO_DATE(t1."/BIC/TUDCREATN", 'YYYYMMDD'), 'YYYY-MM-DD'))
END AS CREATION_DATE,

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
t1."/BIC/TUCSALESG" as LEGACY_SLS_ORG,
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
--Left join cust
 -- on t1."/BIC/TUCSOLDTO" = cust.TUCCUSTSS  
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

union all

select 
md5(concat(sd."/BIC/TND_NUMB", sd.S_ORD_ITEM, sd.SOURSYSTEM)) as PKEY_SALES_ORDER_LISTING,
md5(sd."/BIC/TNPROFITC") as FKEY_PROFIT_CENTER_MASTER,
md5(concat(sd.SOURSYSTEM, sd."/BIC/TNSALEORG", '00', '01', sd."/BIC/TNSOLDTO")) as FKEY_CUSTOMER_MASTER,
md5(concat(sd.SOURSYSTEM,sd."/BIC/TNMATERIL", sd."/BIC/TNSALEORG", '01')) as FKEY_MATERIAL_MASTER,
sd.SOURSYSTEM as SOURSYSTEM,
IFF(sd."/BIC/TNSALEORG" in('1002', 'A002'), 'CA01', 'US01') as COMPANY_CODE,
IFF(sd."/BIC/TNSALEORG" in('1002', 'A002'), 'CA01', 'US01') as SALES_ORG,
sd."/BIC/TND_NUMB" as SALES_DOC,
sd.S_ORD_ITEM as SALES_DOC_ITEM,
sd."/BIC/TNDISTNCH" as DISTR_CHANNEL,
sd."/BIC/TNDIVISIO" as DIVISION,
sd."/BIC/TNDOC_TYP" as SALES_DOC_TYPE,
sd."/BIC/TNBSARK" as METHOD_ORDER_TAKING,
sd."/BIC/TNSOLDTO" as RESELLER_ID,
custtbl.reseller_id_46 as RESELLER_ID_46,
custtbl.reseller_id_combined as RESELLER_ID_COMBINED,
custtbl.reseller_id_68 as RESELLER_ID_68,
custtbl.groupkey as GROUPKEY,
custtbl.reseller_name as RESELLER_NAME,
custtbl.customer_accnt_group as ACCNT_TYPE,
custtbl.cust_grp_2 as CUST_GRP2,
custtbl.super_sales_area as SUPER_SALES_AREA,
custtbl.sales_area as SALES_AREA,
custtbl.sales_executive as SALES_EXECUTIVE,
NULL as INTOUCH_ID,

CASE
  WHEN sd."/BIC/TNCCRD_ON" IS NULL OR sd."/BIC/TNCCRD_ON" = '' OR sd."/BIC/TNCCRD_ON" = '00000000' THEN NULL
  ELSE TO_DATE(TO_CHAR(TO_DATE(sd."/BIC/TNCCRD_ON", 'YYYYMMDD'), 'YYYY-MM-DD'))
END AS SALES_ORDER_DATE,

CASE
  WHEN sd.BILL_DATE IS NULL OR sd.BILL_DATE = '' OR sd.BILL_DATE = '00000000' THEN NULL
  ELSE TO_DATE(TO_CHAR(TO_DATE(sd.BILL_DATE, 'YYYYMMDD'), 'YYYY-MM-DD'))
END AS BILL_DATE,

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

CASE
  WHEN sd.CREATEDON IS NULL OR sd.CREATEDON = '' OR sd.CREATEDON = '00000000' THEN NULL
  ELSE TO_DATE(TO_CHAR(TO_DATE(sd.CREATEDON, 'YYYYMMDD'), 'YYYY-MM-DD'))
END AS CREATION_DATE,

sd.CREATEDBY as CREATED_BY,
bklg_68."/BIC/TNOVRL_ST" as HEADER_INCOMPL_STATUS,
bklg_68."/BIC/TNLFSTA" as ITEM_INCOMPL_STATUS,
bklg_68."/BIC/TNLFGSA" as ITEM_DELIVERY_STATUS,
case when bklg_68."/BIC/TND_NUMB" is not null 
   then TRUE 
   else FALSE 
end as OPEN_SO_FLAG,
nvl(bklg_68."/BIC/TNOMENG",0) as SO_OPEN_QTY,
nvl(bklg_68."/BIC/TNXRSLDC",0) as SO_OPEN_NSP_EUR,
nvl(bklg_68."/BIC/TNXRSLDC",0) as SO_OPEN_NSP_LC,
SD."/BIC/TNSALEORG" as LEGACY_SLS_ORG,
SYSDATE() as UPDATE_DATE_UTC
from {{ source('us_cdp_bw_68','TNSOIO02') }} sd
--from ANALYTICS.EDW_SAP_BW_US_68.TNSOIO02 sd
left join bklg_68
  on sd."/BIC/TND_NUMB" = bklg_68."/BIC/TND_NUMB"
  and sd.s_ord_item = bklg_68.s_ord_item  
left join matl
  on sd."/BIC/TNMATERIL" = matl.material_id
  and sd.soursystem = matl.soursystem
  and sd."/BIC/TNSALEORG" = matl.sales_org
left join custtbl
  on sd."/BIC/TNSOLDTO" = custtbl.reseller_id
  and sd.soursystem = custtbl.soursystem
  and sd."/BIC/TNSALEORG" = custtbl.legacy_sales_org
where sd."/BIC/TNSALEORG" in('1001', '1002')

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
custtbl.RESELLER_ID_46 RESELLER_ID_46,
custtbl.RESELLER_ID_COMBINED as RESELLER_ID_COMBINED,
custtbl.RESELLER_ID_68 as RESELLER_ID_68,
custtbl.GROUPKEY as GROUPKEY,
custtbl.RESELLER_NAME as RESELLER_NAME,
custtbl.customer_accnt_group as ACCNT_TYPE,
NULL as CUST_GRP2,
custtbl.SUPER_SALES_AREA as SUPER_SALES_AREA,
custtbl.SALES_AREA as SALES_AREA,
custtbl.SALES_EXECUTIVE as SALES_EXECUTIVE,
NULL as INTOUCH_ID,
to_char(inv.order_entry_datetime, 'YYYY-MM-DD') as SALES_ORDER_DATE,
to_char(inv.date_flag,'YYYY-MM-DD') as BILL_DATE,
cte_matl.MATERIAL_ID as MATERIAL_ID,
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
case when bklg.order_no is not null 
   then TRUE 
   else FALSE 
end as OPEN_SO_FLAG,
round(bklg.open_qty,0) as SO_OPEN_QTY,
round(bklg.extend_net_price,2) as SO_OPEN_NSP_EUR,
round(bklg.extend_net_price,2) as SO_OPEN_NSP_LC,
'CIS_US' as LEGACY_SLS_ORG,
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
left join {{ source('us_cdp_cis_us','DWD_DISTY_SALES_OPEN_ORDER_DETAIL_US') }}   bklg
--left join ANALYTICS.EDW_CIS_US.DWD_DISTY_SALES_OPEN_ORDER_DETAIL_US bklg
  on inv.order_no = bklg.order_no
  and inv.order_line_no = bklg.order_line_no
  and inv.order_type = bklg.order_type
where inv.order_type in('1', '125')

union all

select 
md5(concat('CIS_CA', lpad(to_char(inv.ORDER_NO), 38, '0'), lpad(to_char(inv.ORDER_LINE_NO),38,'0'), to_char(inv.ORDER_TYPE))) as PKEY_SALES_ORDER_LISTING,
md5(concat('CIS CA', to_char(matl.vpl_no))) as FKEY_PROFIT_CENTER_MASTER,
md5(concat('CIS_CA', to_char(inv.cust_no))) as FKEY_CUSTOMER_MASTER,
md5(concat('CIS_CA', to_char(inv.sku_no))) as FKEY_MATERIAL_MASTER,
'CIS_CA' as SOURSYSTEM,
'CA01' as COMPANY_CODE,
'CA01' as SALES_ORG,
to_char(inv.order_no) as SALES_DOC,
to_char(inv.order_line_no) as SALES_DOC_ITEM,
NULL as DISTR_CHANNEL,
NULL as DIVISION,
to_char(inv.ORDER_TYPE) as SALES_DOC_TYPE,
to_char(inv.from_ref_type) as METHOD_ORDER_TAKING,
to_char(inv.CUST_NO) as RESELLER,
custtbl.RESELLER_ID_46 RESELLER_ID_46,
custtbl.RESELLER_ID_COMBINED as RESELLER_ID_COMBINED,
custtbl.RESELLER_ID_68 as RESELLER_ID_68,
custtbl.GROUPKEY as GROUPKEY,
custtbl.RESELLER_NAME as RESELLER_NAME,
custtbl.customer_accnt_group as ACCNT_TYPE,
NULL as CUST_GRP2,
custtbl.SUPER_SALES_AREA as SUPER_SALES_AREA,
custtbl.SALES_AREA as SALES_AREA,
custtbl.SALES_EXECUTIVE as SALES_EXECUTIVE,
NULL as INTOUCH_ID,
to_char(inv.order_entry_datetime, 'YYYY-MM-DD') as SALES_ORDER_DATE,
to_char(inv.date_flag,'YYYY-MM-DD') as BILL_DATE,
to_char(inv.sku_no) as MATERIAL_ID,
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
to_char((inv.net_u_price*inv.ship_qty)/fx.rate) - to_char((inv.u_cost*inv.ship_qty)/fx.rate) as FRONT_END_PROFIT_EUR,
to_char(inv.net_u_price*inv.ship_qty) - to_char(inv.u_cost*inv.ship_qty) as NET_SALES_PROFIT_LC,
to_char((inv.net_u_price*inv.ship_qty)/fx.rate) - to_char((inv.u_cost*inv.ship_qty)/fx.rate) as NET_SALES_PROFIT_EUR,
to_char(inv.order_entry_datetime, 'YYYY-MM-DD') as CREATION_DATE,
NULL as CREATED_BY,
NULL as HEADER_INCOMPL_STATUS,
NULL as ITEM_INCOMPL_STATUS,
NULL as ITEM_DELIVERY_STATUS,
case when bklg.order_no is not null 
   then TRUE 
   else FALSE 
end as OPEN_SO_FLAG,
round(bklg.open_qty,0) as SO_OPEN_QTY,
round((bklg.extend_net_price/fx.rate),2) as SO_OPEN_NSP_EUR,
round(bklg.extend_net_price,2) as SO_OPEN_NSP_LC,
'CIS_CA' as LEGACY_SLS_ORG,
SYSDATE() as UPDATE_DATE_UTC
from {{ source('ca_cdp_cis_ca','DWD_DISTY_COMMON_SALES_DETAIL_DI_CA') }}   inv
--from ANALYTICS.EDW_CIS_CA.DWD_DISTY_COMMON_SALES_DETAIL_DI_CA   inv
left join {{ source('ca_cdp_cis_ca','DM_PUB_PART_INFO_VIEW_CA') }}   matl
--left join ANALYTICS.EDW_CIS_CA.DM_PUB_PART_INFO_VIEW_CA   matl
  on inv.sku_no = matl.sku_no 
left join {{ source('ca_cdp_cis_ca','DIM_PUB_BIZ_SEGMENT_HIERARCHY_CA') }}  pc
--left join ANALYTICS.EDW_CIS_CA.DIM_PUB_BIZ_SEGMENT_HIERARCHY_CA  pc
  on matl.vpl_no = pc.vpl_no
--left join {{ source('ca_cdp_cis_ca','DIM_PUB_CUSTOMER_INFO_VIEW_CA') }}  cust
-- left join ANALYTICS.EDW_CIS_CA.DIM_PUB_CUSTOMER_INFO_VIEW_CA  cust
--  on inv.cust_no = cust.cust_no
left join matl as cte_matl
  on to_char(matl.sku_no) = cte_matl.material_id
  and cte_matl.sales_org = 'CIS_CA'
  and cte_matl.soursystem = 'CIS_CA'
left join custtbl 
  on to_char(inv.cust_no) =  custtbl.reseller_id
  and custtbl.soursystem = 'CIS_CA' 
--left join ANALYTICS.EDW_CIS_CA.DM_PUB_EXCHANGE_RATE_VIEW_CA fx
left join {{ source('ca_cdp_cis_ca','DM_PUB_EXCHANGE_RATE_VIEW_CA') }}   fx
  on inv.date_flag = fx.date_flag
  and fx.local_currency = 'CAD'
--left join ANALYTICS.EDW_CIS_CA.DWD_DISTY_SALES_OPEN_ORDER_DETAIL_CA bklg
left join {{ source('ca_cdp_cis_ca','DWD_DISTY_SALES_OPEN_ORDER_DETAIL_CA') }}   bklg
  on inv.order_no = bklg.order_no
  and inv.order_line_no = bklg.order_line_no
  and inv.order_type = bklg.order_type
where inv.order_type in('1', '125')



