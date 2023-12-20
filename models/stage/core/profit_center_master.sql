
{{
    config(
        alias = 'profit_center_master',
        materialized = 'table'

    )
}}

select
   md5(hier.prft_ctr) as PROFIT_CENTER_MASTER_KEY
   ,hier.prft_ctr as PROFIT_CENTER
   ,hier.ctrl_area as CONTROLLING_AREA
   ,hier.soursystem as SOURCE_SYSTEM
   ,NULL as FUNCTIONAL_AREA
   ,NULL as VENDOR_DIFFERENT
   ,NULL as GLBL_MFR
   ,NULL as SBU_HRCHY_L0
   ,hier.sbupctr_l1 as SBU_HRCHY_L1
   ,hier.sbupctr_l1_text as SBU_HRCHY_L1_TXT
   ,hier.sbupctr_l2 as SBU_HRCHY_L2
   ,hier.sbupctr_l2_text as SBU_HRCHY_L2_TXT
   ,hier.sbupctr_l3 as SBU_HRCHY_L3
   ,hier.sbupctr_l3_text as SBU_HRCHY_L3_TXT
   ,hier.sbupctr_l4 as SBU_HRCHY_L4
   ,hier.sbupctr_l4_text as SBU_HRCHY_L4_TXT
   ,NULL as TM1_HRCHY_L1
   ,NULL as TM1_HRCHY_L1_TEXT
   ,NULL as TM1_HRCHY_L2
   ,NULL as TM1_HRCHY_L2_TEXT
   ,NULL as TM1_HRCHY_L3
   ,NULL as TM1_HRCHY_L3_TEXT
   ,NULL as TM1_HRCHY_L4
   ,NULL as TM1_HRCHY_L4_TEXT 
   ,SYSDATE() as UPDATE_DATE_UTC
from  {{ source('us_cdp','PROFIT_CTR_HIER') }} hier

union all

select
    md5(concat('CIS US', to_char(VPL_NO))) as PROFIT_CENTER_MASTER_KEY
    , to_char(VPL_NO) as PROFIT_CENTER
    , NULL as CONTROLLING_AREA
    , 'CIS_US' as SOURSYSTEM
    , NULL as FUNCTIONAL_AREA
    , VPL_DESC as VENDOR_DIFFERENT
    , UNIVERSAL_VEND_NAME as GLBL_MFR
    , NULL as SBU_HRCHY_L0
    , SOLUTION_CODE as SBU_HRCHY_L1
    , SOLUTION_NAME as SBU_HRCHY_L1_TEXT
    , to_char(LEVEL_1_SEG_ID) as SBU_HRCHY_L2
    , LEVEL_1_NAME as SBU_HRCHY_L2_TEXT
    , to_char(LEVEL_2_SEG_ID) as SBU_HRCHY_L3
    , LEVEL_2_NAME as SBU_HRCHY_L3_TEXT
    , to_char(LEVEL_3_SEG_ID) as SBU_HRCHY_L4
    , LEVEL_3_NAME as SBU_HRCHY_L4_TXT
    , NULL as TM1_HRCHY_L1
    , NULL as TM1_HRCHY_L1_TEXT
    , NULL as TM1_HRCHY_L2
    , NULL as TM1_HRCHY_L2_TEXT
    , NULL as TM1_HRCHY_L3
    , NULL as TM1_HRCHY_L3_TEXT
    , NULL as TM1_HRCHY_L4
    , NULL as TM1_HRCHY_L4_TEXT
    , SYSDATE() as UPDATE_DATE_UTC
from {{source('us_cdp_cis_us','DIM_PUB_BIZ_SEGMENT_HIERARCHY_US')}}