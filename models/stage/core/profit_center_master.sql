
{{
    config(
        alias = 'profit_center_master',
        materialized = 'table'

    )
}}



select
               md5(hier.prft_ctr) as profit_center_master_key
               ,hier.prft_ctr as PROFIT_CENTER
               ,hier.ctrl_area as CONTROLLING_AREA
               ,hier.soursystem as SOURCE_SYSTEM
               ,NULL as FUNCTIONAL_AREA
               ,NULL as VENDOR_DIFFERENT
               ,NULL as GLBL_MFR

    ,NULL as SBU_HRCHY_L0
               -- ,case when HIERARCHY.TECPCTRL1 = 'TDEUTOT_GCC' then 'GCC'   
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_PCC' then 'GCC'
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_VAD' then 'AS'               
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_SPS' and HIERARCHY.TECPCTRL2 = 'TDEUBRD_MAV' then 'MAVERICK'  
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_MAV' then 'MAVERICK'
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_SPS' and HIERARCHY.TECPCTRL2 = 'TDEUTOT_CAD' then 'DATECH' 
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_DAT' then 'DATECH'
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_MOCE' then 'ES'               
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_PER' then 'ES'                
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_PRI' then 'ES'               
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_SUP' then 'ES'                
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_PCS' then 'ES'                
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_EPS' then 'ES'                
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_APPLE' then 'ES' 
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_BRO' then 'GBU'
               --            when HIERARCHY.TECPCTRL1 = '' then 'OTHERS'                
               --            when HIERARCHY.TECPCTRL1 = ' ' then 'OTHERS'                
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_CCTR' then 'OTHERS'                
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_WWHQ' then 'OTHERS'                
               --            when HIERARCHY.TECPCTRL1 = 'TDEUTOT_SVC' then 'OTHERS'                
               --            else null
               --            end as SBU
                              
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
FROM  {{ ref('profit_ctr_hier') }} hier


