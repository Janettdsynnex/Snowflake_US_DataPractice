{{
    config(
        alias = 'profit_center_master_view'

    )
}}


select
	md5(hier.prft_ctr) as pkey_profit_center
	,hier.prft_ctr
	,hier.ctrl_area
	,hier.soursystem
	,NULL as TUCFUNCAA
	,NULL as TUCVENDIF
	,NULL as TUCZZGMNR

    ,NULL as SBU
	-- ,case when HIERARCHY.TECPCTRL1 = 'TDEUTOT_GCC' then 'GCC'   
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_PCC' then 'GCC'
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_VAD' then 'AS'               
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_SPS' and HIERARCHY.TECPCTRL2 = 'TDEUBRD_MAV' then 'MAVERICK'  
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_MAV' then 'MAVERICK'
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_SPS' and HIERARCHY.TECPCTRL2 = 'TDEUTOT_CAD' then 'DATECH' 
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_DAT' then 'DATECH'
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_MOCE' then 'ES'               
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_PER' then 'ES'                
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_PRI' then 'ES'               
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_SUP' then 'ES'                
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_PCS' then 'ES'                
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_EPS' then 'ES'                
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_APPLE' then 'ES' 
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_BRO' then 'GBU'
	-- 	when HIERARCHY.TECPCTRL1 = '' then 'OTHERS'                
	-- 	when HIERARCHY.TECPCTRL1 = ' ' then 'OTHERS'                
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_CCTR' then 'OTHERS'                
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_WWHQ' then 'OTHERS'                
	-- 	when HIERARCHY.TECPCTRL1 = 'TDEUTOT_SVC' then 'OTHERS'                
	-- 	else null
	-- 	end as SBU
		
	,hier.sbupctr_l1 as PC_SBU_LVL1
	,hier.sbupctr_l1_text as PC_SBU_LVL1_TEXT
	,hier.sbupctr_l2 as PC_SBU_LVL2
	,hier.sbupctr_l2_text as PC_SBU_LVL2_TEXT
	,hier.sbupctr_l3 as PC_SBU_LVL3
	,hier.sbupctr_l3_text as PC_SBU_LVL3_TEXT
	,hier.sbupctr_l4 as PC_SBU_LVL4
	,hier.sbupctr_l4_text as PC_SBU_LVL4_TEXT
	
	,NULL as PC_TM1_LVL1
	,NULL as PC_TM1_LVL1_TEXT
	,NULL as PC_TM1_LVL2
	,NULL as PC_TM1_LVL2_TEXT
	,NULL as PC_TM1_LVL3
	,NULL as PC_TM1_LVL3_TEXT
	,NULL as PC_TM1_LVL4
	,NULL as PC_TM1_LVL4_TEXT 
	,SYSDATE() as UPDATE_DATE_UTC
FROM {{ ref('profit_ctr_hier') }} hier
