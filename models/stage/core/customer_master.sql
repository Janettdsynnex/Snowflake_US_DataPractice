{{
      config(
          alias = 'customer_master',
          materialized = 'table'
      )
  }}



with cust_xref as (
       
        select xref.*, to_char(cust.mcust_no) mcust_no
        from (
            select to_char(cust_no) cust_no, 
                   xref,  
                   xref_no,
                   ROW_NUMBER() OVER (PARTITION BY xref ORDER BY entry_datetime desc) as rn
                from {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_XREF_US') }} a
                --from ANALYTICS.EDW_CIS_US.DIM_PUB_CUSTOMER_XREF_US a
                where xref_type = 'LTD_CUST'                
                      ) xref
        left join {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_INFO_VIEW_US') }} cust
        --left join ANALYTICS.EDW_CIS_US.DIM_PUB_CUSTOMER_INFO_VIEW_US cust
          on xref.cust_no = cust.cust_no
        where rn = 1)        
               

     ,cust68 as (
         select 
            "/BIC/TNCBPCUST",
            "/BIC/TNGLBCUST",
            case when length("/BIC/TNECUSNU") = 0 then "/BIC/TNSERTRM1"
              else "/BIC/TNECUSNU"
            end as TNECUSNU
         from {{ source('us_cdp_bw_68','TNCBPCUST') }} 
         --from ANALYTICS.EDW_SAP_BW_US_68.TNCBPCUST
         where SOURSYSTEM = 'A2'
               )

SELECT 
MD5(CONCAT(T1.SOURSYSTEM,T1.TNSALEORG,'00','01',T1.TNCBPCUST)) AS CUSTOMER_MASTER_KEY,  
T1.SOURSYSTEM AS SOURSYSTEM,
'US01' AS SALES_ORG,
'US01' AS COMPANY_CODE,  
'00' AS DIVISION,
'01' AS DISTR_CHANNEL,
T1.TNCBPCUST AS RESELLER_ID,
T1.TUCCUSTOR_46 AS RESELLER_ID_46,
nvl(cust_xref.mcust_no, T1.TNCBPCUST) as RESELLER_ID_COMBINED,
T1.TNCBPCUST AS RESELLER_ID_68 ,
NULL AS ACCOUNT_CLOSURE_FLAG,
IFF(T1.TNCDELINDC = 'X' OR T7.DEL_INDIC = 'X' , TRUE, FALSE) AS DELETION_FLAG_MAIN, 
NULL AS LAST_PACE_FLAG,
NULL AS PACE_FLAG_UPDATE,
NULL AS LAST_PACE_CLUSTER,
NULL AS LAST_CE_SEGMENT_TEXT,
NULL AS LAST_CE_SEGMENT_ID,
NULL AS CE_PERIOD_START,
NULL AS CE_PERIOD_END,
NULL AS LAST_LCS_STAGE,
NULL AS LAST_LCS_STAGE_TEXT,
NULL AS LAST_LCS_STAGE_UPDATE,
NULL AS LAST_LCS_STAGE_TEXT_CISCO,
NULL AS LAST_LCS_STAGE_UPDATE_CISCO,
NULL AS TS_CUST_ACCOUNT,
nvl(cust_xref.mcust_no, nvl(cust46."/BIC/TUCHIEC03", cust68."/BIC/TNGLBCUST")) as GROUPKEY,
NULL AS DNB_DUNS_NBR,
NULL AS DNB_CONFIDENCE_CODE,
NULL AS DNB_NAME_SCORE,
NULL AS DNB_CITY_SCORE,
NULL AS DNB_STATE_SCORE,
NULL AS DNB_STRNAME_SCORE,
NULL AS DNB_STRNUMB_SCORE,
NULL AS DNB_PHONE_SCORE,
NULL AS DNB_POBOX_SCORE,
T1.TNCCUSTYP as CUSTOMER_ACCNT_GROUP,
NULL AS ADDRESS,
T1.TNCCITY AS CITY,
T1.TNCCOUNTRY AS COUNTRY_KEY, 
NULL AS CUST_CLASSIFICATION,
NULL AS COUNTY_CODE,  
NULL AS CUSTOMER_IS_CONSUMER,
T1.TNCDELINDC AS DELETION_FLAG,
NULL AS DUNS_NBR,
NULL AS FAX_NBR,
T1.TUCINDUSY AS INDUSTRY_KEY,
T6."/BIC/TUCINDUSY" AS INDUSTRY_KEY_TEXT,
NULL AS INDUSTRY_CODE_1,
NULL AS INDUSTRY_CODE_2,
NULL AS INDUSTRY_CODE_3,
NULL AS INDUSTRY_CODE_4,
NULL AS INDUSTRY_CODE_5,
NULL AS LANGUAGE_KEY,
T1.TNCNAME AS RESELLER_NAME,
NULL AS NAME_2,
NULL AS NAME_3,
NULL AS NIELSEN_ID,
NULL AS TRADING_PARTNER,
T1.TNCTELNUM AS FIRST_TELEPHONE_NBR,
NULL AS PLANT,
NULL AS PO_BOX,
T1.TNCPOSTLCD AS POSTAL_CODE,
NULL AS CAM_PO_BOX,
NULL AS CAM_POSTAL_CODE_GEO,
T1.TNCREGION AS REGION,
T1.TNCSTREET AS HOUSE_NBR_AND_STR ,
NULL AS SORT_FIELD,
NULL AS TAX_NBR_1 ,
NULL AS TAX_NBR_2,
NULL AS MATERIAL_USAGE_IND,
NULL AS VENDOR,
NULL AS ORDER_BLOCK,
NULL AS INTL_LOCATION_NBR_1,
NULL AS INTL_LOCATION_NBR_2,
NULL AS CHECK_DIGIT_INTL_LOC,
NULL AS CUST_EMAIL_ADDRESS,

CASE  
  WHEN TO_CHAR(t1.CREATEDON) IN(NULL, '', '00000000') THEN NULL
ELSE TO_DATE(TO_CHAR(T1.CREATEDON, 'YYYY-MM-DD'))
END AS CREATION_DATE,

NULL AS BILL_BLOCK,
NULL AS LEGAL_STATUS,
NULL AS ASGMT_TO_HRCHY,
NULL AS DELIVERY_WAVER,
NULL AS LEASING_COMPANY,
NULL AS ECOM_DISCOUNT_GROUP,
NULL AS INTEREST_GROUP,
NULL AS DELIVERY_BLOCK,
NULL AS TRANSPORTATION_ZONE,
NULL AS NAME_4,
NULL AS DISTRICT ,
NULL AS PO_BOX_CITY,
NULL AS VAT_REGISTRATION_NBR,
NULL AS TELEBOX_NBR,
NULL AS FAX_NBR_2,
NULL AS CUST_COMM_TELETEX,
NULL AS VAT_COUNTRY_KEY,
NULL AS GLBL_MFR,
NULL AS STREET_4,
NULL AS DATA_LINE,
NULL AS ANNUAL_SALES,
NULL AS CURRENCY_SALES_FIG ,
NULL AS FISCAL_YEAR_VARIANT,
NULL AS FLAG_NON_VAT_REG,
NULL AS EUROPEAN_CUST_GROUP,
NULL AS CUST_GLBL_ACCNT,
NULL AS CUST_LOCAL_GRP,
NULL AS BUSINESS_PARTNER,
NULL AS MIN_QTY_SURCHARGE_GR,
NULL AS HUNTING_STATUS,
NULL AS TAX_JURISDICTION_COD,
NULL AS TELEPHONE_EXTENSION,
NULL AS TAX_CODE_3,
NULL AS AUTHORIZATION_GRP,
NULL AS DOING_BUSINESS_AS_1,
NULL AS DOING_BUSINESS_AS_2,
NULL AS DATA_MEDIUM_EXCHANGE,
NULL AS CUST_CLASS_SUPPLIES,
NULL AS VENDOR_CLASS_NBR,
NULL AS VENDOR_CLASS,
T1.TNCBPCUST AS CUST_NBR_COMP_CODE,
NULL AS DUNNING_CLERK,
NULL AS ACCNT_NBR_DUN_RECIP,
NULL AS CREDIT_CONTROL_AREA_CC, 
NULL AS DELETION_FLAG_CC,
NULL AS EMPLOYEE,
NULL AS PAYMENT_TERMS_CC,
NULL AS PAYMENT_BLOCK_KEY,
NULL AS PAYMENT_METHOD_SUPPL,
NULL AS PREV_MASTER_REC_NBR,
NULL AS ACCNT_CLERK,
NULL AS BLOCK_BY_CRED_MNGMT,
NULL AS RISK_CTGRY_NEW_CUST,
NULL AS CUST_RATING,
NULL AS LEGAL_DUNN_PROCEED,
NULL AS CUST_CREDIT_GRP,
NULL AS HEAD_OFFICE_ACCNT_NBR,
NULL AS CREDIT_MEMO_TERMS,
NULL AS COLLECTIVE_INV_VAR,
NULL AS CREDIT_MGMT_REPS_GRP,
NULL AS TOLERANCE_GRP,
NULL AS INS_VALIDITY_DATE,
NULL AS INS_INSTITUTION_NBR,
NULL AS INS_NBR,
NULL AS INTEREST_CALC_NBR,
NULL AS EXCH_RATE_PAYM_TERMS,
NULL AS BANK_STATEMENT,
NULL AS PAYMENT_METHODS,
NULL AS CREDIT_MEMO_PAYM_TERM,
NULL AS INTERNET_ADDRESS,
NULL AS CUST_CREDIT_LIMIT,
NULL AS CREDIT_EXPOSURE_HORZ,
NULL AS NBR_OF_RECORDS_CC,
NULL AS ACCNT_BALANCE,
NULL AS AMOUNT_INSURED,
NULL AS TOT_BANK_GUARANTEES,
NULL AS LAST_GEN_INFO,
NULL AS CURRENCY_KEY_AMNT_INS,
NULL AS CURRENCY_KEY_CC,
NULL AS RECONCILIATION_ACCNT,
NULL AS CHART_OF_ACCNTS,
NULL AS CUST_ACCNT_NBR_CL_REF,
NULL AS CASH_MGMT_GRP,
NULL AS CREDIT_MANAGER,
NULL AS GRP_RELATIONSHIP,
NULL AS AUTHORIZATION_GRP_CC,
NULL AS SORTING_KEY,
NULL AS OLD_CUST_NBR,
NULL AS DUNNING_PROCEDURE,
NULL AS KEY_OF_THE_LOCKBOX,
NULL AS CREDIT_PARENT_ACCNT,
NULL AS NEXT_INTERNAL_REVIEW,
NULL AS DATE_OF_LAST_DUNNING,
NULL AS DUNNING_LEVEL,
NULL AS DUNNING_BLOCK,
T1.TNCBPCUST AS CUST_SALES,
NULL AS LAST_BILL_DATE,
NULL AS VENDOR_CLASS_SS,
NULL AS VENDOR_CLASS_NBR_SS,
T1.TNCCUSTYP AS CUST_ACCNT_ASGMT_GRP,
NULL AS CUST_CLASS_ABC,
NULL AS CUST_GRP,
NULL AS PROFILE_COLUMN_CUST,
NULL AS CUST_GRP_2,
NULL AS CUST_GRP_TXT,
NULL AS FREIGHT_COLUMN,
NULL AS SPECIAL_HANDLING_IND,
NULL AS ORDER_FULFILLMENT,
NULL AS CREDIT_CONTROL_AREA_SS, 
T7.DEL_INDIC AS DELETION_FLAG_SS,
NULL AS INCOTERMS_PART_1,
NULL AS INCOTERMS_PART_2,
NULL AS PLANT_SS,
NULL AS PAYMENT_TERMS_SS,
NULL AS SALES_DISTRICT,
NULL AS SALES_GRP,
NULL AS SALES_OFFICE,
NULL AS ACTIVITY_FLAG,
NULL AS ORDER_BLOCK_SS,
NULL AS CUST_CLASS_ENTERPRISE,
NULL AS CUST_RECEIVE_REBATES,
NULL AS EDI_CUST_GRP,
NULL AS EDI_STATUS,
NULL AS BILL_BLOCK_SS,
NULL AS FLOATING_LOGO,
NULL AS FOUNDATION_DATE ,
NULL AS GOLDMINE_FLAG,
NULL AS GVF_EXCEPTION_CODE,
T1.TUCHIEA01 AS SUPER_SALES_AREA,
T1.TUCHIEA02 AS SALES_AREA,
T1.TUCHIEA03 AS SALES_EXECUTIVE,
NULL AS SALES_DIRECTOR,
NULL AS AREA_SALES_MANAGER,
T1.TUCHIEB03 AS SALES_REP,
NULL AS INTL_HQ,
NULL AS REGIONAL_HQ,
NULL AS COUNTRY_HQ ,
NULL AS REGIONAL_SUBSIDIARY,
NULL AS LOC_HRCHY_D_LVL_01,
NULL AS LOC_HRCHY_D_LVL_02,
NULL AS LOC_HRCHY_D_LVL_03,
NULL AS LOC_HRCHY_D_LVL_04,
NULL AS LOC_HRCHY_E_LVL_01,
NULL AS LOC_HRCHY_E_LVL_02,
NULL AS LOC_HRCHY_E_LVL_03,
NULL AS LOC_HRCHY_E_LVL_04,
NULL AS LOC_HRCHY_F_LEV_01,
NULL AS LOC_HRCHY_F_LVL_02,
NULL AS LOC_HRCHY_F_LVL_03,
NULL AS SUPER_SALES_AREA_G,
NULL AS SALES_AREA_MD_G,
NULL AS INTERNAL_ACCNT_MANAGER,
NULL AS PRICING_PROCEDURE,
NULL AS PRICE_GRP,
NULL AS ORDER_COMBINATION,
NULL AS PARTIAL_DELIVERY_AT_,
NULL AS LAST_EDI_OR_XML,
NULL AS DELIVERY_BLOCK_SS,
NULL AS LAST_INTOUCH_ORDER,
NULL AS LAST_ORDER,
NULL AS DELIVERY_PRIORITY,
NULL AS INVOICE_DATES,
NULL AS PRICE_LIST_TYPE,
NULL AS HP_NAMED_ACCNTS,
NULL AS AZLAN_VALUE_CUST,
NULL AS MS_MANAGED_PARTNER,
NULL AS LAST_COL_CHANGE_DAT,
NULL AS CUST_STATS_GRP,
NULL AS SHIPPING_CONDS,
NULL AS WEEE_EXCEPTION_CODE,
NULL AS TOP_100_ACCNTS_FLAG,
NULL AS WORST_ACCNTS_FLAG,
NULL AS HUNTING_FLAG,
NULL AS WAKE_UP_FLAG,
NULL AS INCLUDED_SMB_PLAN,
NULL AS EXCLUDED_SMB_PLAN,
NULL AS DROP_SHIPMENT_FLAG,
NULL AS PICK_UP_FLAG,
NULL AS EXCHANGE_RATE_TYPE,
NULL AS NBR_OF_RECORDS_SS,
NULL AS CURRENCY_KEY_SS,
NULL AS POD_RELEVANT,
NULL AS ABC_CUST,
NULL AS HP_CLASS,
NULL AS FREIGHT_DROPSHIPMENT,
NULL AS MANAGED_ACCNTS,
NULL AS LOC_HRCHY_H_LVL_01,
NULL AS LOC_HRCHY_H_LVL_02,
NULL AS LOC_HRCHY_H_LVL_03,
NULL AS TELCO_CUST_FLAG,
NULL AS DIGITAL_SIGNATURE,
NULL AS PAM_LC,
NULL AS CENTRAL_PAM_EUR,
NULL AS PAM_LAST_UPDATE,
NULL AS LOC_CURRENCY,
NULL AS CUST_DESIGNATION_GRP,
NULL AS ORDER_PRIORITIZATION,
NULL AS PROCEDURE_PROD_PROPOSAL,
NULL AS INVENTORY_SOURCE,
NULL AS US_BASED_EXPORTERS,
NULL AS FREIGHT_FORWARDER,
NULL AS BUY_BACK_TRADE_IN,
NULL AS AFFORDABILITY_PORTAL, 
NULL AS CUST_UNWILLING,
T1.TNSALEORG AS LEGACY_SALES_ORG,
SYSDATE() as UPDATE_DATE_UTC
FROM {{ source('us_cdp','CUSTOMERMASTER_BASE_68_MAPPED') }}  AS T1 
--FROM  US_DATAPRACTICE.CDP.CUSTOMERMASTER_BASE_68_MAPPED   AS T1

--LEFT JOIN {{ source('us_cdp_bw_68','TNCBPCUST') }} cust68
--LEFT JOIN ANALYTICS.EDW_SAP_BW_US_68.TNCBPCUST cust68
LEFT JOIN cust68
  ON T1.TNCBPCUST = cust68."/BIC/TNCBPCUST"
LEFT JOIN  {{ source('us_cdp_bw_46','TUCINDUSY_TXT') }} AS T6
--LEFT JOIN ANALYTICS.EDW_SAP_BW_US_46.TUCINDUSY_TXT  AS T6
  ON T1.TUCINDUSY = T6."/BIC/TUCINDUSY"
LEFT JOIN  {{ source('us_cdp_bw_46','TUCCUSTSS') }} cust46
--LEFT JOIN ANALYTICS.EDW_SAP_BW_US_46.TUCCUSTSS cust46
  ON ltrim(cust68.TNECUSNU,'0') = ltrim(cust46."/BIC/TUCCUSTSS", '0')
  AND cust46.SOURSYSTEM = 'A3'
  AND cust46."/BIC/TUCDISTRN" = '00'
  AND cust46."/BIC/TUCDIVISN" = '10'
  AND cust46."/BIC/TUCSALESG" = '0100'
--LEFT JOIN ANALYTICS.EDW_SAP_BW_US_68.TNCUST_SL T7
LEFT JOIN {{ source('us_cdp_bw_68','TNCUST_SL') }} T7
  ON T1.TNCBPCUST = T7.TNCUST_SL  
  AND T1.TUCSALESG = T7.TNSALEORG 
  AND T7.SOURSYSTEM = 'A2'
  AND T7.TNDISTNCH = '01'
  AND T7.TNDIV_SLS = '00'
LEFT JOIN cust_xref
  ON ltrim(T1.TNCBPCUST,0) = cust_xref.xref   
WHERE T1.TNSALEORG in('1001')

--PART 2
UNION ALL

SELECT 
MD5(CONCAT(T1.SOURSYSTEM,T2.TUCSALESG,T2.TUCDIVISN,T2.TUCDISTRN,T1.TUCCUSTOR)) AS CUSTOMER_MASTER_KEY,
T1.SOURSYSTEM,
'US01' AS SALES_ORG,  
'US01' AS COMPANY_CODE,  
T2.TUCDIVISN AS DIVISION,
T2.TUCDISTRN AS DISTR_CHANNEL,
T1.TUCCUSTOR AS RESELLER_ID,
T1.TUCCUSTOR AS RESELLER_ID_46,
NVL(cust_xref.mcust_no, T1.TUCCUSTOR) AS RESELLER_ID_COMBINED,  
NULL AS RESELLER_ID_68, 

CASE 
    WHEN T2.TUCHIEA03 IN ('0030001061') THEN TRUE 
    ELSE FALSE 
END AS ACCOUNT_CLOSURE_FLAG,

CASE 
    WHEN T2.TUCHIEA03 IN ('0030001061') 
      OR T1.TUCDELINC = 'X' 
      OR T2.TUCDELINC = 'X' 
      OR T3."/BIC/TUCDELINC" = 'X' 
      THEN TRUE 
    ELSE FALSE 
END AS DELETION_FLAG_MAIN,

NULL AS LAST_PACE_FLAG,
NULL AS PACE_FLAG_UPDATE,
NULL AS LAST_PACE_CLUSTER,
NULL AS LAST_CE_SEGMENT_TEXT,
NULL AS LAST_CE_SEGMENT_ID,
NULL AS CE_PERIOD_START,
NULL AS CE_PERIOD_END,
NULL AS LAST_LCS_STAGE,
NULL AS LAST_LCS_STAGE_TEXT,
NULL AS LAST_LCS_STAGE_UPDATE,
NULL AS LAST_LCS_STAGE_TEXT_CISCO ,
NULL AS LAST_LCS_STAGE_UPDATE_CISCO,
NULL AS TS_CUST_ACCOUNT,
nvl(cust_xref.mcust_no, T2.TUCHIEC03) as GROUPKEY,
NULL AS DNB_DUNS_NBR,
NULL AS DNB_CONFIDENCE_CODE,
NULL AS DNB_NAME_SCORE,
NULL AS DNB_CITY_SCORE,
NULL AS DNB_STATE_SCORE,
NULL AS DNB_STRNAME_SCORE,
NULL AS DNB_STRNUMB_SCORE,
NULL AS DNB_PHONE_SCORE,
NULL AS DNB_POBOX_SCORE,
T1.TUCACCNTP AS CUSTOMER_ACCNT_GROUP,
T1.TUCADDRNR AS ADDRESS,
T1.TUCCITYY AS CITY,
T1.TUCCOUNTY AS COUNTRY_KEY,
T1.TUCCUSTCS AS CUST_CLASSIFICATION,
T1.TUCCOUNTE AS COUNTY_CODE,
T1.TUCCUSFCS AS CUSTOMER_IS_CONSUMER,
T1.TUCDELINC AS DELETION_FLAG,
T1.TUCDBDUNM AS DUNS_NBR,
T1.TUCFAXNUM AS FAX_NBR,
T1.TUCINDUSY AS INDUSTRY_KEY,
T9."/BIC/TUCINDUSY" AS INDUSTRY_KEY_TEXT,
T1.TUCINDCO1  AS INDUSTRY_CODE_1,
T1.TUCINDCO2 AS INDUSTRY_CODE_2,
T1.TUCINDCO3 AS INDUSTRY_CODE_3,
T1.TUCINDCO4 AS INDUSTRY_CODE_4,
T1.TUCINDCO5 AS INDUSTRY_CODE_5,
T1.LANGU AS LANGUAGE_KEY,
T1.TUCNAMEE AS RESELLER_NAME,
T1.TUCNAME22 AS NAME_2,
T1.TUCNAME33 AS NAME_3,
T1.TUCNIELSD AS NIELSEN_ID,
T1.TUCPCOMPY AS TRADING_PARTNER,
T1.TUCPHONEE AS FIRST_TELEPHONE_NBR,
T1.TUCPLANTT AS PLANT,
T1.TUCPOBOXX AS PO_BOX,
T1.TUCPOSTAD AS POSTAL_CODE,
T1.TUCPOSTCX AS CAM_PO_BOX,
T1.TUCPOSTCS AS CAM_POSTAL_CODE_GEO,
T1.TUCREGION AS REGION,
T1.TUCSTREET AS HOUSE_NBR_AND_STR,
T1.TUCSORTLL AS SORT_FIELD,
T1.TUCTAXNUB AS TAX_NBR_1,
T1.TUCTAXNU2 AS TAX_NBR_2,
T1.TUCUSAGED AS MATERIAL_USAGE_IND,
T1.TUCVENDOR AS VENDOR,
T1.TUCAUFSD AS ORDER_BLOCK,
T1.TUCBBBNR AS INTL_LOCATION_NBR_1,
T1.TUCBBSNR AS INTL_LOCATION_NBR_2,
T1.TUCBUBKZ AS CHECK_DIGIT_INTL_LOC,
T1.TUCEMAILX AS CUST_EMAIL_ADDRESS,

CASE  
  WHEN TO_CHAR(t1.TUDERDATX) IN(NULL, '', '00000000') THEN NULL
ELSE TO_DATE(TO_CHAR(T1.TUDERDATX, 'YYYY-MM-DD'))
END AS CREATION_DATE,

T1.TUCFAKSD AS BILL_BLOCK,
T1.TUCGFORM AS LEGAL_STATUS,
T1.TUCHZUOR AS ASGMT_TO_HRCHY,
T1.TUCKATR1 AS DELIVERY_WAVER,
T1.TUCKATR6 AS LEASING_COMPANY,
T1.TUCKDKG3 AS ECOM_DISCOUNT_GROUP,
T1.TUCKDKG4 AS INTEREST_GROUP,
T1.TUCLIFSD AS DELIVERY_BLOCK,
T1.TUCLZONE AS TRANSPORTATION_ZONE,
T1.TUCNAME4 AS NAME_4,
T1.TUCORT2 AS DISTRICT,
T1.TUCPFORT AS PO_BOX_CITY,
T1.TUCSTCEG AS VAT_REGISTRATION_NBR,
T1.TUCTELBX AS TELEBOX_NBR,
T1.TUCTELFX AS FAX_NBR_2,
T1.TUCTTXNUM AS CUST_COMM_TELETEX,
T1.TUCVATCOU AS VAT_COUNTRY_KEY,
T1.TUCZZGMNR AS GLBL_MFR,
T1.TUCSTREE4 AS STREET_4,
T1.TUCDATLIN AS DATA_LINE,
T1.TUCUMSA1 AS ANNUAL_SALES,
T1.TUCUWAERR AS CURRENCY_SALES_FIG,
T1.FISCVARNT AS FISCAL_YEAR_VARIANT,
T1.TUCNONVAT AS FLAG_NON_VAT_REG,
T1.TUCECG AS EUROPEAN_CUST_GROUP,
T1.TUCKFTBUS AS CUST_GLBL_ACCNT,
T1.TUCKFTIND AS CUST_LOCAL_GRP,
T1.TUCBPARTR AS BUSINESS_PARTNER,
T1.TUCKDKG5 AS MIN_QTY_SURCHARGE_GR,
T1.TUCHUNST AS HUNTING_STATUS,
T1.TUCMETXJD AS TAX_JURISDICTION_COD,
T1.TUCELMTET AS TELEPHONE_EXTENSION,
T1.TUCSTCD3 AS TAX_CODE_3,
T1.TUCBEGRU AS AUTHORIZATION_GRP,
T1.TUCDBA1 AS DOING_BUSINESS_AS_1,
T1.TUCDBA2 AS DOING_BUSINESS_AS_2,
T1.TUCDTAWS AS DATA_MEDIUM_EXCHANGE,
T1.TUCKATR9 AS CUST_CLASS_SUPPLIES,
T1.TUCVENCLN AS VENDOR_CLASS_NBR,
T1.TUCVENCLS AS VENDOR_CLASS,
T3."/BIC/TUCCUSTCC" AS CUST_NBR_COMP_CODE,
T3."/BIC/TUCBUSABD" AS DUNNING_CLERK,
T3."/BIC/TUCKNRMA" AS ACCNT_NBR_DUN_RECIP,
T3."/BIC/TUCCCTRAA" AS CREDIT_CONTROL_AREA_CC,
T3."/BIC/TUCDELINC" AS DELETION_FLAG_CC,
T3."/BIC/TUCEMPLOE" AS EMPLOYEE,
T3."/BIC/TUCPMNTTS" AS PAYMENT_TERMS_CC,
T3."/BIC/TUCPMNTBK" AS PAYMENT_BLOCK_KEY,
T3."/BIC/TUCPMTMTL" AS PAYMENT_METHOD_SUPPL,
T3."/BIC/TUCALTKN"  AS PREV_MASTER_REC_NBR,
T3."/BIC/TUCBUSAB" AS ACCNT_CLERK,
T3."/BIC/TUCCRBLB" AS BLOCK_BY_CRED_MNGMT,
T3."/BIC/TUCCTLPC" AS RISK_CTGRY_NEW_CUST,
T3."/BIC/TUCDBRTG"  AS  CUST_RATING,
T3."/BIC/TUDGMVDTT" AS  LEGAL_DUNN_PROCEED,
T3."/BIC/TUCGRUPP"  AS  CUST_CREDIT_GRP,
T3."/BIC/TUCKNRZE"  AS  HEAD_OFFICE_ACCNT_NBR,
T3."/BIC/TUCKVERM"  AS  CREDIT_MEMO_TERMS,
T3."/BIC/TUCPERKZ"  AS  COLLECTIVE_INV_VAR,
T3."/BIC/TUCSBGRP"  AS  CREDIT_MGMT_REPS_GRP,
T3."/BIC/TUCTOGRU"  AS  TOLERANCE_GRP,
NULL as INS_VALIDITY_DATE, 
t3."/BIC/TUCVRBKZ" AS INS_INSTITUTION_NBR,
T3."/BIC/TUCVRSNR"  AS  INS_NBR ,
T3."/BIC/TUCVZSKZ"  AS  INTEREST_CALC_NBR,
T3."/BIC/TUCWAKON"  AS  EXCH_RATE_PAYM_TERMS,
T3."/BIC/TUCXAUSZ"  AS  BANK_STATEMENT,
T3."/BIC/TUCZWELS"  AS  PAYMENT_METHODS,
T3."/BIC/TUCGUZTE"  AS  CREDIT_MEMO_PAYM_TERM,
T3."/BIC/TUCINTADR" AS  INTERNET_ADDRESS,
T3."/BIC/TUKCUCRLI" AS  CUST_CREDIT_LIMIT,
T3."/BIC/TUKHORREC" AS  CREDIT_EXPOSURE_HORZ,
T3."/BIC/TUKRECORD" AS  NBR_OF_RECORDS_CC,
T3."/BIC/TUKTOTREC" AS  ACCNT_BALANCE,
T3."/BIC/TUKVLIBB"  AS  AMOUNT_INSURED,
T3."/BIC/TUKWRTAK"  AS  TOT_BANK_GUARANTEES,
T3."/BIC/TUKGENINO" AS  LAST_GEN_INFO ,
T3."/BIC/TUYWAERSX" AS  CURRENCY_KEY_AMNT_INS,
T3.CURRENCY AS  CURRENCY_KEY_CC,
T3."/BIC/TUCRCACCT" AS  RECONCILIATION_ACCNT,
T3."/BIC/TUCCHRTAS" AS  CHART_OF_ACCNTS,
T3."/BIC/TUCKNKLI"  AS  CUST_ACCNT_NBR_CL_REF,
T3."/BIC/TUCFCFDGP" AS  CASH_MGMT_GRP,
T3."/BIC/TUCCRMAN"  AS  CREDIT_MANAGER,
T3."/BIC/TUCGROREL" AS  GRP_RELATIONSHIP,
T3."/BIC/TUCBEGRU" AS   AUTHORIZATION_GRP_CC,
T3."/BIC/TUCZUAWA"  AS  SORTING_KEY,
T3."/BIC/TUCALTKNA" AS  OLD_CUST_NBR,
T3."/BIC/TUCCADUNC" AS  DUNNING_PROCEDURE,
T3."/BIC/TUCLOCKB"  AS  KEY_OF_THE_LOCKBOX,
T3."/BIC/TUCPRACCT" AS  CREDIT_PARENT_ACCNT,
T3."/BIC/TUDNEXTRV" AS  NEXT_INTERNAL_REVIEW,

CASE  
  WHEN TO_CHAR(T3."/BIC/TUDLASTDN") IN(NULL, '', '00000000') THEN NULL
ELSE TO_DATE(T3."/BIC/TUDLASTDN", 'YYYYMMDD')
END AS DATE_OF_LAST_DUNNING,

T3."/BIC/TUCDUNNLL" AS  DUNNING_LEVEL,
T3."/BIC/TUCDUNNBK" AS  DUNNING_BLOCK,
T2.TUCCUSTSS AS CUST_SALES,

CASE  
  WHEN TO_CHAR(T2.TUDLBILDA) IN(NULL, '', '00000000') THEN NULL
ELSE TO_DATE(TO_CHAR(T2.TUDLBILDA, 'YYYY-MM-DD'))
END AS DATE_OF_LAST_DUNNING,

T2.TUCVENCLS AS VENDOR_CLASS_SS,
T2.TUCVENCLN AS VENDOR_CLASS_NBR_SS,
T2.TUCACCNTN AS CUST_ACCNT_ASGMT_GRP,
T2.TUCCUSTCA AS CUST_CLASS_ABC,
T2.TUCCUSTGP AS CUST_GRP,
T2.TUCCUSTG1 AS PROFILE_COLUMN_CUST,
T2.TUCCUSTG2 AS CUST_GRP_2,
NULL AS CUST_GRP_TXT,
T2.TUCCUSTG3 AS  FREIGHT_COLUMN,
T2.TUCCUSTG4 AS SPECIAL_HANDLING_IND,
T2.TUCCUSTG5 AS ORDER_FULFILLMENT,
T2.TUCCCTRAA AS CREDIT_CONTROL_AREA_SS,
T2.TUCDELINC AS DELETION_FLAG_SS,
T2.TUCINCOTS AS INCOTERMS_PART_1,
T2.TUCINCOT2 AS INCOTERMS_PART_2,
T2.TUCPLANTT AS PLANT_SS,
T2.TUCPMNTTS AS PAYMENT_TERMS_SS,
T2.TUCSALEST AS SALES_DISTRICT,
T2.TUCSALESP AS SALES_GRP,
T2.TUCSALESF AS SALES_OFFICE,
T2.TUCAKTIV  AS ACTIVITY_FLAG,
T2.TUCAUFSD AS ORDER_BLOCK_SS,
T2.TUCCUSCL AS CUST_CLASS_ENTERPRISE,
T2.TUCBOKRE AS CUST_RECEIVE_REBATES,
T2.TUCEDGRP AS EDI_CUST_GRP,
T2.TUCEDSTA AS EDI_STATUS,
T2.TUCFAKSD AS BILL_BLOCK_SS,
T2.TUCFLLOGO AS FLOATING_LOGO,
T2.TUDFODATE AS FOUNDATION_DATE,
T2.TUCGFLAG AS GOLDMINE_FLAG,
T2.TUCGVFCO AS GVF_EXCEPTION_CODE,
T2.TUCHIEA01 AS SUPER_SALES_AREA,
T2.TUCHIEA02 AS SALES_AREA,
T2.TUCHIEA03 AS SALES_EXECUTIVE,
T2.TUCHIEB01 AS SALES_DIRECTOR,
T2.TUCHIEB02 AS AREA_SALES_MANAGER,
T2.TUCHIEB03 AS SALES_REP,
T2.TUCHIEC01 AS INTL_HQ,
T2.TUCHIEC02 AS REGIONAL_HQ,
T2.TUCHIEC03 AS COUNTRY_HQ,
T2.TUCHIEC04 AS REGIONAL_SUBSIDIARY,
T2.TUCHIED01 AS LOC_HRCHY_D_LVL_01,
T2.TUCHIED02 AS LOC_HRCHY_D_LVL_02,
T2.TUCHIED03 AS LOC_HRCHY_D_LVL_03,
T2.TUCHIED04 AS LOC_HRCHY_D_LVL_04,
T2.TUCHIEE01 AS LOC_HRCHY_E_LVL_01,
T2.TUCHIEE02 AS LOC_HRCHY_E_LVL_02,
T2.TUCHIEE03 AS LOC_HRCHY_E_LVL_03 ,
T2.TUCHIEE04 AS LOC_HRCHY_E_LVL_04,
T2.TUCHIEF01 AS LOC_HRCHY_F_LEV_01,
T2.TUCHIEF02 AS LOC_HRCHY_F_LVL_02,
T2.TUCHIEF03 AS LOC_HRCHY_F_LVL_03,
T2.TUCHIEG01 AS SUPER_SALES_AREA_G,
T2.TUCHIEG02 AS SALES_AREA_MD_G,
T2.TUCHIEG03 AS INTERNAL_ACCNT_MANAGER,
T2.TUCKALKS AS PRICING_PROCEDURE,
T2.TUCKONDA AS PRICE_GRP,
T2.TUCKZAZU AS ORDER_COMBINATION,
T2.TUCKZTLF AS PARTIAL_DELIVERY_AT_,
T2.TUDLEDORD AS LAST_EDI_OR_XML,
T2.TUCLIFSD AS DELIVERY_BLOCK_SS,
T2.TUDLITORD AS LAST_INTOUCH_ORDER,
T2.TUDLORDDA AS LAST_ORDER,
T2.TUCLPRIO AS DELIVERY_PRIORITY,
T2.TUCPERFK AS INVOICE_DATES,
T2.TUCPLTYP AS PRICE_LIST_TYPE,
T2.TUCPRAT5 AS HP_NAMED_ACCNTS,
T2.TUCPRAT6 AS AZLAN_VALUE_CUST,
T2.TUCPRAT7 AS MS_MANAGED_PARTNER,
T2.TUDUDATEE AS LAST_COL_CHANGE_DAT,
T2.TUCVERSG AS CUST_STATS_GRP,
T2.TUCVSBED AS SHIPPING_CONDS,
T2.TUCWECOD AS WEEE_EXCEPTION_CODE,
T2.TUCZPRAT1 AS TOP_100_ACCNTS_FLAG,
T2.TUCZPRAT2 AS WORST_ACCNTS_FLAG ,
T2.TUCZPRAT3 AS HUNTING_FLAG,
T2.TUCZPRAT4 AS WAKE_UP_FLAG,
T2.TUCZPRAT5 AS INCLUDED_SMB_PLAN,
T2.TUCZPRAT6 AS EXCLUDED_SMB_PLAN,
T2.TUCZZDRBL AS DROP_SHIPMENT_FLAG ,
T2.TUCZZPIBL AS PICK_UP_FLAG,
T2.TUCKURST AS EXCHANGE_RATE_TYPE,
T2.TUKRECORD AS NBR_OF_RECORDS_SS ,
T2.CURRENCY AS CURRENCY_KEY_SS,
T2.TUCPODREL AS POD_RELEVANT,
T2.TUCCUABC AS ABC_CUST,
T2.TUCHPCL AS HP_CLASS,
T2.TUCFRDS AS FREIGHT_DROPSHIPMENT,
T2.TUCZPRAT7 AS MANAGED_ACCNTS,
T2.TUCHIEH01 AS LOC_HRCHY_H_LVL_01,
T2.TUCHIEH02 AS LOC_HRCHY_H_LVL_02,
T2.TUCHIEH03 AS LOC_HRCHY_H_LVL_03,
T2.TUCTELCO AS TELCO_CUST_FLAG,
T2.TUCDIGSIG AS DIGITAL_SIGNATURE,
T2.TUKPAML AS PAM_LC,
NULL AS CENTRAL_PAM_EUR,
T2.TUDPAMLUU AS PAM_LAST_UPDATE,
T2.TUYLOCCUY AS LOC_CURRENCY,
T2.TUCTCDGRP AS CUST_DESIGNATION_GRP,
T2.TUCZORPRI AS ORDER_PRIORITIZATION,
T2.TUCPVKSM AS PROCEDURE_PROD_PROPOSAL,
T2.TUCINVSOU AS INVENTORY_SOURCE,
T2.TUCUSEXFL AS  US_BASED_EXPORTERS,
T2.TUCFRFWFL AS FREIGHT_FORWARDER,
T2.TUCZPRA12 AS BUY_BACK_TRADE_IN,
T2.TUCZPRA13 AS AFFORDABILITY_PORTAL,
T2.TUCCUSUNV AS CUST_UNWILLING,
T2.TUCSALESG AS LEGACY_SALES_ORG,
SYSDATE() as UPDATE_DATE_UTC
FROM  {{ source('us_cdp','TUCCUSTOR_VIEW') }}   AS T1
JOIN  {{ source('us_cdp','TUCCUSTSS_VIEW') }}    AS T2 
--FROM  US_DATAPRACTICE.CDP.TUCCUSTOR_VIEW   AS T1
--JOIN  US_DATAPRACTICE.CDP.TUCCUSTSS_VIEW  AS T2 
  ON T1.TUCCUSTOR = T2.TUCCUSTSS 
  AND T1.SOURSYSTEM = T2.SOURSYSTEM
  AND T2.TUCDISTRN = '00'
  AND T2.TUCDIVISN = '10'
  AND T2.TUCSALESG = '0100'
JOIN   {{ source('us_cdp_bw_46','TUCCUSTCC') }} AS T3 
--JOIN    ANALYTICS.EDW_SAP_BW_US_46.TUCCUSTCC AS T3 
  ON T2.TUCCUSTSS = LTRIM(T3."/BIC/TUCCUSTCC") 
  AND T2.SOURSYSTEM = T3.SOURSYSTEM
  AND T2.TUCSALESG = T3."/BIC/TUCCOMPCE"

LEFT JOIN  {{source('us_cdp_bw_46','TUCINDUSY_TXT') }}  AS T9
--LEFT JOIN  ANALYTICS.EDW_SAP_BW_US_46.TUCINDUSY_TXT  AS T9
  ON T1.TUCINDUSY = T9."/BIC/TUCINDUSY"
  
LEFT JOIN (SELECT DISTINCT TUCSALESG,TNCBPCUST,TNECUSNU 
           FROM  {{source('us_cdp','CUSTOMERMASTER_BASE_68') }} )  AS T4
           --FROM  US_DATAPRACTICE.CDP.CUSTOMERMASTER_BASE_68)  AS T4 
  ON T1.TUCCUSTOR = T4.TNECUSNU 
  AND T2.TUCCUSTSS = T4.TNCBPCUST 
  AND T2.TUCSALESG = T4.TUCSALESG
LEFT JOIN cust_xref
  ON ltrim(T1.TUCCUSTOR,0) = cust_xref.xref
  
WHERE 
T1.SOURSYSTEM = 'A3'

-- PART 3
union all

select 
md5(concat('CIS_US',to_char(CUST.CUST_NO))) as CUSTOMER_MASTER_KEY
, 'CIS_US' as SOURSYSTEM
, 'US01' as SALES_ORG  
, 'US01' as COMPANY_CODE  
, NULL as DIVISION
, 0 as DISTR_CHANNEL
, to_char(CUST.CUST_NO) as RESELLER_ID
, to_char(CUST.CUST_NO) as RESELLER_ID_46
, to_char(CUST.MCUST_NO) as RESELLER_ID_COMBINED
, NULL as RESELLER_ID_68

, CASE
    WHEN IS_DISCONTINUED = 'Y' THEN TRUE
    ELSE FALSE
  END AS ACCOUNT_CLOSURE_FLAG

, CASE 
    WHEN IS_DISCONTINUED = 'Y' THEN TRUE
    ELSE FALSE
  END AS DELETION_FLAG_MAIN
  
, NULL as LAST_PACE_FLAG
, NULL as PACE_FLAG_UPDATE
, NULL as LAST_PACE_CLUSTER
, NULL as LAST_CE_SEGMENT_TEXT
, NULL as LAST_CE_SEGMENT_ID
, NULL as CE_PERIOD_START
, NULL as CE_PERIOD_END
, NULL as LAST_LCS_STAGE
, NULL as LAST_LCS_STAGE_TEXT
, NULL as LAST_LCS_STAGE_UPDATE
, NULL as LAST_LCS_STAGE_TEXT_CISCO
, NULL as LAST_LCS_STAGE_UPDATE_CISCO
, NULL as TS_CUST_ACCOUNT
, to_char(cust.MCUST_NO) as GROUPKEY
, NULL as DNB_DUNS_NBR
, NULL as DNB_CONFIDENCE_CODE
, NULL as DNB_NAME_SCORE
, NULL as DNB_CITY_SCORE
, NULL as DNB_STATE_SCORE
, NULL as DNB_STRNAME_SCORE
, NULL as DNB_STRNUMB_SCORE
, NULL as DNB_PHONE_SCORE
, NULL as DNB_POBOX_SCORE

, CASE 
    WHEN cust.cust_acct_type IN(null, '') THEN 'UNKNOWN'
    ELSE UPPER(cust.cust_acct_type)
  END as CUSTOMER_ACCNT_GROUP 
  
--, to_char(addr.ADDR_NO) as ADDRESS
, NULL as ADDRESS
, to_char(cust.BILL_TO_CUST_CITY) as CITY
, to_char(cust.BILL_TO_CUST_COUNTRY) as COUNTRY_KEY
, NULL as CUST_CLASSIFICATION
, NULL as COUNTY_CODE
, NULL as CUSTOMER_IS_CONSUMER

, CASE 
    WHEN IS_DISCONTINUED = 'Y' THEN 'X'
    ELSE ''
  END AS DELETION_FLAG
  
, NULL as DUNS_NBR
, NULL as FAX_NBR
, to_char(cust.cust_seg_id) as INDUSTRY_KEY
, to_char(cust.sales_segment) as INDUSTRY_KEY_TEXT
, NULL as INDUSTRY_CODE_1
, NULL as INDUSTRY_CODE_2
, NULL as INDUSTRY_CODE_3
, NULL as INDUSTRY_CODE_4
, NULL as INDUSTRY_CODE_5
, NULL as LANGUAGE_KEY
, to_char(cust.MCUST_NAME) as RESELLER_NAME
, to_char(cust.customer_alias_name) as NAME_2
, NULL as NAME_3
, NULL as NIELSEN_ID
, NULL as TRADING_PARTNER
, to_char(cust.BILL_TO_CONTACT_PHONE) as FIRST_TELEPHONE_NBR
, NULL as PLANT
, NULL as PO_BOX
, to_char(cust.BILL_TO_CUST_ZIP) as POSTAL_CODE
, NULL as CAM_PO_BOX
, NULL as CAM_POSTAL_CODE_GEO
, to_char(cust.BILL_TO_CUST_STATE) as REGION
, to_char(cust.BILL_TO_CUST_ADDR) as HOUSE_NBR_AND_STR
, NULL as SORT_FIELD
, NULL as TAX_NBR_1
, NULL as TAX_NBR_2
, NULL as MATERIAL_USAGE_IND
, NULL as VENDOR
, NULL as ORDER_BLOCK
, NULL as INTL_LOCATION_NBR_1
, NULL as INTL_LOCATION_NBR_2
, NULL as CHECK_DIGIT_INTL_LOC
, to_char(cust.BILL_TO_CONTACT_EMAIL) as CUST_EMAIL_ADDRESS
, to_date(cust.CUSTOMER_ENTRY_DATETIME) as CREATION_DATE
, NULL as BILL_BLOCK
, NULL as LEGAL_STATUS
, NULL as ASGMT_TO_HRCHY
, NULL as DELIVERY_WAVER
, NULL as LEASING_COMPANY
, NULL as ECOM_DISCOUNT_GROUP
, NULL as INTEREST_GROUP
, NULL as DELIVERY_BLOCK
, NULL as TRANSPORTATION_ZONE
, NULL as NAME_4
, NULL as DISTRICT
, NULL as PO_BOX_CITY
, NULL as VAT_REGISTRATION_NBR
--, to_char(resales.resales_no) as VAT_REGISTRATION_NBR
, NULL as TELEBOX_NBR
, NULL as FAX_NBR_2
, NULL as CUST_COMM_TELETEX
, NULL as VAT_COUNTRY_KEY
, NULL as GLBL_MFR
, NULL as STREET_4
, NULL as DATA_LINE
, 0 as ANNUAL_SALES
, NULL as CURRENCY_SALES_FIG
, NULL as FISCAL_YEAR_VARIANT
, NULL as FLAG_NON_VAT_REG
, NULL as EUROPEAN_CUST_GROUP
, NULL as CUST_GLBL_ACCNT
, NULL as CUST_LOCAL_GRP
, NULL as BUSINESS_PARTNER
, NULL as MIN_QTY_SURCHARGE_GR
, NULL as HUNTING_STATUS
, NULL as TAX_JURISDICTION_COD
, NULL as TELEPHONE_EXTENSION
, NULL as TAX_CODE_3
, NULL as AUTHORIZATION_GRP
, NULL as DOING_BUSINESS_AS_1
, NULL as DOING_BUSINESS_AS_2
, NULL as DATA_MEDIUM_EXCHANGE
, NULL as CUST_CLASS_SUPPLIES
, NULL as VENDOR_CLASS_NBR
, NULL as VENDOR_CLASS
, NULL as CUST_NBR_COMP_CODE
, NULL as DUNNING_CLERK
, NULL as ACCNT_NBR_DUN_RECIP
, NULL as CREDIT_CONTROL_AREA_CC
, NULL as DELETION_FLAG_CC
, NULL as EMPLOYEE
, to_char(cust.DEFAULT_TERMS) as PAYMENT_TERMS_CC
, NULL as PAYMENT_BLOCK_KEY
, NULL as PAYMENT_METHOD_SUPPL
, NULL as PREV_MASTER_REC_NBR
, NULL as ACCNT_CLERK
, NULL as BLOCK_BY_CRED_MNGMT
, NULL as RISK_CTGRY_NEW_CUST
, NULL as CUST_RATING
, NULL as LEGAL_DUNN_PROCEED
, NULL as CUST_CREDIT_GRP
, NULL as HEAD_OFFICE_ACCNT_NBR
, NULL as CREDIT_MEMO_TERMS
, NULL as COLLECTIVE_INV_VAR
, NULL as CREDIT_MGMT_REPS_GRP
, NULL as TOLERANCE_GRP
, NULL as INS_VALIDITY_DATE
, NULL as INS_INSTITUTION_NBR
, NULL as INS_NBR
, NULL as INTEREST_CALC_NBR
, NULL as EXCH_RATE_PAYM_TERMS
, NULL as BANK_STATEMENT
, NULL as PAYMENT_METHODS
, NULL as CREDIT_MEMO_PAYM_TERM
, NULL as INTERNET_ADDRESS
, to_char(creditinfo.credit_limit) as CUST_CREDIT_LIMIT
, 0 as CREDIT_EXPOSURE_HORZ
, 0 as NBR_OF_RECORDS_CC
, 0 as ACCNT_BALANCE
, to_char(creditinfo.insurance_limit) as AMOUNT_INSURED
, 0 as TOT_BANK_GUARANTEES
, 0 as LAST_GEN_INFO
, NULL as CURRENCY_KEY_AMNT_INS
, to_char(cust.CURRENCY) as CURRENCY_KEY_CC
, NULL as RECONCILIATION_ACCNT
, NULL as CHART_OF_ACCNTS
, NULL as CUST_ACCNT_NBR_CL_REF
, NULL as CASH_MGMT_GRP
, NULL as CREDIT_MANAGER
, NULL as GRP_RELATIONSHIP
, NULL as AUTHORIZATION_GRP_CC
, NULL as SORTING_KEY
, NULL as OLD_CUST_NBR
, NULL as DUNNING_PROCEDURE
, NULL as KEY_OF_THE_LOCKBOX
, NULL as CREDIT_PARENT_ACCNT
, NULL as NEXT_INTERNAL_REVIEW
, NULL as DATE_OF_LAST_DUNNING
, NULL as DUNNING_LEVEL
, NULL as DUNNING_BLOCK
, NULL as CUST_SALES
, to_char(creditinfo.last_pay_date) as LAST_BILL_DATE
, NULL as VENDOR_CLASS_SS
, NULL as VENDOR_CLASS_NBR_SS
, NULL as CUST_ACCNT_ASGMT_GRP
, NULL as CUST_CLASS_ABC
, NULL as CUST_GRP
, NULL as PROFILE_COLUMN_CUST
, NULL as CUST_GRP_2
, NULL as CUST_GRP_TXT
, NULL as FREIGHT_COLUMN
, NULL as SPECIAL_HANDLING_IND
, NULL as ORDER_FULFILLMENT
, NULL as CREDIT_CONTROL_AREA_SS
, NULL as DELETION_FLAG_SS
, NULL as INCOTERMS_PART_1
, NULL as INCOTERMS_PART_2
, NULL as PLANT_SS
, to_char(cust.DEFAULT_TERMS) as PAYMENT_TERMS_SS
, NULL as SALES_DISTRICT
, NULL as SALES_GRP
, NULL as SALES_OFFICE
, NULL as ACTIVITY_FLAG
, NULL as ORDER_BLOCK_SS
, NULL as CUST_CLASS_ENTERPRISE
, NULL as CUST_RECEIVE_REBATES
, NULL as EDI_CUST_GRP
, NULL as EDI_STATUS
, NULL as BILL_BLOCK_SS
, NULL as FLOATING_LOGO
, NULL as FOUNDATION_DATE
, NULL as GOLDMINE_FLAG
, NULL as GVF_EXCEPTION_CODE
, to_char(CUST.division) as SUPER_SALES_AREA
, to_char(CUST.cust_type) as SALES_AREA
, to_char(CUST.SALES_TERR) as SALES_EXECUTIVE
, NULL as SALES_DIRECTOR
, NULL as AREA_SALES_MANAGER
, to_char(terr.USER_ID) as SALES_REP
, NULL as INTL_HQ
, NULL as REGIONAL_HQ
, NULL as COUNTRY_HQ
, NULL as REGIONAL_SUBSIDIARY
, NULL as LOC_HRCHY_D_LVL_01
, NULL as LOC_HRCHY_D_LVL_02
, NULL as LOC_HRCHY_D_LVL_03
, NULL as LOC_HRCHY_D_LVL_04
, NULL as LOC_HRCHY_E_LVL_01
, NULL as LOC_HRCHY_E_LVL_02
, NULL as LOC_HRCHY_E_LVL_03
, NULL as LOC_HRCHY_E_LVL_04
, NULL as LOC_HRCHY_F_LEV_01
, NULL as LOC_HRCHY_F_LVL_02
, NULL as LOC_HRCHY_F_LVL_03
, NULL as SUPER_SALES_AREA_G
, NULL as SALES_AREA_MD_G
, to_char(terr.SALES_TERR) as INTERNAL_ACCNT_MANAGER
, NULL as PRICING_PROCEDURE
, NULL as PRICE_GRP
, NULL as ORDER_COMBINATION
, NULL as PARTIAL_DELIVERY_AT_
, to_date(creditinfo.last_edi_or_xml_date) as LAST_EDI_OR_XML
, NULL as DELIVERY_BLOCK_SS
, to_date(creditinfo.last_ec_order_date) as LAST_INTOUCH_ORDER
, to_date(creditinfo.last_purchase) as LAST_ORDER
, 0 as DELIVERY_PRIORITY
, NULL as INVOICE_DATES
, NULL as PRICE_LIST_TYPE
, NULL as HP_NAMED_ACCNTS
, NULL as AZLAN_VALUE_CUST
, NULL as MS_MANAGED_PARTNER
, NULL as LAST_COL_CHANGE_DAT
, NULL as CUST_STATS_GRP
, NULL as SHIPPING_CONDS
, NULL as WEEE_EXCEPTION_CODE
, NULL as TOP_100_ACCNTS_FLAG
, NULL as WORST_ACCNTS_FLAG
, NULL as HUNTING_FLAG
, NULL as WAKE_UP_FLAG
, NULL as INCLUDED_SMB_PLAN
, NULL as EXCLUDED_SMB_PLAN
, NULL as DROP_SHIPMENT_FLAG
, NULL as PICK_UP_FLAG
, NULL as EXCHANGE_RATE_TYPE
, 0 as NBR_OF_RECORDS_SS
, NULL as CURRENCY_KEY_SS
, NULL as POD_RELEVANT
, NULL as ABC_CUST
, NULL as HP_CLASS
, NULL as FREIGHT_DROPSHIPMENT
, NULL as MANAGED_ACCNTS
, NULL as LOC_HRCHY_H_LVL_01
, NULL as LOC_HRCHY_H_LVL_02
, NULL as LOC_HRCHY_H_LVL_03
, NULL as TELCO_CUST_FLAG
, NULL as DIGITAL_SIGNATURE
, 0 as PAM_LC
, NULL as CENTRAL_PAM_EUR
, NULL as PAM_LAST_UPDATE
, to_char(cust.currency_profile)  as LOC_CURRENCY
, NULL as CUST_DESIGNATION_GRP
, NULL as ORDER_PRIORITIZATION
, NULL as PROCEDURE_PROD_PROPOSAL
, NULL as INVENTORY_SOURCE
, NULL as US_BASED_EXPORTERS
, NULL as FREIGHT_FORWARDER
, NULL as BUY_BACK_TRADE_IN
, NULL as AFFORDABILITY_PORTAL
, NULL as CUST_UNWILLING
, NULL as LEGACY_SALES_ORG
, SYSDATE() as UPDATE_DATE_UTC
from {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_INFO_VIEW_US') }} as cust
   -- left join {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_ADDRESS_CONTACTS_INFO_US') }} as addr
--from ANALYTICS.EDW_CIS_US.DIM_PUB_CUSTOMER_INFO_VIEW_US  cust
--left join ANALYTICS.EDW_CIS_US.DIM_PUB_CUSTOMER_ADDRESS_CONTACTS_INFO_US  addr
--on cust.mcust_no = addr.cust_no and addr.xref_seq = 1 and (addr.stop_email = 'N' OR addr.stop_email is null ) AND addr.BAD_EMAIL = 'Y' and cust.IS_DISCONTINUED is not null --  --  CHANGED 12/5 JR

left join {{ source('us_cdp_cis_us','DIM_PUB_SALES_HIERARCHY_BY_TERR_USER_ROLE_VIEW_US') }} as terr
--left join ANALYTICS.EDW_CIS_US.DIM_PUB_SALES_HIERARCHY_BY_TERR_USER_ROLE_VIEW_US  terr
  on cust.sales_terr = to_char(terr.sales_terr)
  and user_role = 'Rep' 
  and is_primary = 'Y'
  and user_end_date is null
  
left join {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_XREF_US') }} as xref
--left join ANALYTICS.EDW_CIS_US.DIM_PUB_CUSTOMER_XREF_US xref
  on cust.cust_no = xref.cust_no
  and xref_type = 'MASTER_SUB'
  and active = 'Y'
  
left join {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_CREDIT_INFO_US') }}  as creditinfo
--left join ANALYTICS.EDW_CIS_US.DIM_PUB_CUSTOMER_CREDIT_INFO_US  as creditinfo
  on cust.cust_no = creditinfo.cust_no

--left join {{ source('us_cdp_cis_us','DIM_PUB_CUSTOMER_RESALES_NO_US') }} as  resales
--left join ANALYTICS.EDW_CIS_US.DIM_PUB_CUSTOMER_RESALES_NO_US as  resales
--on  cust.cust_no = resales.cust_no
