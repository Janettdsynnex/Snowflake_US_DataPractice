{{
    config(
        alias = 'customermaster_base_68'

    )
}}


SELECT DISTINCT 
CASE WHEN TNSALEORG IN ('1001','A001','US33')  THEN  '0100'
     ELSE TNSALEORG 
END AS TUCSALESG,
TNSALEORG,
T2.* FROM {{ source('us_cdp_bw_68','TNCUST_SL') }}  AS T1
INNER JOIN {{ source('us_cdp','TNCBPCUST_VIEW') }} AS T2 ON T1.TNCUST_SL = T2.TNCBPCUST AND T1.SOURSYSTEM = T2.SOURSYSTEM
WHERE T1.SOURSYSTEM = 'A2'
AND TNDISTNCH = '01'
AND TNDIV_SLS = '00'
AND TNCCUSTYP = '0001'
AND TNSALEORG IN ('1001','A001','US33')
--AND TNCDELINDC = '' done on 1/4 JB

