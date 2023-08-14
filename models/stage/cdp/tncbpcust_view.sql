{{
    config(
        alias = 'tncbpcust_view'

    )
}}
with cust_xref as (
    select "/BIC/TUCCUSTOR" as TUCCUSTOR
    from {{ source('us_cdp_bw_46','TUCCUSTOR') }} 
    where SOURSYSTEM = 'A3'
      and "/BIC/TUCCUSTOR" not in('0000000000', '')   )
SELECT
    t1."SOURSYSTEM" AS SOURSYSTEM,
    "OBJVERS" AS OBJVERS,
    t1."/BIC/TNCBPCUST" AS TNCBPCUST,
   NVL(NULLIF(txtlg, ''), NVL(NULLIF(txtmd, ''), txtsh)) AS TNCBPCUST_TEXT ,
   cust_xref.TUCCUSTOR AS TNECUSNU,   
    "/BIC/TNNAME" AS TNCNAME,
    "/BIC/TNINDUSTY" AS TNCINDUSTY,
    "/BIC/TNPOSTLCD" AS TNCPOSTLCD,
    "/BIC/TNCITY" AS TNCCITY,
    "/BIC/TNSTREET" AS TNCSTREET,   
    "/BIC/TNREGION" AS TNCREGION,
    "/BIC/TNTELNUM" AS TNCTELNUM,
    "/BIC/TNCOUNTRY" AS TNCCOUNTRY,    
    "/BIC/TNDELINDC" AS TNCDELINDC,
    TRY_TO_DATE("CREATEDON"::STRING, 'YYYYMMDD') AS CREATEDON,
    "/BIC/TNCUSTYP" AS TNCCUSTYP
FROM {{ source('us_cdp_bw_68','TNCBPCUST') }}  t1
LEFT JOIN {{ source('us_cdp_bw_68','TNCBPCUST_TXT') }} t2 ON t1."/BIC/TNCBPCUST" = t2."/BIC/TNCBPCUST"
LEFT JOIN cust_xref ON ltrim(t1."/BIC/TNSERTRM1",'0') = ltrim(cust_xref.TUCCUSTOR, '0')


