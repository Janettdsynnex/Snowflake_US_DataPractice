{{
    config(
        alias = 'tuccustor_view'

    )
}}

select
      SOURSYSTEM
    ,"/BIC/TUCCUSTOR" as tuccustor
    , OBJVERS
    , CHANGED
    ,"/BIC/TUCACCNTP" as tucaccntp
    ,"/BIC/TUCADDRNR" as tucaddrnr
    ,"/BIC/TUCBPARTR" as tucbpartr
    ,"/BIC/TUCCITYY" as tuccityy
    ,"/BIC/TUCCOUNTY" as tuccounty
    ,"/BIC/TUCCOUNTE" as tuccounte
    ,"/BIC/TUCCUSTCS" as tuccustcs
    ,"/BIC/TUCCUSFCS" as tuccusfcs
    ,"/BIC/TUCDBDUNM" as tucdbdunm
    ,"/BIC/TUCDELINC" as tucdelinc
    ,"/BIC/TUCELMTET" as tucelmtet
    ,"/BIC/TUCFAXNUM" as tucfaxnum
    ,FISCVARNT
    ,t1."/BIC/TUCINDUSY" as tucindusy
    ,t2.txtsh as tucindusy_text
    ,"/BIC/TUCINDCO1" as tucindco1  
    ,"/BIC/TUCINDCO2" as tucindco2
    ,"/BIC/TUCINDCO3" as tucindco3
    ,"/BIC/TUCINDCO4" as tucindco4
    ,"/BIC/TUCINDCO5" as tucindco5
    ,t1.LANGU
    ,"/BIC/TUCMETXJD" as tucmetxjd
    ,"/BIC/TUCNAMEE" as tucnamee
    ,"/BIC/TUCNAME22" as tucname22
    ,"/BIC/TUCNAME33" as tucname33
    ,"/BIC/TUCNIELSD" as tucnielsd
    ,"/BIC/TUCPCOMPY" as tucpcompy
    ,"/BIC/TUCPHONEE" as tucphonee
    ,"/BIC/TUCPLANTT" as tucplantt
    ,"/BIC/TUCPOBOXX" as tucpoboxx
    ,"/BIC/TUCPOSTAD" as tucpostad
    ,"/BIC/TUCPOSTCX" as tucpostcx
    ,"/BIC/TUCPOSTCS" as tucpostcs
    ,"/BIC/TUCREGION" as tucregion
    ,"/BIC/TUCSORTLL" as tucsortll
    ,"/BIC/TUCSTREET" as tucstreet
    ,"/BIC/TUCTAXNUB" as tuctaxnub
    ,"/BIC/TUCTAXNU2" as tuctaxnu2
    ,"/BIC/TUCUSAGED" as tucusaged
    ,"/BIC/TUCVENDOR" as tucvendor
    ,"/BIC/TUCAUFSD" as tucaufsd
    ,"/BIC/TUCBBBNR" as tucbbbnr
    ,"/BIC/TUCBBSNR" as tucbbsnr
    ,"/BIC/TUCBEGRU" as tucbegru
    ,"/BIC/TUCBUBKZ" as tucbubkz
    ,"/BIC/TUCDATLIN" as tucdatlin
    ,"/BIC/TUCDBA1" as tucdba1
    ,"/BIC/TUCDBA2" as tucdba2
    ,"/BIC/TUCDTAWS" as tucdtaws
    ,"/BIC/TUCECG" as tucecg
    ,"/BIC/TUCEMAILX" as tucemailx
    ,try_to_date("/BIC/TUDERDATX"::string,'YYYYMMDD') as tuderdatx
    ,"/BIC/TUCFAKSD" as tucfaksd
    ,"/BIC/TUCGFORM" as tucgform
    ,"/BIC/TUCHUNST" as tuchunst
    ,"/BIC/TUCHZUOR" as tuchzuor
    ,"/BIC/TUCKATR1" as tuckatr1
    ,"/BIC/TUCKATR6" as tuckatr6
    ,"/BIC/TUCKATR9" as tuckatr9
    ,"/BIC/TUCKDKG3" as tuckdkg3
    ,"/BIC/TUCKDKG4" as tuckdkg4
    ,"/BIC/TUCKDKG5" as tuckdkg5
    ,"/BIC/TUCKFTBUS" as tuckftbus
    ,"/BIC/TUCKFTIND" as tuckftind
    ,"/BIC/TUCKONZS" as tuckonzs
    ,"/BIC/TUCLIFSD" as tuclifsd
    ,"/BIC/TUCLZONE" as tuclzone
    ,"/BIC/TUCNAME4" as tucname4
    ,"/BIC/TUCNONVAT" as tucnonvat
    ,"/BIC/TUCORT2" as tucort2
    ,"/BIC/TUCPFORT" as tucpfort
    ,"/BIC/TUCSTCD3" as tucstcd3
    ,"/BIC/TUCSTCEG" as tucstceg
    ,"/BIC/TUCSTREE4" as tucstree4
    ,"/BIC/TUCTELBX" as tuctelbx
    ,"/BIC/TUCTELFX" as tuctelfx
    ,"/BIC/TUCTTXNUM" as tucttxnum
    ,"/BIC/TUCVATCOU" as tucvatcou
    ,"/BIC/TUCVENCLN" as tucvencln
    ,"/BIC/TUCVENCLS" as tucvencls
    ,"/BIC/TUCZZGMNR" as tuczzgmnr
    ,"/BIC/TUKUMSA1" as  tucumsa1
    ,"/BIC/TUYUWAERR" as tucuwaerr
FROM {{ source('us_cdp_bw_46','TUCCUSTOR') }}  t1
left join {{ source('us_cdp_bw_46','TUCINDUSY_TXT') }}  t2
  on t1."/BIC/TUCINDUSY" = t2."/BIC/TUCINDUSY"
  and t2.langu = 'E'  
