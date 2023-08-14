{{
    config(
        alias = 'profit_ctr_hier'

    )
}}


with hier_txt as (
              select 
                hieid
               ,nodename
               ,txtsh
               ,txtmd
               ,txtlg
              from {{ source('us_cdp_bw_46','RSTHIERNODE') }} 
              where langu = 'E'
                and objvers = 'A'  )

     ,hier as (
          select *
          from {{ source('us_cdp_bw_46','TUCPROFIR_HIER') }} )     
    
      
select 
l2.ctrl_area
,l2.soursystem
,l2.prft_ctr
,l2.prft_ctr_txt
,l2.l4_nodename as SBUPCTR_L4
,l2.l4_nodetext as SBUPCTR_L4_TEXT
,l2.l3_nodename as SBUPCTR_L3
,l2.l3_nodetext as SBUPCTR_L3_TEXT
,l2.l2_nodename as SBUPCTR_L2
,l2.l2_nodetext as SBUPCTR_L2_TEXT
,l1.nodename as SBUPCTR_L1
,l1_hier_txt.txtlg as SBUPCTR_L1_TEXT
from hier as l1
join hier_txt  as l1_hier_txt
  on l1.nodename = l1_hier_txt.nodename
  and l1.hieid = l1_hier_txt.hieid
join (
        select
        l3.hieid
        ,l3.objvers
        ,l3.ctrl_area
        ,l3.soursystem
        ,l3.prft_ctr
        ,l3.prft_ctr_txt
        ,l3.l4_nodename
        ,l3.l4_nodetext        
        ,l3.l3_nodename
        ,l3.l3_nodetext
        ,l2.parentid as l2_parentid
        ,l2.nodename as l2_nodename 
        ,l2_hier_txt.txtlg as l2_nodetext
        from hier as l2
        join hier_txt as l2_hier_txt
          on l2.nodename = l2_hier_txt.nodename
          and l2.hieid = l2_hier_txt.hieid
        join (
                select 
                l4.hieid
                ,l4.objvers
                ,l4.ctrl_area
                ,l4.soursystem
                ,l4.prft_ctr
                ,l4.prft_ctr_txt                
                ,l4.l4_nodename
                ,l4.l4_nodetext
                ,l3.parentid as l3_parentid
                ,l3.nodename as l3_nodename 
                ,l3_hier_txt.txtlg as l3_nodetext
                from hier as l3
                join hier_txt as l3_hier_txt
                  on l3.nodename = l3_hier_txt.nodename
                  and l3.hieid = l3_hier_txt.hieid
                join (
                       select
                      l5.hieid
                      ,l5.objvers
                      ,l5.ctrl_area
                      ,l5.soursystem
                      ,l5.prft_ctr
                      ,l5.prft_ctr_txt                                   
                      ,l4.parentid as l4_parentid
                      ,l4.nodename as l4_nodename 
                      ,l4_hier_txt.txtlg as l4_nodetext
                    from  hier as l4 
                    join hier_txt as l4_hier_txt
                      on l4.nodename = l4_hier_txt.nodename
                      and l4.hieid = l4_hier_txt.hieid
                    join (select 
                          substr(hier.nodename, 1, 4) as ctrl_area
                          ,substr(hier.nodename, 5, 2) as soursystem
                          ,substr(hier.nodename, 7, 10) as prft_ctr  
                          ,nodename as l5_nodename
                          ,nodeid
                          ,hieid
                          ,objvers
                          ,parentid as l5_parentid
                          ,txtmd as prft_ctr_txt 
                        from hier
                        join {{ source('us_cdp_bw_46','TUCPROFIR_TXT') }}  txt
                          on substr(hier.nodename, 1, 4) = txt."/BIC/TUCCOAREA"
                          and substr(hier.nodename, 5, 2) = txt.soursystem
                          and substr(hier.nodename, 7, 10) = txt."/BIC/TUCPROFIR"
                          and txt.dateto = '99991231'
                        where hieid = 'KS4R5BA2GXGBH9J562VA2XH3U'
                          and objvers = 'A'
                          and iobjnm = 'TUCPROFIR' ) as l5
                    on l4.hieid = l5.hieid
                    and l4.objvers = l5.objvers
                    and l4.nodeid = l5.l5_parentid ) as l4
                on l3.hieid = l4.hieid
                and l3.objvers = l4.objvers
                and l3.nodeid = l4.l4_parentid ) as l3
          on l2.hieid = l3.hieid
          and l2.objvers = l3.objvers
          and l2.nodeid = l3.l3_parentid ) l2
    on l1.hieid = l2.hieid
    and l1.objvers = l2.objvers
    and l1.nodeid = l2.l2_parentid