select
    QUERY_ID as query_ID,
       DATABASE_NAME,
       SCHEMA_NAME,
       TABLES,
       QUERY_TYPE, 
       USER_NAME, 
    split(st.VALUE, '.')[0]::string as DB_st,
    split(st.VALUE, '.')[1]::string as SCH_st,
    split(st.VALUE, '.')[2]::string as TBL_st,
    st.VALUE::string as FULL_TABLE_NAME_st,
    split(tt.VALUE, '.')[0]::string as DB_tt,
    split(tt.VALUE, '.')[1]::string as SCH_tt,
    split(tt.VALUE, '.')[2]::string as TBL_tt,
    tt.VALUE::string as FULL_TABLE_NAME_tt
from junk,
     lateral flatten(TABLES:source_tables) st,
     lateral flatten(TABLES:target_tables) tt
where FULL_TABLE_NAME_st is null
;

select distinct split(st.VALUE, '.')[0]::string as DB_st from junk,
     lateral flatten(TABLES:source_tables) st
;
select
    QUERY_ID as query_ID,
       DATABASE_NAME,
       SCHEMA_NAME,
       QUERY_TYPE,
       USER_NAME,
       ROLE_NAME,
       WAREHOUSE_NAME,
       WAREHOUSE_SIZE
       TABLES,
        split(st.VALUE, '.')[0]::string as DB_st,
    split(st.VALUE, '.')[1]::string as SCH_st,
    split(st.VALUE, '.')[2]::string as TBL_st,
    st.VALUE::string as FULL_TABLE_NAME_st,
    split(tt.VALUE, '.')[0]::string as DB_tt,
    split(tt.VALUE, '.')[1]::string as SCH_tt,
    split(tt.VALUE, '.')[2]::string as TBL_tt,
    tt.VALUE::string as FULL_TABLE_NAME_tt,
       EXECUTION_TIME,
       ERROR,
       START_TIME,
       END_TIME,
       TOTAL_ELAPSED_TIME
from junk2,
     lateral flatten(input => TABLES:source_tables, OUTER => true) AS st,
     lateral flatten(input => TABLES:target_tables, OUTER => true) AS tt
  ;
select
        QUERY_ID as query_ID,
        DATABASE_NAME,
        SCHEMA_NAME,
        QUERY_TYPE,
        USER_NAME,
        ROLE_NAME,
        WAREHOUSE_NAME,
        WAREHOUSE_SIZE
        TABLES,
        split(st.VALUE, '.')[0]::string as DB_st,
        split(st.VALUE, '.')[1]::string as SCH_st,
        split(st.VALUE, '.')[2]::string as TBL_st,
        st.VALUE::string as FULL_TABLE_NAME_st,
        split(tt.VALUE, '.')[0]::string as DB_tt,
        split(tt.VALUE, '.')[1]::string as SCH_tt,
        split(tt.VALUE, '.')[2]::string as TBL_tt,
        tt.VALUE::string as FULL_TABLE_NAME_tt,
        EXECUTION_TIME,
        ERROR,
        START_TIME,
        END_TIME,
        TOTAL_ELAPSED_TIME
from junk,
     lateral flatten(input => TABLES:source_tables, OUTER => true) AS st,
     lateral flatten(input => TABLES:target_tables, OUTER => true) AS tt
;
;
use warehouse junk
;
      # 1- (NON_Printify_orders / ifnull(pod_order_count, 0))                                            as Printify_wallet_share,
;
select distinct MERCHANT_ID,
                MERCHANT_SELLER_STATE
        from
            junk4
;
and CREATED_DT >= DATEADD(day, -30, current_date())
;
select count(distinct MONGO_ID)
from junk5
where CREATED_DT::date between '2021-12-01' and current_date() and MERCHANT_ID = 7084134
;
mode((case WHEN IS_PRINTIFY_ORDER = false and is_pod_pr = 1 then lb else null end) in ('unknown', 'other'))end
;
date_trunc(month, CREATED_DT)::date as month,
;