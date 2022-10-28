--/// Query for analyzing DWH users and stakeholders - understanding DWH user activity ///

SELECT
    QUERY_ID                            AS query_ID,
    DATABASE_NAME                       AS database_name,
    SCHEMA_NAME                         AS schema_name,
    TABLES                              AS tables,
    QUERY_TYPE                          AS query_type, 
    USER_NAME                           AS user_name, 
    split(st.VALUE, '.')[0]::string     AS DB_st,
    split(st.VALUE, '.')[1]::string     AS SCH_st,
    split(st.VALUE, '.')[2]::string     AS TBL_st,
    st.VALUE::string                    AS FULL_TABLE_NAME_st,
    split(tt.VALUE, '.')[0]::string     AS DB_tt,
    split(tt.VALUE, '.')[1]::string     AS SCH_tt,
    split(tt.VALUE, '.')[2]::string     AS TBL_tt,
    tt.VALUE::string                    AS FULL_TABLE_NAME_tt,
    EXECUTION_TIME                      AS excecution_time,
    ERROR                               AS error,
    START_TIME                          AS start_time,
    END_TIME                            AS end_time,
    TOTAL_ELAPSED_TIME                  AS total_elapsed_time
FROM test,
     lateral flatten(TABLES:source_tables) st,
     lateral flatten(TABLES:target_tables) tt
WHERE FULL_TABLE_NAME_st IS NOT NULL
;