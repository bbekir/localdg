---DB METADATA

---ROW count
--------------
DECLARE
    dynquery varchar2(1000);
    rowcnt   number;
    s_schema varchar2(20) := '';
    CURSOR curtablo
        IS
        SELECT table_name FROM user_tables WHERE 1 = 1 ORDER BY table_name;
BEGIN
    dbms_output.put_line( 'Table' || ';' || 'rowCnt' );
    FOR rec IN curtablo LOOP
        dynquery := 'SELECT COUNT(*) FROM ' || s_schema || '.' || rec.table_name;
        EXECUTE IMMEDIATE dynquery INTO rowcnt;
        dbms_output.put_line( rec.table_name || ';' || rowcnt );
    END LOOP;
END;



--segment boyutları
SELECT owner, segment_name AS table_name, sum( bytes ) / 1024 / 1024 AS mb
FROM dba_segments
WHERE segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
  AND owner = 'CRM_DM'
GROUP BY owner
       , segment_name


SELECT * FROM dba_tables WHERE owner = 'LBRDWH'
;

----Tablespace boyutları
SELECT df.tablespace_name AS tbs
     , round(totalusedspace / 1024 ,2) AS used_gb
     , round((df.totalspace - tu.totalusedspace) / 1024 ,2)AS free_gb
     , round(df.totalspace / 1024,2)  AS total_gb
     , round( 100 * ((df.totalspace - tu.totalusedspace) / df.totalspace) ) AS pct_free
FROM (SELECT tablespace_name, round( sum( bytes ) / 1048576 ) AS totalspace
      FROM dba_data_files
      GROUP BY tablespace_name) df, (SELECT round( sum( bytes ) / (1024 * 1024) ) AS totalusedspace, tablespace_name
                                     FROM dba_segments
                                     GROUP BY tablespace_name) tu
WHERE df.tablespace_name = tu.tablespace_name
--   AND tu.tablespace_name LIKE '%%'


---- Datafile boyutları
SELECT file# AS no, name, trunc( bytes / 1000000000 ) AS gb
FROM v$datafile g

    ----> Alternatifleri

SELECT SUM(mb)/1024 as gb from (
SELECT owner, table_name, round( (num_rows * avg_row_len) / (1024 * 1024) ) AS mb
FROM all_tables
WHERE owner  LIKE 'CRM_DM%'
) a


--istatistikler toplanma ihtiyacı hissedilir ise ..
BEGIN
    dbms_stats.gather_table_stats( 'MYSCHEMA', 'MYTABLE' );
END;

--- tablo boyutları özet


WITH tab         AS (SELECT segment_name AS table_name, owner, bytes
                     FROM dba_segments
                     WHERE segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
                     UNION ALL
                     SELECT l.table_name, l.owner, s.bytes
                     FROM dba_lobs l, dba_segments s
                     WHERE s.segment_name = l.segment_name
                       AND s.owner = l.owner
                       AND s.segment_type = 'LOBSEGMENT')
   , ind         AS (SELECT i.table_name, i.owner, s.bytes
                     FROM dba_indexes i, dba_segments s
                     WHERE s.segment_name = i.index_name
                       AND s.owner = i.owner
                       AND s.segment_type IN ('INDEX', 'INDEX PARTITION', 'INDEX SUBPARTITION')
                     UNION ALL
                     SELECT l.table_name, l.owner, s.bytes
                     FROM dba_lobs l, dba_segments s
                     WHERE s.segment_name = l.index_name
                       AND s.owner = l.owner
                       AND s.segment_type = 'LOBINDEX')
   , tsz         AS (SELECT t.owner, t.table_name, round( sum( t.bytes ) / 1024 / 1024 / 1024, 4 ) AS sizegb
                     FROM tab t
                     WHERE 1 = 1 --t.owner IN ('OS')
                     GROUP BY t.table_name
                            , t.owner
                     ORDER BY sum( t.bytes ))
   , isz         AS (SELECT i.owner, i.table_name, round( sum( i.bytes ) / 1024 / 1024 / 1024, 4 ) AS sizegb
                     FROM ind i
                     WHERE 1 = 1 -- i.owner IN ('OS')
                     GROUP BY i.table_name
                            , i.owner
                     ORDER BY sum( i.bytes ))
   , tab_general AS (SELECT a.owner
                          , a.table_name
                          , a.partitioned
                          , a.num_rows
                          , a.last_analyzed
                          , a.logging
                          , a.compression
                          , a.hybrid
                          , to_char( d.created, 'yyyy.mm.dd' ) AS created
                          , to_char( d.last_ddl_time, 'yyyy.mm.dd' ) AS last_ddl
                          , replace( substr( d.timestamp, 1, 10 ), '-', '.' ) AS timestamp
                     FROM dba_objects d, dba_tables a
                     WHERE 1 = 1
                      --d.owner IN ('OS')
                       AND a.owner = d.owner
                       AND d.object_type = 'TABLE'
                       AND d.object_name = a.table_name)
SELECT g.owner
     , g.table_name
     , g.partitioned
     , g.logging
     , g.last_analyzed
     , g.last_ddl
     , g.created
     , g.timestamp
     , nvl( g.compression, 'ENABLED' ) AS compression
     , g.logging
     , g.num_rows
     , t.sizegb AS datasize
     , i.sizegb AS indexsize
     , nvl( t.sizegb, 0 ) + nvl( i.sizegb, 0 ) AS totalsizegb
FROM tab_general g
         LEFT JOIN tsz t ON t.owner = g.owner AND t.table_name = g.table_name
         LEFT JOIN isz i ON t.owner = i.owner AND t.table_name = i.table_name
WHERE 1 = 1
AND g.owner NOT IN ('SYS', 'SYSTEM', 'ANONYMOUS', 'XDB', 'MDSYS', 'WMSYS', 'ORDDATA', 'CTXSYS','DBSNMP','ORDSYS','OLAPSYS','OJVMSYS')
ORDER BY nvl( t.sizegb, 0 ) + nvl( i.sizegb, 0 ) DESC;



--Tablo Listesi


SELECT a.owner
     , a.table_name
     , a.partitioned
     , a.num_rows
     , a.last_analyzed
     , to_char( d.created, 'yyyy.mm.dd' ) AS created
     , to_char( d.last_ddl_time, 'yyyy.mm.dd' ) AS last_ddl
     , replace( substr( d.timestamp, 1, 10 ), '-', '.' ) AS timestamp
FROM dba_objects d, all_tables a
WHERE d.owner IN ('')
  AND a.owner = d.owner
  AND d.object_type = 'TABLE'
  AND d.object_name = a.table_name


--db obje listesi
SELECT d.owner
     , d.object_name
     , d.object_type
     , to_char( d.created, 'yyyy.mm.dd' ) AS created
     , to_char( d.last_ddl_time, 'yyyy.mm.dd' ) AS last_ddl
     , replace( substr( d.timestamp, 1, 10 ), '-', '.' ) AS timestamp
     , d.status
     , d.generated
FROM dba_objects d
WHERE object_type IN
      ('DATABASE LINK', 'DIRECTORY', 'FUNCTION', 'INDEX', 'JOB', 'MATERIALIZED VIEW', 'PACKAGE', 'PACKAGE BODY',
       'PROCEDURE', 'SCHEDULE', 'SEQUENCE', 'SYNONYM', 'TABLE', 'TRIGGER', 'VIEW')
  AND owner IN ('DW')

-- scheduled jobs
SELECT owner AS schema_name
     , job_name
     , job_style
     , CASE WHEN job_type IS NULL THEN 'PROGRAM' ELSE job_type END AS job_type
     , CASE WHEN job_type IS NULL THEN program_name ELSE job_action END AS job_action
     , start_date
     , CASE WHEN repeat_interval IS NULL THEN schedule_name ELSE repeat_interval END AS schedule
     , last_start_date
     , next_run_date
     , state
FROM sys.all_scheduler_jobs
ORDER BY owner
       , job_name;



BEGIN
    dbms_output.enable( 1000000 );
END;


-- tablolara günlük erişim istatistiği


SELECT decode( day, 2, 'Monday', 3, 'Tuesday', 4, 'Wednesday', 5, 'Thursday', 6, 'Friday', 7, 'Saturday', 1,
               'Sunday' ) AS day
     , tab
     , owner
     , cnt
FROM (SELECT p.object_owner AS owner
           , p.object_name AS tab
           , to_char( sn.end_interval_time, 'd' ) AS day
           , count( 1 ) AS cnt
      FROM dba_hist_sql_plan p, dba_hist_sqlstat s, dba_hist_snapshot sn
      WHERE p.object_owner <> 'SYS'
        AND p.sql_id = s.sql_id
        AND s.snap_id = sn.snap_id
        --         AND p.operation IN ('INSERT STATEMENT', 'CREATE TABLE STATEMENT')
      GROUP BY p.object_owner
             , p.object_name
             , to_char( sn.end_interval_time, 'd' )
      ORDER BY day
             , tab
             , object_owner);

---tablo kullanımı yıllık detay- -güvenilir gelmedi. 430 tablo sadece peh
SELECT p.object_owner AS owner
     , p.object_name AS tab
     , decode( p.operation, 'INSERT STATEMENT', 'I', 'CREATE TABLE STATEMENT', 'I', 'S' ) AS typ
     , to_char( sn.end_interval_time, 'YYYY' ) AS year
     , count( 1 ) AS cnt
FROM dba_hist_sql_plan p, dba_hist_sqlstat s, dba_hist_snapshot sn
WHERE 1 = 1
  AND p.object_owner <> 'SYS'
  AND p.object_owner = 'AKUSTIK'
  --   AND p.object_type = 'TABLE'
  AND p.sql_id = s.sql_id
  AND s.snap_id = sn.snap_id
GROUP BY p.object_owner
       , p.object_name
       , decode( p.operation, 'INSERT STATEMENT', 'I', 'CREATE TABLE STATEMENT', 'I', 'S' )
       , to_char( sn.end_interval_time, 'YYYY' )
ORDER BY object_name;



SELECT to_char( begin_interval_time, 'yy/mm/dd/hh24' ) AS begin_interval, physical_reads_total, object_name
FROM dba_hist_seg_stat s, dba_hist_seg_stat_obj o, dba_hist_snapshot sn
WHERE o.owner = 'AKUSTIK'
  AND s.obj# = o.obj#
  AND sn.snap_id = s.snap_id
ORDER BY begin_interval_time;


---Kolonlar


SELECT ta.table_name AS "Table Name"
     , ta.column_name AS "Column Name"
     , ta.comments AS "Column Comment"
     , ta.datatype AS "Column Datatype"
     , nvl( co.primary_key_flg, 'No' ) AS "Column Is PK"
     , nvl( co.foreign_key_flg, 'No' ) AS "Column Is FK"
     , ta.nullable AS "Null Option"
FROM (SELECT a.owner
           , a.table_name AS table_name
           , a.column_name AS column_name
           , a.column_id
           , b.comments AS comments
           , initcap( a.data_type ) ||
             decode( a.data_type, 'CHAR', '(' || a.char_length || ')', 'VARCHAR', '(' || a.char_length || ')',
                     'VARCHAR2', '(' || a.char_length || ')', 'NCHAR', '(' || a.char_length || ')', 'NVARCHAR',
                     '(' || a.char_length || ')', 'NVARCHAR2', '(' || a.char_length || ')', 'NUMBER',
                     '(' || nvl( a.data_precision, a.data_length ) ||
                     decode( a.data_scale, NULL, NULL, ',' || a.data_scale ) || ')', NULL ) AS datatype
           , CASE WHEN a.nullable = 'Y' THEN 'Null' ELSE 'Not Null' END AS nullable
      FROM sys.all_tab_columns a, sys.all_col_comments b
      WHERE a.owner = ''
        --           and (:v_TABLE_NAME is null or  instr(upper(a.table_name),upper(:v_TABLE_NAME)) > 0)
        AND substr( a.table_name, 1, 4 ) != 'BIN$'
        AND substr( a.table_name, 1, 3 ) != 'DR$'
        AND a.table_name = b.table_name
        AND a.owner = b.owner
        AND a.column_name = b.column_name) ta
         LEFT JOIN (SELECT ac.owner
                         , ac.table_name
                         , dc.column_name
                         , CASE WHEN ac.constraint_type = 'P' THEN 'Yes' ELSE 'No' END AS primary_key_flg
                         , CASE WHEN ac.constraint_type = 'F' THEN 'Yes' ELSE 'No' END AS foreign_key_flg
                    FROM sys.all_ind_columns dc, sys.all_indexes di, sys.all_constraints ac
                    WHERE dc.table_owner = ''
                      --          and (:v_TABLE_NAME is null or  instr(upper(dc.table_name),upper(:v_TABLE_NAME)) > 0)
                      AND dc.table_name = ac.table_name
                      AND dc.index_name = di.index_name
                      AND dc.index_name = ac.constraint_name
                      AND dc.index_owner = di.owner
                      AND ac.owner = user
                      AND ac.constraint_type IN ('P', 'F')
                      AND substr( ac.table_name, 1, 4 ) != 'BIN$'
                      AND substr( ac.table_name, 1, 3 ) != 'DR$') co
                   ON ta.owner = co.owner AND ta.table_name = co.table_name AND ta.column_name = co.column_name
ORDER BY ta.table_name
       , ta.column_id
       ;


