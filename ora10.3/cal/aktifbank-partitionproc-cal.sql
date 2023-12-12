DROP TABLE akustik.xyz2;
-- nümerik alan üzerinden partition
CREATE TABLE xyz2 (
    c1 number(8, 0),
    c2 varchar2(40)
                  ) NOLOGGING PARTITION BY RANGE (c1) INTERVAL (1) (
    PARTITION p0 VALUES LESS THAN (20221228),
    PARTITION p1 VALUES LESS THAN (20221229),
    PARTITION p2 VALUES LESS THAN (20221230),
    PARTITION p3 VALUES LESS THAN (20221231)
                                                                   )


INSERT INTO xyz2 VALUES ( 20221228, 'name09' );
INSERT INTO xyz2 VALUES ( 20221229, 'name10' );
INSERT INTO xyz2 VALUES ( 20221230, 'name11' );
INSERT INTO xyz2 VALUES ( 20221231, 'name12' );
INSERT INTO xyz2 VALUES ( 20230101, 'name01' );

COMMIT;
-- timestamp alan üzerinden partition
CREATE TABLE xyz (
    c1 number(8, 0),
    c2 varchar2(40),
    c3 timestamp
                 ) NOLOGGING PARTITION BY RANGE (c3) INTERVAL (numtodsinterval( 1, 'DAY' )) (
    PARTITION p0 VALUES LESS THAN (to_date( '20230101', 'YYYYMMDD' ))
                                                                                            );

INSERT INTO xyz VALUES ( 1, 'a', sysdate - 100 );
INSERT INTO xyz VALUES ( 2, 'b', sysdate - 99 );
INSERT INTO xyz VALUES ( 3, 'b', sysdate - 20 );
INSERT INTO xyz VALUES ( 4, 'b', sysdate - 19 );
INSERT INTO xyz VALUES ( 4.5, 'f', sysdate - 19 - 3 / 24 );
INSERT INTO xyz VALUES ( 5, 'c', sysdate - 18 );

INSERT INTO xyz VALUES ( 6, 'd', sysdate - 17 );

COMMIT;


SELECT t.*, t.partition_name, t.num_rows FROM all_tab_partitions t WHERE 1 = 1 AND table_owner = 'REPO'
-- and table_name = 'XYZ';
SELECT * FROM user_ind_partitions WHERE status = 'UNUSABLE';

SELECT * FROM xyz;


ALTER TABLE xyz
    TRUNCATE PARTITION FOR (to_date( '20230128', 'yyyymmdd' )) UPDATE GLOBAL INDEXES;

DECLARE
    p_table       varchar2(100);
    p_day         varchar2(100);
    p_date_format varchar2(100); e_partition_not_exist EXCEPTION ; PRAGMA EXCEPTION_INIT ( e_partition_not_exist,-2149 );
BEGIN
    p_table := 'XYZ';
    p_day := '20240128';
    p_date_format := 'YYYYMMDD';
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_table || ' TRUNCATE PARTITION FOR ( TO_DATE(''' || p_day || ''', ''' ||
                          p_date_format || '''))';
    EXCEPTION
        WHEN e_partition_not_exist THEN NULL; WHEN OTHERS THEN RAISE ;
    END;
END;



BEGIN
    dbms_output.enable( );
END;




SELECT dbms_rowid.rowid_object( rowid ) FROM xyz;


SELECT rowid FROM xyz







with high_vals as
  (select dbms_xmlgen.getxmltype('
select p.high_value,
       p.partition_name
from   dba_tab_partitions p
where 1=1 and p.table_name=''COSTS'' ')
          as xml
   from   dual
), dts as (
  select partition_name,
         to_date(substr(high_value, 12, 19), 'yyyy-mm-dd hh24:mi:ss') st_dt,
         lead(to_date(substr(high_value, 12, 19), 'yyyy-mm-dd hh24:mi:ss'))
           over (order by high_value) en_dt
  from   high_vals p,
         xmltable('/ROWSET/ROW'
          passing p.xml
          columns partition_name varchar2(30)
                    path '/ROW/PARTITION_NAME',
                  high_value varchar2(60)
                    path '/ROW/HIGH_VALUE'
         )
)
  select * from dts
--   where  timestamp'2016-06-08 00:15:00' between st_dt and nvl(en_dt, timestamp'2016-06-08 00:15:00');
;




WITH monthbase
     AS (    SELECT TO_CHAR (ADD_MONTHS (DATE '2013-02-01', LEVEL), 'yyyymm') AS monthpart
                  , TO_NUMBER (TO_CHAR (ADD_MONTHS (DATE '2013-02-01', LEVEL + 1), 'yyyymmdd'))  highval
               FROM DUAL
         CONNECT BY LEVEL <=
                       MONTHS_BETWEEN (DATE '2020-01-01', DATE '2013-02-01')-1)
SELECT    ', PARTITION part_'
       || monthpart
       || ' VALUES LESS THAN ('
       || highval
       || ')'
          AS partsql
  FROM monthbase
  order by monthpart;