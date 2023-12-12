CREATE OR REPLACE PROCEDURE daterange_gather_partition_stat ( p_owner        IN VARCHAR2
                        , p_table        IN VARCHAR2
                        , p_partition_column   IN VARCHAR2 DEFAULT NULL
                        , p_first_date IN VARCHAR2
                        , p_cascade_flag  IN boolean  DEFAULT TRUE
                        , p_degree IN VARCHAR2 DEFAULT NULL)
AUTHID CURRENT_USER IS
PRAGMA AUTONOMOUS_TRANSACTION;
  c_proc       VARCHAR2 (30) := $$plsql_unit;
  l_partname  varchar2(100);
  l_dynquery CLOB;

  BEGIN


         l_dynquery := 'SELECT dbms_rowid.rowid_object( rowid )  FROM ' || p_schema || '.' || p_table
                    || 'WHERE 1=1' ||
                       ' AND ' || p_partition_column ||'= ' || TO_DATE(p_first_date, 'yyyymmdd')

                        ;
        EXECUTE IMMEDIATE dynquery INTO rowcnt;


     sys.dbms_stats.gather_table_stats (
     ownname          => p_owner,
     tabname          => p_table,
     partname         =>  p_partition,
     estimate_percent => dbms_stats.auto_sample_size,
     method_opt       => 'for all columns size auto',
     cascade          => p_cascade_flag,
     degree           => NVL(p_degree,5));

END;

SELECT subobject_name INTO l_partname FROM dba_objects WHERE data_object_id IN ();