CREATE OR REPLACE PROCEDURE gather_partition_stat ( p_owner        IN VARCHAR2
                        , p_table        IN VARCHAR2
                        , p_partition    IN VARCHAR2 DEFAULT NULL
                        , p_cascade_flag  IN boolean  DEFAULT TRUE
                        , p_degree IN VARCHAR2 DEFAULT NULL)
AUTHID CURRENT_USER IS
PRAGMA AUTONOMOUS_TRANSACTION;
  c_proc       VARCHAR2 (30) := $$plsql_unit;
  l_dynsql CLOB;

  BEGIN


     sys.dbms_stats.gather_table_stats (
     ownname          => p_owner,
     tabname          => p_table,
     partname         =>  p_partition,
     estimate_percent => dbms_stats.auto_sample_size,
     method_opt       => 'for all columns size auto',
     cascade          => p_cascade_flag,
     degree           => NVL(p_degree,5));

END;



SELECT subobject_name FROM dba_objects WHERE data_object_id IN (SELECT dbms_rowid.rowid_object( rowid ) FROM repo.xyz);


---disable all table constraint

PROCEDURE disable_table_constraints  (owner_in        IN VARCHAR2
                        , table_in        IN VARCHAR2
         )

BEGIN
   FOR i IN (SELECT   constraint_name, table_name
               FROM   user_constraints
              WHERE   table_name = table_in
              --and owner= owner_id --hangi yetkiyle çalışacak? akustik e
              )
   LOOP
      EXECUTE IMMEDIATE 'ALTER TABLE ' || i.table_name || ' DISABLE CONSTRAINT ' || i.constraint_name || '';
   END LOOP;
END;

