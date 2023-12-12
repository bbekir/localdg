SELECT * FROM dba_source WHERE 1 = 1 AND upper( text ) LIKE '%COUPON_SELECTION_EXT%';

SELECT s.last_load_time, s.*
FROM v$sql s
WHERE upper( sql_fulltext ) LIKE '%INSERT%COUPON_SELECTION%'
ORDER BY s.last_load_time DESC
       ---ODI_APP.ETL_BLYRNDWH_TABINCDAYS

SELECT *
FROM prod_odi_repo.snp_sess_task
WHERE 1 = 1
  AND upper( col_txt ) LIKE '%DATAGUARD.BILYONER.COM%'
--   AND last_date > sysdate - 2


  SELECT *
FROM prod_odi_repo.snp_sess_task
WHERE 1 = 1
  AND upper( def_txt ) LIKE '%DATAGUARD.BILYONER.COM%'

;

SELECT *
FROM snp_scen_task
WHERE 1 = 1
  -- and upper(col_txt) like '%TABINCDAYS%'
  AND upper( def_txt ) LIKE '%COUPON_SELECTION_EXT%'



SELECT * FROM snp_txt_header WHERE 1 = 1 AND full_text LIKE '%COUPON_SELECTION_EXT%'


SELECT * from snp_session s
where upper( s.error_message ) LIKE '%DATAGUARD.BILYONER.COM%'


SELECT s.sess_no, s.sess_name
FROM prod_odi_repo.snp_sess_task_log l
         LEFT JOIN snp_session s ON l.sess_no = s.sess_no
WHERE 1 = 1
  --    AND upper( l.error_message) LIKE '%DATAGUARD.BILYONER.COM%'
  AND def_txt LIKE '%DATAGUARD.BILYONER.COM%'
--   AND last_date > sysdate - 2
;

SELECT b.scen_no, b.scen_name, b.*
FROM snp_sb_task t
         LEFT JOIN snp_sb b ON t.sb_no = b.sb_no
WHERE 1 = 1
  -- and upper(def_txt) like '%DATAGUARD.BILYONER.COM%'
  AND upper( col_txt ) LIKE '%DATAGUARD.BILYONER.COM%'
;
SELECT * FROM snp_txt_header
where 1=1
and upper(full_text) like '%DATAGUARD.BILYONER.COM%'

;

SELECT * FROM snp_line_trt
where 1=1
and variable_defs like '%%';

