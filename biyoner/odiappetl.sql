

SELECT count( * ), max( sess_endtime ) -- into v_daily_rcnt, v_daily_sdt
FROM odi_app.etl_blynrdwh_log
WHERE 1 = 1
  -- sess_mappingname = p_sess_mapname
  AND sess_status = 'S'
  AND sess_mapfullorinc = 'R'
  AND sess_retcode = '0'
  AND sess_log_time >= trunc( sysdate );


SELECT * FROM odi_app.etl_blyrndwh_tabincdays;

SELECT CASE
           WHEN substr( 'I_MAP_COUPON_SELECTION_V1', 1, 1 ) IN ('I', 'F') THEN substr( 'I_MAP_COUPON_SELECTION_V1', 3,
                                                                                       length( 'I_MAP_COUPON_SELECTION_V1' ) -
                                                                                       2 )
           ELSE 'I_MAP_COUPON_SELECTION_V1'
       END
FROM dual;

SELECT trunc( max( sess_endtime ) ) - 17 -- into v_lastsuccessdt
FROM odi_app.etl_blynrdwh_log
WHERE regexp_like( sess_mappingname, '^[IF]\_' || replace( 'MAP_COUPON_SELECTION_V1', '_', '\_' ) || '$' )
  AND sess_status = 'S'
  AND (sess_retcode = '0' OR sess_retcode IS NULL)
  AND sess_log_time >= sysdate - 60;