SELECT fn_ctl_load(nextval('sys_load_log_id_seq'),8,'Eti_api_answers_20210303.csv',2,'202102') AS insert;
SELECT fn_ctl_scheduled_load(208,3612,208,'DD_SURVEYDATA_202309_209.csv','bekir')
SELECT rplus.log_upd( nextval('sys_load_log_id_seq'), 'test' );

SELECT get_medialist( 1136, 37, '202101' );

SELECT get_medialist(1082,32,'202001');

SELECT get_user_params(1082);

SELECT rplus.get_pointkpi( 32, '202001', '10' );

CALL rplus.pop_auth( 2 );

CALL rplus.clear_prev_period( 'stg_api_answer', 28, '202001', '1' );

CALL rpsurvey.create_period(2,'f1','2022-06-05','2022-09-05','Q',3,'bekir');
CALL rpsurvey.create_schedule(2,'f1','bekir','2020-01-01','2025-01-01',TRUE );

CALL file_stage( 'rplus','stg_survey', '/apps/content/data/incoming/DD_SURVEYDATA_202309_209.csv' );

SELECT change_report_order( 32::bigint, 5::bigint, 5::bigint, 271::bigint, 4::bigint, 11::smallint );

PERFORM log_upd( 544, format( '''deneme ok %s ssdfsdf --> %s  %s ''', to_char( current_timestamp, 'YYYY.MM.DD HH24:MI:SS' ), 'sdfs', chr( 10 ) ) );


