SET SEARCH_PATH = 'rpsurvey';

--jsondaki sırasıyla
--1
SELECT * FROM rpsurvey.period_form_section WHERE project_id = 170;
--2
SELECT * FROM rpsurvey.period_form_question WHERE project_id = 170;
--3
SELECT * FROM rpsurvey.survey WHERE project_id = 170;
--4
SELECT * FROM rpsurvey.period_unit WHERE project_id = 170;
--5
SELECT * FROM rpsurvey.period_person WHERE project_id = 170;
--6
SELECT * FROM rpsurvey.period_unit_person_rel WHERE project_id = 170;
--7
SELECT * FROM rpsurvey.answer WHERE project_id = 170;
--8
SELECT * FROM rpsurvey.survey_media WHERE project_id = 170;



DO
$$
    BEGIN
        PERFORM log_upd( 544, format( '''deneme ok %s ssdfsdf --> %s  %s ''', to_char( current_timestamp, 'YYYY.MM.DD HH24:MI:SS' ), 'sdfs', chr( 10 ) ) );
    END
$$;



SELECT *
FROM rpsurvey.schedule
WHERE 1 = 1
  AND project_id = 112
  AND form_qcd = '4'
  AND data_source_id = 3
  AND period_qcd = '202307'
  AND load_status <> 'LOADED';




--model main uery
SELECT s.survey_period as "Period"
     , e.entity_id as "Entity Id"
     , e.entity_name as "Entity Name"
     , k.kpi_name as "KPI Name"
     , Round(avg( s.metric1_value ),2) AS "Average Value"
FROM rplus.srv_survey_kpi s, srv_entity e, app_user_auth u, srv_kpi k
WHERE s.survey_id = u.survey_id
  AND s.project_id = u.project_id
  AND k.project_id = s.project_id
  AND s.project_id = e.project_id
  AND s.entity_id = e.entity_id
  AND s.kpi_cd = k.kpi_cd
  AND u.project_id =140
  AND s.survey_period ='202301'-- fn_get_current_period( @@projectid@@ ) @@filtre@@
GROUP BY s.survey_period
       , e.entity_id
       , e.entity_name
       , k.kpi_name