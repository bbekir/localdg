 SELECT           l.load_plan_name as lp_name
                , i.i_lp_inst instance
                , g.nb_run runNo
                , LEVEL AS lvl
                , LPAD (' ', 5 * (LEVEL - 1)) || s.lp_step_name  AS step
--                 , CONNECT_BY_ROOT (s.i_lp_step) root_step_id
                , s.i_lp_step id
--                 , s.par_i_lp_step par_step_id
                , CASE
                     WHEN s.lp_step_type = 'SE' THEN 'Serial'
                     WHEN s.lp_step_type = 'PA' THEN 'Parallel'
                     WHEN s.lp_step_type = 'CS' THEN 'Case'
                     WHEN s.lp_step_type = 'CW' THEN 'Case'
                     WHEN s.lp_step_type = 'CE' THEN 'Case'
                     WHEN s.lp_step_type = 'RS' THEN 'Scenario'
                     WHEN s.lp_step_type = 'EX' THEN 'Exception'
                     ELSE NULL
                  END
                     AS type
                , CASE
                     WHEN s.restart_type = 'SF' THEN 'From Failure'
                     WHEN s.restart_type = 'PA' THEN 'All Children'
                     WHEN s.restart_type = 'PF' THEN 'Failed Children'
                     WHEN s.restart_type = 'RN' THEN 'From new session-Scen'
                     WHEN s.restart_type = 'RT' THEN 'From failed task-Scen'
                     WHEN s.restart_type = 'RS' THEN 'From failed step-Scen'
                  END
                     AS restart_type
--                 , CASE WHEN s.ind_enabled = 1 THEN 'ENABLED' WHEN s.ind_enabled = 0 THEN 'DISABLED' END  AS is_enabled
                , s.scen_name AS scenario
                , s.scen_version as v
                , g.sess_no sessNo
                , CASE
                     WHEN g.status = 'D' THEN 'Done'
                     WHEN g.status = 'E' THEN 'ERROR!'
                     WHEN g.status = 'W' THEN 'Waiting'
                     WHEN g.status = 'R' THEN 'Running'
                     WHEN g.status = 'M' THEN 'Warning'
                  END
                     AS status
                ,trunc(g.start_date,'mi') as start_date
                , trunc(g.end_date,'mi')  as end_date
                , (g.end_date - g.start_date ) as duration_min
                , g.nb_row AS row_cnt
                , g.nb_ins AS insert_cnt
                , g.nb_del AS delete_cnt
                , g.nb_upd AS update_cnt
                , g.nb_err AS error_cnt
                , g.error_message
                , g.return_code
--                , g.i_txt_mess
                , g.sess_keywords
                , CASE
                     WHEN s.except_behavior = 'R' THEN 'Excp.&Raise'
                     WHEN s.except_behavior = 'I' THEN 'Excp.&Ignore'
                  END
                     AS except
                , s.step_priority as priority
                , s.step_order as ord
                , s.step_timeout as timeout
                , s.var_name || ' ' || ' ' || s.var_op || s.var_value AS var_condition
                , s.max_par_error
                , s.sess_keywords
                , s.var_long_value
             FROM  snp_load_plan l
                ,  snp_lp_inst i
                ,  snp_lpi_step s
                ,  snp_lpi_step_log g
            WHERE     1 = 1
                  AND l.i_load_plan = i.i_load_plan
                  AND i.i_lp_inst = s.i_lp_inst
                  AND s.i_lp_inst = g.i_lp_inst(+)
                  AND s.i_lp_step = g.i_lp_step(+)
                  AND g.start_date >= to_date('20230615','yyyymmdd')
                  AND g.start_date < to_date('20230616','yyyymmdd')
       START WITH s.par_i_lp_step IS NULL --and i.i_lp_inst=1091499
       CONNECT BY PRIOR g.i_lp_step = s.par_i_lp_step
                    AND g.i_lp_inst = PRIOR g.i_lp_inst
                    AND g.nb_run = PRIOR g.nb_run
ORDER SIBLINGS BY s.step_order;











select PARTITION_NAME,num_rows, p.* from all_tab_partitions p where table_name='CUSTOMER_BONUS_DAILY';