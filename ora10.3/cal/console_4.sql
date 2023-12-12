CREATE OR REPLACE PACKAGE ODIETL.Plib AUTHID CURRENT_USER
IS
   -- aciklamalar icin body' e bakiniz....
   --
   c_max_date             DATE              := TO_DATE ('31122100', 'DDMMYYYY');
   c_min_date             DATE              := TO_DATE ('01011994', 'DDMMYYYY');
   c_bscs                 NUMBER            ;  -- package body init kismainda set ediliyor
   c_prep                 NUMBER            ;  -- package body init kismainda set ediliyor
   c_collection           NUMBER            ;  -- package body init kismainda set ediliyor
   c_normal               NUMBER            := 0;
   c_error                NUMBER            := -1;
   c_tmp_def_tablespace   VARCHAR2 (30)     := 'DWH_STAGING_TEMP';
   c_prm_def_tablespace   VARCHAR2 (30)     := 'DWH_STAGING_PERM';
   c_sp2db                NUMBER            := 17 ;
   c_run_id                  NUMBER            := 0; -- Log objesine run_id gecirmek icin

   -- call data recycle reason constatnts
   c_rec_ettdate             CONSTANT VARCHAR2 (1) := 'A';
   c_rec_rule                CONSTANT VARCHAR2 (1) := 'B';
   c_rec_call_type           CONSTANT VARCHAR2 (1) := 'C';
   c_rec_subscriber          CONSTANT VARCHAR2 (1) := 'D';
   c_rec_distance            CONSTANT VARCHAR2 (1) := 'F';
   c_rec_rate_band_id        CONSTANT VARCHAR2 (1) := 'G';
   c_rec_traffic_id          CONSTANT VARCHAR2 (1) := 'H';
   c_rec_tariff_id           CONSTANT VARCHAR2 (1) := 'J';
   c_rec_subs_type_id        CONSTANT VARCHAR2 (1) := 'N';
   c_rec_payment_opt_id      CONSTANT VARCHAR2 (1) := 'O';

   c_rec_network             CONSTANT VARCHAR2 (1) := 'P';
   c_rec_cell_location       CONSTANT VARCHAR2 (1) := 'Q';
   c_rec_equipment_type      CONSTANT VARCHAR2 (1) := 'R';
   c_rec_time_band           CONSTANT VARCHAR2 (1) := 'S';
   c_rec_termination_type    CONSTANT VARCHAR2 (1) := 'T';
   c_rec_destination         CONSTANT VARCHAR2 (1) := 'V';
   c_rec_service              CONSTANT VARCHAR2 (1) := 'Y';

   --PROCEDURE Report(P_Modul IN VARCHAR2, P_SubModul IN VARCHAR2,  P_Message IN VARCHAR2, P_Tur IN VARCHAR2 DEFAULT 'YOK') ;
   PROCEDURE drop_table (piv_owner IN VARCHAR2, piv_tablename IN VARCHAR2);
   PROCEDURE  drop_mview(   piv_owner     IN VARCHAR2, piv_mviewname IN VARCHAR2);
   PROCEDURE truncate_table (piv_owner IN VARCHAR2, piv_tablename IN VARCHAR2);

   PROCEDURE drop_partition (piv_owner IN VARCHAR2, piv_tablename IN VARCHAR2, piv_part_name IN VARCHAR2);

   PROCEDURE truncate_partition (piv_owner IN VARCHAR2, piv_tablename IN VARCHAR2, piv_part_name IN VARCHAR2);

   PROCEDURE enable_row_movement (piv_owner IN VARCHAR2, piv_tablename IN VARCHAR2);

   PROCEDURE disable_row_movement (piv_owner IN VARCHAR2, piv_tablename IN VARCHAR2);

-- *************************************************************************************************
   PROCEDURE add_column (piv_owner IN VARCHAR2, piv_tablename IN VARCHAR2, piv_column IN VARCHAR2, piv_type IN VARCHAR2);

   FUNCTION get_column_names(pin_owner VARCHAR2, pin_table_name VARCHAR2) RETURN LONG;

   PROCEDURE gather_stat ( piv_owner        IN VARCHAR2
                        , piv_tname        IN VARCHAR2
                        , pin_est_per      IN NUMBER DEFAULT 1
                        , pin_degree       IN NUMBER DEFAULT 8
                        , pin_granularity  IN VARCHAR2 DEFAULT NULL
                        , pin_part_name    IN VARCHAR2 DEFAULT NULL
                        , pin_method_opt   IN VARCHAR2 DEFAULT NULL
                        , pin_cascade      IN VARCHAR2  DEFAULT 'TRUE') ;


   PROCEDURE gather_stat2 ( piv_owner        IN VARCHAR2
                        , piv_tname        IN VARCHAR2
                        , pin_est_per      IN NUMBER DEFAULT 1
                        , pin_degree       IN NUMBER DEFAULT 8
                        , pin_granularity  IN VARCHAR2 DEFAULT NULL
                        , pin_part_name    IN VARCHAR2 DEFAULT NULL
                        , pin_method_opt   IN VARCHAR2 DEFAULT NULL) ;

  PROCEDURE set_stat (
      piv_owner                  IN       VARCHAR2
    , piv_tablename                  IN       VARCHAR2
    , pin_nof_rows               IN       NUMBER
    , pin_nof_blocks             IN       NUMBER
   ) ;


   FUNCTION object_exist (piv_owner IN VARCHAR2, piv_objectname IN VARCHAR2)
      RETURN BOOLEAN;

   PROCEDURE execute_with_log(pin_command IN VARCHAR2);

   PROCEDURE enable_parallel_dml;

   PROCEDURE disable_parallel_dml;

   PROCEDURE enable_parallel_query;

   PROCEDURE disable_parallel_query;

   PROCEDURE set_parallel_group (piv_group VARCHAR2);

   PROCEDURE set_hash_size (ar_hash_size NUMBER);

   PROCEDURE set_sort_size (ar_hash_size NUMBER);

   PROCEDURE set_parallel_min_percent (p_percentage NUMBER DEFAULT 100);

   PROCEDURE skip_unusable_indexes_true;

   PROCEDURE skip_unusable_indexes_false;

   PROCEDURE parallel_broadcast_false;

   PROCEDURE parallel_broadcast_true;

   PROCEDURE increase_buffer_size (v_buffsize IN NUMBER);

   PROCEDURE blankremover (piv_coming IN OUT VARCHAR2);

   PROCEDURE translate_tc (piv_coming IN OUT VARCHAR2);

   FUNCTION  translate_tc (piv_Coming  IN VARCHAR2, piv_Case IN VARCHAR2 DEFAULT 'N') RETURN VARCHAR2;

   PROCEDURE create_index (piv_user IN VARCHAR2, piv_indname IN VARCHAR2, piv_indsql IN VARCHAR2);

   PROCEDURE make_not_null (p_owner VARCHAR2, p_table_name VARCHAR2, p_col_name VARCHAR2);

   PROCEDURE make_not_null_wtrashold (
      p_ett_date                 IN       DATE
    , p_process_name             IN       VARCHAR2
    , p_table_owner              IN       VARCHAR2
    , p_table_name               IN       VARCHAR2
    , p_table_column             IN       VARCHAR2
    , p_treshold                 IN       NUMBER
    , p_rc                       OUT      NUMBER
    , p_ex_table_owner           IN       VARCHAR2 DEFAULT 'SCHDWH'
    , p_ex_table_name            IN       VARCHAR2 DEFAULT 'TTA01SUBS_DUP_KEYS_EXP');

   PROCEDURE check_dim_values (
      piv_sourcetab1             IN       VARCHAR2
    , piv_sourcecol1             IN       VARCHAR2
    , piv_sourcetab2             IN       VARCHAR2
    , piv_sourcecol2             IN       VARCHAR2
    , piv_targettab              IN       VARCHAR2
    , piv_text                   IN       VARCHAR2
    , piv_ts                     IN       VARCHAR2 DEFAULT NULL);

--   PROCEDURE p_create_external_table (p_owner IN VARCHAR2 DEFAULT USER, p_table_name IN VARCHAR2, p_result OUT NUMBER);

   PROCEDURE imp_hash_aj (
      p_owner                    IN       VARCHAR2 DEFAULT USER
    , p_table_name               IN       VARCHAR2
    , p_column_name              IN       VARCHAR2
    , p_haj_owner                IN       VARCHAR2 DEFAULT USER
    , p_haj_table_name           IN       VARCHAR2
    , p_haj_column_name          IN       VARCHAR2
    , p_ins_owner                IN       VARCHAR2 DEFAULT USER
    , p_ins_table_name           IN       VARCHAR2
    , p_result                   OUT      NUMBER);

   PROCEDURE tekrarkontrol (p_pck_name IN VARCHAR2, p_prc_name IN VARCHAR2, p_ett_date IN DATE);

   PROCEDURE tekrarkontrolson (p_pck_name IN VARCHAR2, p_prc_name IN VARCHAR2, p_ett_date IN DATE);

   FUNCTION check_duplicate (
      p_table_owner              IN       VARCHAR2
    , p_table_name               IN       VARCHAR2
    , p_group_by                 IN       VARCHAR2
    , p_dup_res_table_owner      IN       VARCHAR2
    , p_dup_res_table_name       IN       VARCHAR2)
      RETURN NUMBER;

   FUNCTION get_ett_date (p_owner IN VARCHAR2)   RETURN DATE;

   FUNCTION get_param_value (p_owner IN VARCHAR2, p_name IN VARCHAR2)
      RETURN VARCHAR2;

   PROCEDURE set_param_value(p_owner IN VARCHAR2, p_name IN VARCHAR2, p_value IN VARCHAR2);

   FUNCTION get_param_value2 (p_name IN VARCHAR2, src_date IN DATE DEFAULT SYSDATE)
      RETURN VARCHAR2;

   PROCEDURE set_param_value2 (p_name IN VARCHAR2, p_value IN VARCHAR2, src_date IN DATE DEFAULT SYSDATE);

   PROCEDURE create_unique_index (
      p_ett_date                 IN       DATE
    , p_process_name             IN       VARCHAR2
    , p_table_name               IN       VARCHAR2
    , p_table_owner              IN       VARCHAR2
    , p_index_name               IN       VARCHAR2
    , p_index_columns            IN       VARCHAR2
    , p_storage_clause           IN       LONG
    , p_treshold                 IN       NUMBER
    , p_rc                       OUT      NUMBER);

   PROCEDURE set_trace (p_trc_identifier IN VARCHAR2);

   PROCEDURE ett_date_formatted (
      p_owner                    IN       VARCHAR2
    , p_ymd                      OUT      VARCHAR2
    , p_ymm                      OUT      VARCHAR2
    , p_ymm_1                    OUT      VARCHAR2
    , p_ymm_2                    OUT      VARCHAR2
    , p_ymm_3                    OUT      VARCHAR2
    , p_ymm_4                    OUT      VARCHAR2
    , p_vym                      OUT      VARCHAR2);

--   PROCEDURE tpa11srefclause (p_table_name VARCHAR2, p_return OUT LONG);

   FUNCTION default_non_par (
     pin_tablespace_type        IN       VARCHAR2 DEFAULT 'T' -- T temporary, P permanent
   , pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
   , pin_parallel_degree        IN       NUMBER DEFAULT 16
   , pin_ini_trans              IN       NUMBER DEFAULT 1
   , pin_pct_free               IN       NUMBER DEFAULT 0
   , pin_pct_used               IN       NUMBER DEFAULT 99
   , pin_freelists              IN       NUMBER DEFAULT 1
   , pin_pct_increase           IN       NUMBER DEFAULT 0)
     RETURN LONG;


   FUNCTION default_hash (
      pin_hash_partition_columns IN       VARCHAR2
    , pin_part_cnt               IN       NUMBER DEFAULT 16
    , pin_tablespace_type        IN       VARCHAR2 DEFAULT 'T'                                          -- T temporary, P permanent
    , pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
    , pin_tsnum_start            IN       NUMBER DEFAULT 1
    , pin_tsnum_end              IN       NUMBER DEFAULT 16
    , pin_spread                 IN       BOOLEAN  DEFAULT TRUE
    , pin_parallel_degree        IN       NUMBER DEFAULT 16
    , pin_ini_trans              IN       NUMBER DEFAULT 1
    , pin_pct_free               IN       NUMBER DEFAULT 0
    , pin_pct_used               IN       NUMBER DEFAULT 99
    , pin_freelists              IN       NUMBER DEFAULT 1
    , pin_pct_increase           IN       NUMBER DEFAULT 0)
      RETURN LONG;

FUNCTION default_range_or_comp(
     pin_range_part_col_name      IN       VARCHAR2
   , pin_range_part_col_type      IN       VARCHAR2 -- DATE, NUMBER, VARCHAR2
   , pin_range_part_col_vals      IN       VARCHAR2 DEFAULT NULL -- SEPARETED WITH COMMA OR SPACE
   , pin_range_date_start         IN       DATE DEFAULT TRUNC(SYSDATE-20)
   , pin_range_date_end           IN       DATE DEFAULT TRUNC(SYSDATE)
   , pin_range_date_granularity   IN       VARCHAR2 DEFAULT 'D' -- 'D' : GUN, 'M' : AY, 'Y' : YIL
   , pin_subpart_hash_columns     IN       VARCHAR2 DEFAULT NULL
   , pin_subpart_count            IN       NUMBER DEFAULT NULL
   , pin_tablespace_type          IN       VARCHAR2 DEFAULT 'T' -- T temporary, P permanent
   , pin_default_tablespace       IN       VARCHAR2 DEFAULT NULL
   , pin_tsnum_start              IN       NUMBER DEFAULT 1
   , pin_tsnum_end                IN       NUMBER DEFAULT 16
   , pin_parallel_degree          IN       NUMBER DEFAULT 16
   , pin_ini_trans                IN       NUMBER DEFAULT 1
   , pin_pct_free                 IN       NUMBER DEFAULT 0
   , pin_pct_used                 IN       NUMBER DEFAULT 99
   , pin_freelists                IN       NUMBER DEFAULT 1
   , pin_pct_increase             IN       NUMBER DEFAULT 0
   ,  pin_date_number_based       IN       BOOLEAN  DEFAULT FALSE)
    RETURN LONG;

 PROCEDURE add_date_partition (
     piv_owner                  IN       VARCHAR2
   , piv_tablename              IN       VARCHAR2
   , max_date                   IN       DATE
   , pin_tablespace_type        IN       VARCHAR2 DEFAULT NULL -- NULL mevcutu kullan, T temporary, P permanent
   , pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
   , pin_spread                 IN       BOOLEAN  DEFAULT TRUE
   , pin_number_based           IN       BOOLEAN  DEFAULT FALSE
   , pin_tsnum_start            IN       NUMBER DEFAULT 1
   , pin_tsnum_end              IN       NUMBER DEFAULT 16
   , pin_part_prefix_replace    IN       VARCHAR2 DEFAULT 'P'
   , pin_dual_index                IN         BOOLEAN  DEFAULT FALSE
  );

  PROCEDURE drop_date_partition (
     piv_owner                  IN       VARCHAR2
   , piv_tablename              IN       VARCHAR2
   , pid_date_less_eq           IN       DATE
   , pin_part_prefix_replace    IN       VARCHAR2 DEFAULT 'P'
   , pin_dual_index                IN         BOOLEAN  DEFAULT FALSE
  );


    PROCEDURE window_date_partitions (
     piv_owner                  IN       VARCHAR2
   , piv_tablename              IN       VARCHAR2
   , pid_max_date               IN       DATE
   , pin_window_size            IN       NUMBER
   , pin_tablespace_type        IN       VARCHAR2 DEFAULT NULL -- NULL mevcutu kullan, T temporary, P permanent
   , pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
   , pin_spread                 IN       BOOLEAN  DEFAULT TRUE
   , pin_number_based           IN       BOOLEAN  DEFAULT FALSE
   , pin_tsnum_start            IN       NUMBER DEFAULT 1
   , pin_tsnum_end              IN       NUMBER DEFAULT 16
   , pin_part_prefix_replace    IN       VARCHAR2 DEFAULT 'P'
   , pin_dual_index                IN         BOOLEAN  DEFAULT FALSE
  );

  TYPE token_list IS VARRAY (100) OF VARCHAR2 (50);

  PROCEDURE TOKENIZE( p_input IN VARCHAR2
                     , l_tokens IN OUT token_list
                     , p_cnt OUT NUMBER
                     , p_delimiter IN VARCHAR2 DEFAULT NULL
                     , p_distinct BOOLEAN DEFAULT FALSE) ;

   o_log   log_type := log_type ( 'YES', USER, SYSDATE, NULL,NULL,'PLIB',NULL,'PLIB','PLIB',0,NULL,NULL,NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL );

   -- debug amacli
  PROCEDURE w (piv_message IN VARCHAR2);

  FUNCTION table_exist (piv_owner IN VARCHAR2, piv_tablename IN VARCHAR2)
      RETURN BOOLEAN;

  FUNCTION partition_exist
        (   piv_owner IN VARCHAR2,
            piv_tablename IN VARCHAR2 ,
            piv_partitionname IN VARCHAR2 ,
            piv_partition_type IN VARCHAR2 DEFAULT 'P'
         ) RETURN BOOLEAN ;

  FUNCTION record_exist
         (  piv_owner IN VARCHAR2,
            piv_tablename IN VARCHAR2 ,
            piv_predicates IN VARCHAR2 DEFAULT NULL
         ) RETURN BOOLEAN ;

  FUNCTION date_string (p_ett_date IN DATE) RETURN VARCHAR2;





  PROCEDURE sch_mail ( p_package_owner  IN VARCHAR2,
                       p_package_name  IN VARCHAR2,
                       p_procedure_name IN  VARCHAR2,
                       p_message IN VARCHAR2);


  PROCEDURE send_mail ( p_from_name VARCHAR2,
                        p_to_name VARCHAR2,
                        p_subject VARCHAR2,
                        p_message VARCHAR2);




  FUNCTION subpartitions_select_string(
       pin_table_owner         IN VARCHAR2
     , pin_table_name          IN VARCHAR2
     , pin_subpartition_order  IN NUMBER)
     RETURN LONG ;


  FUNCTION get_last_partition (pin_table_name IN VARCHAR2, pin_owner IN VARCHAR2, pin_date IN DATE) RETURN VARCHAR2;

  FUNCTION get_module_max_ett_date(p_scope VARCHAR2, p_module IN VARCHAR2) RETURN DATE;
  FUNCTION get_module_max_finished_date(p_scope VARCHAR2, p_module IN VARCHAR2) RETURN DATE;
--  FUNCTION is_module_finished(p_scope VARCHAR2 , p_module IN VARCHAR2, p_ett_date DATE) RETURN BOOLEAN ;
  --------------------------------------------------------------

   PROCEDURE make_indexes_unusable (
      piv_owner                  IN       VARCHAR2
    , piv_table_name             IN       VARCHAR2
    , piv_index_name             IN       VARCHAR2 DEFAULT NULL
    , piv_part_name              IN       VARCHAR2 DEFAULT NULL);

   PROCEDURE make_indexes_rebuild (
      piv_owner                  IN       VARCHAR2
    , piv_table_name             IN       VARCHAR2
    , piv_index_name             IN       VARCHAR2 DEFAULT NULL
    , piv_part_name              IN       VARCHAR2 DEFAULT NULL
    , piv_parallel_degree        IN       NUMBER DEFAULT 8);

   PROCEDURE part_ind_rebuild_concurrent
                               (  p_tab_name       IN VARCHAR2 ,
                                  p_idx_name       IN VARCHAR2 ,
                           p_jobs_per_batch IN NUMBER DEFAULT 1,
                                  p_procs_per_job  IN NUMBER DEFAULT 1,
                                  p_force_opt      IN BOOLEAN DEFAULT FALSE);



    PROCEDURE window_date_partitions_reeng (
     piv_owner                  IN       VARCHAR2
   , piv_tablename              IN       VARCHAR2
   , pid_max_date               IN       DATE
   , pin_window_size            IN       NUMBER
   , pin_tablespace_type        IN       VARCHAR2 DEFAULT NULL -- NULL mevcutu kullan, T temporary, P permanent
   , pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
   , pin_spread                 IN       BOOLEAN  DEFAULT TRUE
   , pin_number_based           IN       BOOLEAN  DEFAULT FALSE
   , pin_tsnum_start            IN       NUMBER DEFAULT 1
   , pin_tsnum_end              IN       NUMBER DEFAULT 16
   , pin_part_prefix_replace    IN       VARCHAR2 DEFAULT 'P'
   , pin_dual_index                IN         BOOLEAN  DEFAULT FALSE
  );

--  change_constraint_status
--  ARGUMENTS
--  piv_action      : can be either DISABLE or ENABLE
--  piv_owner       : schema name
--  piv_table_name  : table name
--  piv_constr_type : constraint type P 'Primary Key', C 'Check' and others ..
--  piv_constr_name : name of the constraint eg. SYS_C0010539

--  EXAMPLE USAGE

--  change_constraint_status('ENABLE', 'ODIETL', 'SUBS_LAST_PARTY', 'C')
--  enables all Check constraints on ODIETL.SUBS_LAST_PARTY

--  change_constraint_status('DISABLE', 'ODIETL', 'SUBS_LAST_PARTY', NULL, 'SYS_C0010539' )
--  disables constraint SYS_C0010539 on ODIETL.SUBS_LAST_PARTY

--  change_constraint_status('DISABLE', 'ODIETL', 'SUBS_LAST_PARTY')
--  disables all constraints on ODIETL.SUBS_LAST_PARTY
PROCEDURE change_constraint_status (
      piv_action                 IN       VARCHAR2 -- DISABLE / ENABLE
    , piv_owner                  IN       VARCHAR2
    , piv_table_name             IN       VARCHAR2
    , piv_constr_type            IN       VARCHAR2 DEFAULT NULL -- C,U,P,..
    , piv_constr_name            IN       VARCHAR2 DEFAULT NULL) ;

PROCEDURE reset_sequence (
         piv_owner        IN   VARCHAR2
      ,    piv_seq_name     IN   VARCHAR2
      ,    piv_start_value  IN   NUMBER DEFAULT 1
   );

END Plib;
/

CREATE OR REPLACE PACKAGE BODY ODIETL.Plib IS
------------------------------------------------------------------------------------------------
-- PLIB v1.1 build 55
-------------------------------------------------------------------------------------------------
--  Mustafa Kandemir
--  Oracle/Turkiye
--
--  LAST UPDATE :  19 Agustos 2004
--

  -- ------------------------------------------------------------
  -- Global variables
  -- ------------------------------------------------------------
  v_user              VARCHAR2(30);          -- current user
  gv_proc             VARCHAR2(50);          -- name of tyhe current procedure

  -- error handling variables
  gv_CallStack         VARCHAR2(2000);        -- Call stak fro dbms_utiliy.format_call_stack
  gv_errorCode        NUMBER;                -- Code for the error
  gv_errorMsg         VARCHAR2(200);         -- Message text for the error
  gv_currentUser      VARCHAR2(8);           -- Current database user
  gv_information      VARCHAR2(200);         -- Information about the error
  gn_error               NUMBER := 1;             -- Execution ended with error

  c_control_table     VARCHAR2(80) := 'ANT.GTEKRARKONTROL';
  v_dyntask           VARCHAR2(32000);          -- used for dynamic sql statements

  c_delim             VARCHAR2(10) := ' : ';          -- name of tyhe current procedure
  c_pck               VARCHAR2(30) := 'PLIB';          -- name of tyhe current procedure
  c_crlf              VARCHAR2 (20) := CHR (13) || CHR (10);
  c_tab               VARCHAR2(1) := CHR(9);

  c_module_notrunning VARCHAR2(100):='NOTRUNNING'; -- EUL veya ETL hic baslamadi
  c_module_running    VARCHAR2(100):='RUNNING';    -- EUL veya ETL basldai
  c_module_finished   VARCHAR2(100):='FINISHED';   -- EUL veya ETL bitti


 FUNCTION date_string (p_ett_date IN DATE) RETURN VARCHAR2
 IS
 BEGIN
    RETURN ' TO_DATE(''' || TO_CHAR(p_ett_date,'DDMMYYYY') || ''',''DDMMYYYY'') ';
 END;
 --------------------------------------------------------------------------
   PROCEDURE reset_sequence (
         piv_owner        IN   VARCHAR2
      ,    piv_seq_name     IN   VARCHAR2
      ,    piv_start_value  IN   NUMBER DEFAULT 1
   )
   AS
      cval     INTEGER;
      inc_by   VARCHAR2 (25);
   BEGIN
      EXECUTE IMMEDIATE 'ALTER SEQUENCE ' ||piv_owner ||'.'|| piv_seq_name || ' MINVALUE 0';

      EXECUTE IMMEDIATE 'SELECT ' || piv_owner ||'.'|| piv_seq_name || '.NEXTVAL FROM dual'
                   INTO cval;

      cval := cval - piv_start_value + 1;

      IF cval < 0
      THEN
         inc_by := ' INCREMENT BY ';
         cval := ABS (cval);
      ELSE
         inc_by := ' INCREMENT BY -';
      END IF;

      EXECUTE IMMEDIATE 'ALTER SEQUENCE ' || piv_owner ||'.'|| piv_seq_name || inc_by || cval;

      EXECUTE IMMEDIATE 'SELECT ' || piv_owner||'.'||piv_seq_name || '.NEXTVAL FROM dual'
                   INTO cval;

      EXECUTE IMMEDIATE 'ALTER SEQUENCE ' || piv_owner ||'.'|| piv_seq_name || ' INCREMENT BY 1';

      COMMIT;
   END;

  --------------------------------------------------------------
  --
  --   TOKENIZE
  --
  --
  --
  --
  --
  --------------------------------------------------------------

   PROCEDURE TOKENIZE( p_input IN VARCHAR2
                     , l_tokens IN OUT token_list
                     , p_cnt OUT NUMBER
                     , p_delimiter IN VARCHAR2 DEFAULT NULL
                     , p_distinct BOOLEAN DEFAULT FALSE ) IS
   i             NUMBER;
   ptr           NUMBER;
   t             VARCHAR2(4000);
   l             NUMBER;
   c             CHAR;
   b_token_start BOOLEAN;
   b_distinct_check BOOLEAN;
   v_token       VARCHAR2(500);
   n_token_cnt   NUMBER;
 BEGIN

   T := UPPER(P_INPUT);
   l := LENGTH(T);

   l_tokens := token_list();
   ptr := 1;
   n_token_cnt := 0;
   b_token_start := FALSE;

   -- loop until the length of input string is reached
   WHILE ptr <= l LOOP
       c := SUBSTR(T, ptr, 1);

       IF p_delimiter IS NULL THEN
           IF  (C >= 'A' AND C <= 'Z')  OR (C >= '0' AND C <= '9')  OR C IN ('Ö','Ü','Ç','?','I','?','?') THEN
                 -- a valid char for a tokenization
                 IF b_token_start = FALSE THEN  -- the first valid char, start of token
                    b_token_start := TRUE;
                    v_token := c;
                 ELSE  -- no not the first character, extend the current token
                    v_token := v_token || c;
               END IF;
           ELSE
               IF b_token_start = TRUE THEN -- the token extension ends here
                  b_distinct_check:=TRUE;
                  IF p_distinct THEN
                     FOR j IN 1..n_token_cnt LOOP
                        IF l_tokens(j)=v_token THEN
                           b_distinct_check:=FALSE;
                           EXIT;
                        END IF;
                     END LOOP;
                  END IF;
                  IF b_distinct_check THEN
                    n_token_cnt := n_token_cnt + 1;
                    l_tokens.EXTEND();
                    l_tokens(n_token_cnt) := v_token;  -- put the current token to list
                  END IF;
                  v_token := NULL;
                  b_token_start := FALSE;
               END IF;
          END IF;
       ELSE -- p_delimiter NULL dan farkli ise kismi
           IF INSTR(p_delimiter, C) =  0 THEN
                 -- a valid char for a tokenization
                 IF b_token_start = FALSE THEN  -- the first valid char, start of token
                    b_token_start := TRUE;
                    v_token := c;
                 ELSE  -- no not the first character, extend the current token
                    v_token := v_token || c;
               END IF;
           ELSE
               IF b_token_start = TRUE THEN -- the token extension ends here
                  b_distinct_check:=TRUE;
                  IF p_distinct THEN
                     FOR j IN 1..n_token_cnt LOOP
                        IF l_tokens(j)=v_token THEN
                           b_distinct_check:=FALSE;
                           EXIT;
                        END IF;
                     END LOOP;
                  END IF;
                  IF b_distinct_check THEN
                    n_token_cnt := n_token_cnt + 1;
                    l_tokens.EXTEND();
                    l_tokens(n_token_cnt) := v_token;  -- put the current token to list
                  END IF;
                  v_token := NULL;
                  b_token_start := FALSE;
               END IF;
          END IF;
       END IF;
      -- increment the source string pointer
      ptr := ptr + 1;
   END LOOP;
   IF b_token_start = TRUE THEN  -- the first valid char, start of token
                  b_distinct_check:=TRUE;
                  IF p_distinct THEN
                     FOR j IN 1..n_token_cnt LOOP
                        IF l_tokens(j)=v_token THEN
                           b_distinct_check:=FALSE;
                           EXIT;
                        END IF;
                     END LOOP;
                  END IF;
                  IF b_distinct_check THEN
                    n_token_cnt := n_token_cnt + 1;
                    l_tokens.EXTEND();
                    l_tokens(n_token_cnt) := v_token;  -- put the current token to list
                  END IF;
   END IF;
   P_CNT := n_token_cnt;
   RETURN;
  END;




  --------------------------------------------------------------
  --
  --   get_column_names
  --
  -- return the list of columns.
  -- if table not found returns null
  -- Orig. AY
  --
  --------------------------------------------------------------
  FUNCTION get_column_names(pin_owner VARCHAR2, pin_table_name VARCHAR2) RETURN LONG IS
  v_cols    LONG;
  i        PLS_INTEGER;
  v_col_name  VARCHAR2(30);

  CURSOR cur_cols IS
   SELECT LOWER(column_name) column_name
  FROM ALL_TAB_COLUMNS
  WHERE owner = pin_owner
    AND table_name = UPPER(pin_table_name)
    ORDER BY column_id;
  BEGIN
    i := 0;
    v_cols := NULL;

    OPEN cur_cols;
    LOOP
      FETCH  cur_cols INTO v_col_name;
    EXIT WHEN cur_cols%NOTFOUND;
        IF i = 0 THEN
        v_cols := c_tab || ' ' || v_col_name || c_crlf;
      ELSE
        v_cols := v_cols || c_tab || ',' || v_col_name || c_crlf;
      END IF;
      i := i +1;
    END LOOP;

    CLOSE cur_cols;

    RETURN v_cols;
  END;

  --------------------------------------------------------------
  --
  --   drop_table
  --
  --   drops a table(piv_tablename owned by piv_owner)
  --   if exists, if not, does nothing and does not give
  --   any error
  --
  --------------------------------------------------------------
  PROCEDURE  drop_table(   piv_owner     IN VARCHAR2,
                           piv_tablename IN VARCHAR2
                       ) IS
  cnt  INTEGER;
  Rc   NUMBER;
  BEGIN

    gv_proc  := c_pck || '.drop_table' ;

    SELECT COUNT(piv_tablename)
      INTO cnt
      FROM ALL_TABLES
     WHERE table_name = UPPER(piv_tablename)
       AND owner = UPPER(piv_owner) ;

    IF cnt > 0 THEN
       v_dyntask := 'DROP TABLE ' || piv_owner || '.' || piv_tablename;
       EXECUTE IMMEDIATE v_dyntask;
       Plib.o_log.LOG(6, 4,  gv_proc || c_delim || 'tablo drop edildi', piv_owner || '.' || piv_tablename, NULL,v_dyntask );
    ELSE
       Plib.o_log.LOG(6, 4,  gv_proc || c_delim || 'tablo bulunamadi', piv_owner || '.' || piv_tablename, NULL,NULL );
    END IF;

 EXCEPTION
  WHEN OTHERS THEN
    NULL;
  END;
  --------------------------------------------------------------
  --
  --   drop_mview
  --
  --   drops a materialized view(piv_tablename owned by piv_owner)
  --   if exists, if not, does nothing and does not give
  --   any error
  --

  --------------------------------------------------------------
  --
  --   drop_or_trunc_partition
  --   Not called directly
  --   19/01/2004 MK
  --
  --------------------------------------------------------------
  PROCEDURE Truncate_table(   piv_owner     IN VARCHAR2
                            , piv_tablename IN VARCHAR2
  ) IS
  BEGIN

     gv_proc  := c_pck || '.truncate_table' ;

     v_dyntask := 'TRUNCATE TABLE ' || piv_owner || '.' ||  piv_tablename ;
     EXECUTE IMMEDIATE v_dyntask;
     Plib.o_log.LOG(8, gv_proc || c_delim ||  piv_owner  || '.' || piv_tablename , NULL,v_dyntask );

    EXCEPTION
      WHEN OTHERS THEN
         Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
         RAISE;
  END;



  --------------------------------------------------------------
  --
  --   drop_or_trunc_partition
  --   Not called directly
  --   19/01/2004 MK
  --
  --------------------------------------------------------------
  PROCEDURE  drop_or_trunc_partition( piv_owner     IN VARCHAR2
                                    , piv_tablename IN VARCHAR2
                                    , piv_part_name IN VARCHAR2
                                    , piv_dt        IN VARCHAR2) IS
  partition_not_found EXCEPTION;
  PRAGMA EXCEPTION_INIT(partition_not_found, -20100);
  cnt  INTEGER;
  Rc   NUMBER;
  BEGIN
     gv_proc  := c_pck || '.drop_or_trunc_partition';
    -- if partition found return 1, otherwise return 0
    SELECT COUNT(piv_tablename)
      INTO cnt
      FROM ALL_TAB_PARTITIONS
     WHERE table_name = UPPER(piv_tablename)
       AND table_owner = UPPER(piv_owner)
       AND partition_name = UPPER(piv_part_name) ;

    IF cnt > 0 THEN
       IF piv_dt = 'D' THEN
              v_dyntask := 'ALTER TABLE ' || piv_owner || '.' ||  piv_tablename || ' DROP PARTITION ' || piv_part_name;
             EXECUTE IMMEDIATE v_dyntask;
           Plib.o_log.LOG(16,  gv_proc || c_delim ||  piv_owner  || '.' || piv_tablename || '.' || piv_part_name , NULL,v_dyntask );

       ELSE
              v_dyntask := 'ALTER TABLE ' || piv_owner || '.' ||  piv_tablename || ' TRUNCATE PARTITION ' || piv_part_name;
             EXECUTE IMMEDIATE v_dyntask;
           Plib.o_log.LOG(18, gv_proc || c_delim ||  piv_owner  || '.' || piv_tablename || '.' || piv_part_name , NULL,v_dyntask );
       END IF;
    ELSE
       RAISE partition_not_found ;
    END IF;
  EXCEPTION
    WHEN partition_not_found THEN
       Plib.o_log.LOG(1, 1, gv_proc || c_delim || 'Partiton ' || piv_owner  || '.' || piv_tablename ||'.' || piv_part_name || ' bulunamadi ! ', piv_part_name, NULL,  v_dyntask);
       RAISE_APPLICATION_ERROR(-20100, gv_proc || c_delim || 'Partiton ' || piv_owner  || '.' || piv_tablename ||'.' || piv_part_name || ' bulunamadi ! ' );
    WHEN OTHERS THEN
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;



  ----------------------------------------------------------------
  --
  --   drop_partition
  --
  --   drops a partition of a table(piv_tablename owned by piv_owner)
  --   if partition not exists raises an error
  --   caller has to handle error
  --
  --   19/01/2004 MK
  --------------------------------------------------------------
  PROCEDURE drop_partition( piv_owner     IN VARCHAR2
                          , piv_tablename IN VARCHAR2
                            , piv_part_name IN VARCHAR2) IS
  BEGIN
    drop_or_trunc_partition( piv_owner, piv_tablename,  piv_part_name, 'D');
  END;

  ----------------------------------------------------------------
  --
  --   truncate_partition
  --
  --   truncates a partition of a table(piv_tablename owned by piv_owner)
  --   if partition not exists raises an error
  --   caller has to handle error
  --
  --   19/01/2004 MK
  --------------------------------------------------------------
  PROCEDURE truncate_partition (
     piv_owner                  IN       VARCHAR2
   , piv_tablename              IN       VARCHAR2
   , piv_part_name              IN       VARCHAR2)
  IS
  BEGIN
     drop_or_trunc_partition (piv_owner, piv_tablename, piv_part_name, 'T');
  END;


  --------------------------------------------------------------
  --
  --   add_column
  --
  --   add column to table
  --
  --------------------------------------------------------------
  PROCEDURE  add_column ( piv_owner     IN VARCHAR2
                        , piv_tablename IN VARCHAR2
                        , piv_column    IN VARCHAR2
                        , piv_type      IN VARCHAR2) IS
  Rc  NUMBER;
  BEGIN
    gv_proc  := c_pck || '.add_column';
    v_dyntask := 'ALTER  TABLE ' || piv_owner || '.' || piv_tablename || ' ADD (' || piv_column || ' ' || piv_type || ')';
    EXECUTE IMMEDIATE v_dyntask ;
    Plib.o_log.LOG(7,  gv_proc || c_delim || piv_owner  || '.' || piv_tablename || '.' || piv_column , NULL,v_dyntask );

  EXCEPTION
    WHEN OTHERS THEN
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;


  --------------------------------------------------------------
  --
  --   gather_stat
  --
  --   Gather statistic using dbms_stats package
  --
  --------------------------------------------------------------
  PROCEDURE gather_stat ( piv_owner        IN VARCHAR2
                        , piv_tname        IN VARCHAR2
                        , pin_est_per      IN NUMBER DEFAULT 1
                        , pin_degree       IN NUMBER DEFAULT 8
                        , pin_granularity  IN VARCHAR2 DEFAULT NULL
                        , pin_part_name    IN VARCHAR2 DEFAULT NULL
                        , pin_method_opt   IN VARCHAR2 DEFAULT NULL
                        , pin_cascade      IN VARCHAR2  DEFAULT 'TRUE') IS
  BEGIN

     gv_proc  := c_pck || '.gather_stat';
     -- gather ststistics on created temporary table
     v_dyntask :=  'begin dbms_stats.gather_table_stats('''
                   || UPPER(piv_owner)
                   || ''', '''
                   || UPPER(piv_tname)
                   || ''', estimate_percent=> ' || TO_CHAR(pin_est_per,'FM990.00')
                   || ', degree=> ' || pin_degree ;
     IF pin_granularity IS NOT NULL THEN
           v_dyntask := v_dyntask || ', granularity=>''' || pin_granularity || '''';
     END IF;
     -- partition adi verilmis mi?
     IF pin_part_name  IS NOT NULL THEN
           v_dyntask := v_dyntask || ', partname=>''' || pin_part_name || '''';
     END IF;

     IF pin_method_opt IS NOT NULL THEN
       v_dyntask := v_dyntask || ', method_opt=>''' || pin_method_opt || '''';
     END IF;

    IF pin_cascade IS NOT NULL THEN
       v_dyntask := v_dyntask || ', cascade=> TRUE' ;
    END IF;


     v_dyntask := v_dyntask || '); end; ';

     -- execute the above statement
     EXECUTE IMMEDIATE v_dyntask;
     Plib.o_log.LOG(23, 4,  gv_proc || c_delim || ' islem OK.',  piv_owner  || '.' || piv_tname , NULL,v_dyntask );
  EXCEPTION
    WHEN OTHERS THEN
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;


  --------------------------------------------------------------
  --
  --   gather stat on a given table in ANT
  --
  --------------------------------------------------------------
  PROCEDURE gather_stat2 (piv_owner IN VARCHAR2
                       , piv_tname IN VARCHAR2
                       , pin_est_per IN NUMBER DEFAULT 1
                       , pin_degree  IN NUMBER DEFAULT 8
                       , pin_granularity  IN VARCHAR2 DEFAULT NULL
                       , pin_part_name    IN VARCHAR2 DEFAULT NULL
               , pin_method_opt   IN VARCHAR2 DEFAULT NULL
               ) IS
  BEGIN
     gv_proc  := c_pck || '.gather_stat2';
    -- gather ststistics on created temporary table
     v_dyntask :=  'begin dbms_stats.gather_table_stats('''
                   || UPPER(piv_owner)
                   || ''', '''
                   || UPPER(piv_tname)
                   || ''', estimate_percent=> ' || TO_CHAR(pin_est_per,'FM990.00')
                   || ', degree=> ' || pin_degree ;
     IF pin_granularity IS NOT NULL THEN
           v_dyntask := v_dyntask || ', granularity=>''' || pin_granularity || '''';
     END IF;
     -- partition adi verilmis mi?
     IF pin_part_name  IS NOT NULL THEN
           v_dyntask := v_dyntask || ', partname=>''' || pin_part_name || '''';
     END IF;

    IF pin_method_opt IS NOT NULL THEN
       v_dyntask := v_dyntask || ', method_opt=>''' || pin_method_opt || '''';
    END IF;
    v_dyntask := v_dyntask || ', cascade=> TRUE' ;


     v_dyntask := v_dyntask || '); end; ';

     EXECUTE IMMEDIATE v_dyntask;
--      ANT.Ant_Exec( v_dyntask );
      Plib.o_log.LOG(23, 4,  gv_proc || c_delim || ' islem OK.',  piv_owner  || '.' || piv_tname , NULL,v_dyntask );
  EXCEPTION
    WHEN OTHERS THEN
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;

  --------------------------------------------------------------
  --
  --   set_stat
  --
  --   Set statistics using dbms_stats package
  --
  --------------------------------------------------------------
  PROCEDURE set_stat (
      piv_owner                  IN       VARCHAR2
    , piv_tablename              IN       VARCHAR2
    , pin_nof_rows               IN       NUMBER
    , pin_nof_blocks             IN       NUMBER
   ) IS
   n_part_flag             NUMBER;
   n_part_count            NUMBER;
   n_comp_flag             NUMBER;
   n_subpart_count         NUMBER;
   BEGIN


     gv_proc  := c_pck || '.set_stat';

     SELECT COUNT(*)
       INTO n_part_flag
       FROM ALL_TABLES
      WHERE TABLE_NAME  =  UPPER(piv_tablename)
        AND owner =  UPPER(piv_OWNER)
        AND partitioned = 'YES';

     -- tablo partition li degilse
     IF n_part_flag = 0 THEN
        DBMS_STATS.SET_TABLE_STATS (piv_owner , piv_tablename, NULL, NULL, NULL, pin_nof_rows, pin_nof_blocks );
        Plib.o_log.LOG(23, 4,  gv_proc || c_delim || ' Set stat to '|| TO_CHAR(pin_nof_rows ,'fm999,999,999,999') || '/' || TO_CHAR(pin_nof_blocks ,'fm999,999,999,999') || ' is OK.',  piv_owner  || '.' || piv_tablename , NULL, NULL );
        RETURN;
     END IF;

     -- tablo partition li
      SELECT COUNT(*), SUM( DECODE(composite,'YES',1,0) )
        INTO n_part_count, n_comp_flag
        FROM ALL_TAB_PARTITIONS
       WHERE TABLE_NAME  =  UPPER(piv_tablename)
         AND table_owner =   UPPER(piv_owner);

      FOR crec IN ( SELECT partition_name
                        FROM ALL_TAB_PARTITIONS
                      WHERE TABLE_NAME  =  UPPER(piv_tablename)
                        AND table_owner = UPPER(piv_owner)
                   )
      LOOP
         DBMS_STATS.SET_TABLE_STATS (piv_owner , piv_tablename, crec.partition_name, NULL, NULL, (pin_nof_rows/ n_part_count), (pin_nof_blocks/n_part_count) );
       END LOOP;

     IF  n_comp_flag = 0 THEN
         Plib.o_log.LOG(23, 4,  gv_proc || c_delim || ' Set stat to '|| TO_CHAR(pin_nof_rows ,'fm999,999,999,999') || '/' || TO_CHAR(pin_nof_blocks ,'fm999,999,999,999') || ' is OK.',  piv_owner  || '.' || piv_tablename , NULL, NULL );
         RETURN;
     END IF;

     -- composite tablo isleri
     SELECT COUNT(*)
       INTO n_subpart_count
       FROM ALL_TAB_SUBPARTITIONS
      WHERE table_name  =  UPPER(piv_tablename)
        AND table_owner =  UPPER(piv_owner);

     FOR crec IN (  SELECT subpartition_name
                      FROM ALL_TAB_SUBPARTITIONS
                     WHERE table_name  =  UPPER(piv_tablename)
                       AND table_owner = UPPER(piv_owner)
                       )
     LOOP
        DBMS_STATS.SET_TABLE_STATS (piv_owner , piv_tablename, crec.subpartition_name, NULL, NULL, (pin_nof_rows/ n_subpart_count), (pin_nof_blocks/n_subpart_count) );
     END LOOP;
     Plib.o_log.LOG(23, 4,  gv_proc || c_delim || ' Set stat to '|| TO_CHAR(pin_nof_rows ,'fm999,999,999,999') || '/' || TO_CHAR(pin_nof_blocks ,'fm999,999,999,999') || ' is OK.',  piv_owner  || '.' || piv_tablename , NULL, NULL );
     RETURN;

  EXCEPTION
    WHEN OTHERS THEN
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;

   END;

  --------------------------------------------------------------
  --
  --   increase_buffer_size
  --
  --
  --------------------------------------------------------------
  PROCEDURE increase_buffer_size( v_buffsize IN NUMBER) IS
  BEGIN
     gv_proc  := c_pck || '.increase_buffer_size';
     DBMS_OUTPUT.ENABLE ( v_buffsize);
     Plib.o_log.LOG(1, 4 ,  gv_proc || c_delim || 'Buffer size set to : '|| TO_CHAR(v_buffsize) );
  EXCEPTION
    WHEN OTHERS THEN
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;





  --------------------------------------------------------------
  --
  --   object_exist
  --
  --   check whether object given by piv_objectname exists
  --   in schema piv_owner
  --


  PROCEDURE execute_with_log(pin_command IN VARCHAR2) IS
  BEGIN
     gv_proc  := c_pck || '.execute_with_log';
     EXECUTE IMMEDIATE pin_command;
     Plib.o_log.LOG(1, 4 , gv_proc || c_delim || 'Command executed successfully', NULL, NULL, pin_command );
  EXCEPTION
    WHEN OTHERS THEN
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, pin_command);
       RAISE;
  END;


  --------------------------------------------------------------
  --
  --   enable_parallel_dml
  --
  --   enable parallel execution of DML statements (i.e. UDPATE, INSERT, DELETE..)
  --
  --------------------------------------------------------------
  PROCEDURE enable_parallel_dml IS
  BEGIN
     gv_proc  := c_pck || '.enable_parallel_dml';
     v_dyntask := 'ALTER SESSION ENABLE PARALLEL DML';
     EXECUTE IMMEDIATE v_dyntask;
     Plib.o_log.LOG(1, 4 , gv_proc || c_delim || 'Parallel DML enabled' );
  EXCEPTION
    WHEN OTHERS THEN
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;

  --------------------------------------------------------------
  --
  --   disable_parallel_dml
  --
  --   disable parallel execution of DML statements (i.e. UDPATE, INSERT, DELETE..)
  --

  PROCEDURE enable_parallel_query IS
  BEGIN
     gv_proc  := c_pck || '.enable_parallel_query';
     v_dyntask := 'ALTER SESSION ENABLE PARALLEL QUERY';
     EXECUTE IMMEDIATE v_dyntask;
     Plib.o_log.LOG(1, 4 , gv_proc || c_delim || 'Parallel query enabled' );
  EXCEPTION
    WHEN OTHERS THEN
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;



  --------------------------------------------------------------
  --
  --   disable_row_movement
  --
  --   disable row_movement between partitions
  --
  --------------------------------------------------------------
  PROCEDURE disable_row_movement(   piv_owner     IN VARCHAR2,
                                    piv_tablename IN VARCHAR2
                       ) IS
  BEGIN
     gv_proc  := c_pck || '.disable_row_movement';
     v_dyntask := 'ALTER TABLE ' || piv_owner  || '.' || piv_tablename || ' DISABLE ROW MOVEMENT';
     EXECUTE IMMEDIATE v_dyntask;
     Plib.o_log.LOG(1, 4 , gv_proc || c_delim || 'Row movement disabled', piv_owner  || '.' || piv_tablename );
  EXCEPTION
     WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, piv_owner  || '.' || piv_tablename, NULL, v_dyntask);
       RAISE;
  END;



  --
  --   set_parallel_group
  --
  --   set parallel group of current system
  --   after connecting the specific instance of a RAC
  --
  --------------------------------------------------------------
  PROCEDURE set_parallel_group ( piv_group VARCHAR2) IS
  BEGIN
     gv_proc  := c_pck || '.set_parallel_group';
     v_dyntask := 'ALTER SYSTEM SET PARALLEL_INSTANCE_GROUP=' || piv_group || ' scope=memory sid=''TDWH1'' ';
     EXECUTE IMMEDIATE v_dyntask;
     Plib.o_log.LOG(1, 4 , gv_proc || c_delim || 'Command OK : ' || v_dyntask );
  EXCEPTION
     WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;

  --------------------------------------------------------------
  --
  --   set_hash_size
  --
  --   Set session parameter hash_area_size
  --
  --------------------------------------------------------------
  PROCEDURE set_hash_size(ar_hash_size NUMBER) IS
  BEGIN
     gv_proc  := c_pck || '.set_hash_size';
     v_dyntask := 'ALTER SESSION SET HASH_AREA_SIZE=' || ar_hash_size;
     EXECUTE IMMEDIATE v_dyntask;
     Plib.o_log.LOG(1, 4 , gv_proc || c_delim || 'Hash Area size is set to: '|| TO_CHAR(ar_hash_size) );

  EXCEPTION
     WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;


   --------------------------------------------------------------
   --
   --   set_sort_size
   --
   -- Set session parameter sort_area_size
   --------------------------------------------------------------
   PROCEDURE set_sort_size(ar_hash_size NUMBER) IS
   BEGIN
     gv_proc  := c_pck || '.set_sort_size';
     v_dyntask := 'ALTER SESSION SET SORT_AREA_SIZE=' || ar_hash_size;
     EXECUTE IMMEDIATE  v_dyntask ;
     Plib.o_log.LOG(1, 4 , gv_proc || c_delim || 'Sort Area size is set to: '|| TO_CHAR(ar_hash_size) );
  EXCEPTION
     WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
   END;



   --   skip_unusable_indexes_true
   --
   --   set skip_unusable_indexes to true
   --------------------------------------------------------------
   PROCEDURE skip_unusable_indexes_true IS
   BEGIN
     gv_proc  := c_pck || '.skip_unusable_indexes_true';
     v_dyntask := 'ALTER SESSION SET SKIP_UNUSABLE_INDEXES = TRUE';
     EXECUTE IMMEDIATE   v_dyntask ;
     Plib.o_log.LOG(1, 4 , gv_proc || c_delim || 'Skip_unusable_indexes is set to TRUE' );
  EXCEPTION
     WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
   END;

   --------------------------------------------------------------
   --
   --   skip_unusable_indexes_false
   --
   --   set skip_unusable_indexes to false
   --------------------------------------------------------------
   PROCEDURE skip_unusable_indexes_false IS
   BEGIN
     gv_proc  := c_pck || '.skip_unusable_indexes_false';
     v_dyntask := 'ALTER SESSION SET SKIP_UNUSABLE_INDEXES = FALSE';
     EXECUTE IMMEDIATE   v_dyntask ;
     Plib.o_log.LOG(1, 4 , gv_proc || c_delim || 'Skip_unusable_indexes is set to FALSE' );
  EXCEPTION
     WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
   END;



  -------------------------------------------------------------------------
  --
  --   PROCEDURE TEKRARKONTROL
  --
  --   Ayni verinin tabloya tekrar insert edilmesini engellemek amacli
  --   Ayni ETT_DATE ile parametrede p_tablo_adi ile verilen
  --   tekrar insert edilmesini engeller
  --
  --------------------------------------------------------------------------
  PROCEDURE tekrarkontrol( p_pck_name        IN VARCHAR2
                         , p_prc_name        IN VARCHAR2
                         , p_ett_date        IN DATE
                         ) IS
     ALREADY_LOADED EXCEPTION;
     PRAGMA EXCEPTION_INIT(ALREADY_LOADED, -20100);
     V_DATE DATE;
  BEGIN

   gv_proc  := c_pck || '.tekrarkontrol';
   v_dyntask :=  '
    SELECT T1.LAST_ETT_DATE
      FROM ' || c_control_table || ' T1
     WHERE T1.pck_name = ''' ||  p_pck_name || '''
       AND T1.proc_NAME = '''  ||  p_prc_name || '''
       AND T1.LAST_ETT_DATE = TO_DATE(''' || TO_CHAR(p_ett_date,'yyyymmdd') || ''', ''yyyymmdd'')';

    EXECUTE IMMEDIATE v_dyntask INTO v_date;

    -- BURAYA GELDIYSEK JOB CALISMIS DEMEK HATA VERELIM
    RAISE_APPLICATION_ERROR(-20100, 'HATA: ' || p_pck_name || '.' ||  p_prc_name || ' ETT_DATE ' ||
             TO_CHAR(p_ett_date, 'DD/MM/YYYY') || ' ICIN DAHA ONCE CALISMIS.' );

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
          -- daha once calismamis
             NULL;
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, c_control_table, NULL, v_dyntask);
       RAISE;
END;








  --------------------------------------------------------------
  --
  --   FUNCTION Check_duplicate
  --   table to be checked : p_Table_Owner.p_Table_Name
  --   field(s) to be checked for duplicates : p_group_by ( e.g. 'fld1,fld2')
  --   table where the duplicate data to be inserted after trunc :
  --              p_Dup_Res_Table_Owner.p_Dup_Res_Table_Name
  --   Return code: -1 (No duplicates), otherwise number of duplicates
  --   Note:
  --     p_Dup_Res_Table_Owner.p_Dup_Res_Table_Name
  --     should have 'group_by' fields and 'rid rowid' field
  --
  --------------------------------------------------------------
  FUNCTION check_duplicate(p_Table_Owner       IN VARCHAR2
                         , p_Table_Name        IN VARCHAR2
                         , p_group_by          IN VARCHAR2
                         , p_Dup_Res_Table_Owner IN VARCHAR2
                         , p_Dup_Res_Table_Name  IN VARCHAR2
                         )
            RETURN NUMBER IS
  PRAGMA AUTONOMOUS_TRANSACTION;
  Ln_cnt NUMBER;
  BEGIN

     gv_proc  := c_pck || '.check_duplicate';

    IF object_exist( UPPER(p_Dup_Res_Table_Owner), LOWER(p_Dup_Res_Table_Name)) THEN
       Plib.truncate_table(p_Dup_Res_Table_Owner, p_Dup_Res_Table_Name );
       EXECUTE IMMEDIATE v_dyntask;
     ELSE
       v_dyntask := 'CREATE TABLE '
                || p_Dup_Res_Table_Owner ||'.'
                || p_Dup_Res_Table_Name
                || default_non_par()
                || ' AS
                     SELECT '|| p_group_by || ', ROWID rid FROM ' || p_Table_Owner || '.' || p_Table_Name || ' WHERE 1=2 ';
      EXECUTE IMMEDIATE v_dyntask;
     END IF;

     o_log.LOG (8, 4,   p_Dup_Res_Table_Owner || '.' || p_Dup_Res_Table_Name || ' Tablosu bosaltildi.', NULL, NULL,v_dyntask );

     v_dyntask := 'insert /*+ append parallel(t1,16) */ into ' || p_Dup_Res_Table_Owner || '.' || p_Dup_Res_Table_Name || ' t1 (' || p_group_by || ', rid )
                    SELECT /*+ parallel(t1,16) */ ' || p_group_by  || ' , MIN(ROWID) rid
                     FROM ' || p_Table_Owner || '.' || p_Table_Name || ' t1
                    GROUP BY ' || p_group_by  || '
                   HAVING COUNT(*) > 1';

      EXECUTE IMMEDIATE v_dyntask;
      ln_CNT := SQL%rowcount;
      COMMIT;

      IF ln_CNT > 0 THEN
         o_log.LOG (2, 3,   gv_proc || c_delim || p_Dup_Res_Table_Owner || '.' || p_Dup_Res_Table_Name || ' tablosuna kayit eklendi.', p_Dup_Res_Table_Owner || '.' || p_Dup_Res_Table_Name , ln_CNT, v_dyntask );
      ELSE
         o_log.LOG (2, 3,   gv_proc || c_delim || p_Dup_Res_Table_Owner || '.' || p_Dup_Res_Table_Name || ' duplike kayit yok. OK.'  , p_Dup_Res_Table_Owner || '.' || p_Dup_Res_Table_Name , ln_CNT, v_dyntask );
      END IF;

      RETURN ln_CNT;

   EXCEPTION
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;

  --------------------------------------------------------------
  --
  --   FUNCTION GET_ETT_DATE
  --   <p_owner>.CADM_ETT_PARAMETERS tablosunda ETT_DATE
  --   parametresinin degerini dondurur.  Bulamaz ise hata doner
  --
  --------------------------------------------------------------
  FUNCTION Get_Ett_Date(p_owner IN VARCHAR2) RETURN DATE IS
  BEGIN
    RETURN TO_DATE(GET_PARAM_VALUE(p_owner, 'ETT_DATE'),'YYYYMMDD');
  END;

  --------------------------------------------------------------
  --
  --   FUNCTION GET_PARAM_VALUE
  --  <p_owner> parametresi ile belirtilen sema altindaki CADM_ETT_PARAMETERS
  --  tablosundan adi <p_name> parametresi ile belirtilen paramtre degerini
  --  dondurur. Bulamazsa NULL doner.
  --
  --------------------------------------------------------------
  FUNCTION get_param_value(p_owner IN VARCHAR2, p_name IN VARCHAR2) RETURN VARCHAR2 IS
  v_str VARCHAR2(100);
  param_not_found EXCEPTION;
  PRAGMA EXCEPTION_INIT(param_not_found, -20100);
  BEGIN

   gv_proc  := c_pck || '.get_param_value';

   BEGIN
      IF P_OWNER IS NULL THEN
        v_dyntask := ' SELECT VALUE
                         FROM CADM_ETT_PARAMETERS
                        WHERE NAME = ''' || p_name || '''' ;
     ELSE
           v_dyntask := ' SELECT VALUE
                       FROM  '|| p_owner || '.CADM_ETT_PARAMETERS
                      WHERE NAME = ''' || p_name || '''' ;
     END IF;

     EXECUTE IMMEDIATE v_dyntask  INTO v_str;

     EXCEPTION
     WHEN NO_DATA_FOUND THEN --  not found ?
        Plib.o_log.LOG(1, 1, gv_proc || c_delim || p_name  ||' parametresi bulunamadi. ! ', NULL, NULL,  NULL);
        RAISE_APPLICATION_ERROR(-20100, gv_proc || c_delim || p_name  || ' parametresi bulunamadi. ! ' );
   END;

   RETURN v_str;

   EXCEPTION
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM);
       RAISE;
  END;

  --------------------------------------------------------------
  --
  --   FUNCTION SET_PARAM_VALUE
  --  <p_owner> parametresi ile belirtilen sema altindaki CADM_ETT_PARAMETERS
  --  tablosundan adi <p_name> parametresi ile belirtilen paramtre degerini
  --  gunler. Bulamaz ise hata doner..
  --
  --------------------------------------------------------------
  PROCEDURE set_param_value(p_owner IN VARCHAR2, p_name IN VARCHAR2, p_value IN VARCHAR2)  IS
  param_not_found EXCEPTION;
  PRAGMA EXCEPTION_INIT(param_not_found, -20100);
  BEGIN

   gv_proc  := c_pck || '.set_param_value';

     IF P_OWNER IS NULL THEN
        v_dyntask :=  ' UPDATE cadm_ett_parameters
                           SET value = ''' || p_value || '''
                         WHERE name = ''' || p_name || '''
                    ';
         EXECUTE IMMEDIATE v_dyntask;
         -- RAISE ERROR IF PARAMETER NOT FOUND
         IF SQL%ROWCOUNT = 0 THEN
            RAISE param_not_found;
         END IF;
    ELSE
        v_dyntask :=  ' UPDATE '|| p_owner || '.cadm_ett_parameters
                           SET value = ''' || p_value || '''
                         WHERE name = ''' || p_name || '''
                    ';
         EXECUTE IMMEDIATE v_dyntask;
         -- RAISE ERROR IF PARAMETER NOT FOUND
         IF SQL%ROWCOUNT = 0 THEN
            RAISE param_not_found;
         END IF;
    END IF;

   EXCEPTION
     WHEN param_not_found THEN
        ROLLBACK;
        Plib.o_log.LOG(1, 1, gv_proc || c_delim ||  'HATA : ' || p_name || ' parametresi bulunmadi', p_owner || '.cadm_ett_parameters', NULL,  NULL);
        RAISE_APPLICATION_ERROR (-20101, 'HATA : ' || p_name || ' parametresi bulunmadi');
     WHEN OTHERS THEN
        -- Assign values to the log variables, using built-in functions.
        Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
        ROLLBACK;
        RAISE;
  END;



  --------------------------------------------------------------
  --
  --   FUNCTION GET_SYSTEM_ID
  --  <p_owner> parametresi ile belirtilen sema altindaki CADM_ETT_PARAMETERS
  --  tablosundan adi <p_name> parametresi ile belirtilen paramtre degerini
  --  dondurur. Bulamazsa NULL doner.
  --
  --------------------------------------------------------------
  FUNCTION get_system_id(p_owner IN VARCHAR2, p_name IN VARCHAR2) RETURN NUMBER IS
  n_id VARCHAR2(100);
  system_id_not_found EXCEPTION;
  PRAGMA EXCEPTION_INIT(system_id_not_found, -20100);
  BEGIN

   gv_proc  := c_pck || '.get_system_id';

   BEGIN
      v_dyntask := ' SELECT source_system
                       FROM  '|| p_owner ||'.source_systems
                      WHERE source_system_desc = ''' || p_name || '''' ;
     EXECUTE IMMEDIATE v_dyntask  INTO n_id;

     EXCEPTION
     WHEN NO_DATA_FOUND THEN --  not found ?
        Plib.o_log.LOG(1, 1, gv_proc || c_delim || p_name  ||' sistem nosu bulunamadi. ! ', p_owner || '.source_systems', NULL,  NULL);
        RAISE_APPLICATION_ERROR(-20100, gv_proc || c_delim || p_name  || ' nosu bulunamadi. ! ' );
   END;

   RETURN n_id;

   EXCEPTION
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM);
       RAISE;
  END;


  --------------------------------------------------------------
  --
  --   table_exist
  --
  --
  --------------------------------------------------------------
  FUNCTION table_exist( piv_owner IN VARCHAR2, piv_tablename IN VARCHAR2) RETURN BOOLEAN IS
  cnt              INTEGER;
  v_proc           VARCHAR2(50);          -- name of tyhe current procedure
  BEGIN

    v_proc := c_pck || '.table_exist';
    cnt := 0;
    SELECT COUNT(piv_tablename) INTO cnt
      FROM ALL_TABLES
     WHERE table_name = UPPER(piv_tablename) AND  owner = UPPER(piv_owner) ;

    IF cnt = 0 THEN
       RETURN FALSE;
    ELSE
          RETURN TRUE;
    END IF;

   EXCEPTION
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, v_proc || c_delim || SQLERRM);
       RAISE;
  END;

  --------------------------------------------------------------
  --
  --   partition_exist
  --
  --------------------------------------------------------------
  FUNCTION partition_exist
      (   piv_owner IN VARCHAR2,
          piv_tablename IN VARCHAR2 ,
          piv_partitionname IN VARCHAR2 ,
          piv_partition_type IN VARCHAR2 DEFAULT 'P'  -- Partition 'P' ; Subpartition 'S'
       ) RETURN BOOLEAN IS
  cnt              INTEGER;
  v_proc           VARCHAR2(50);          -- name of tyhe current procedure
  invalid_type    EXCEPTION;
  BEGIN

    v_proc := c_pck || '.partition_exist';
    cnt := 0;

    IF piv_partition_type = 'P' THEN
       SELECT COUNT(piv_partitionname) INTO cnt
         FROM ALL_TAB_PARTITIONS
        WHERE table_name = UPPER(piv_tablename)
          AND table_owner = UPPER(piv_owner)
          AND partition_name= UPPER(piv_partitionname);
    ELSIF piv_partition_type = 'S' THEN
       SELECT COUNT(piv_partitionname) INTO cnt
         FROM ALL_TAB_SUBPARTITIONS
        WHERE table_name = UPPER(piv_tablename)
          AND table_owner = UPPER(piv_owner)
          AND subpartition_name= UPPER(piv_partitionname);
    ELSE
       RAISE invalid_type;
    END IF;

    IF cnt = 0 THEN
       RETURN FALSE;
    ELSE
        RETURN TRUE;
    END IF;

   EXCEPTION
      WHEN invalid_type THEN
       Plib.o_log.LOG(1, 1, gv_proc || c_delim || 'Partition tipi hatal? girildi. Partition icin ''P'' Subpartition icin ''S'' olmali ! ');
       RAISE_APPLICATION_ERROR(-20100, gv_proc || c_delim || 'Partition tipi hatal? girildi. Partition icin ''P'' Subpartition icin ''S'' olmali ! ' );
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, v_proc || c_delim || SQLERRM);
       RAISE;
  END;




  -----------------------------------------------------------------------------------------------------------------
  --
  --  insert exception records to INS_RECORDS_INTO_EXP
  --
  --  Ayça Ürün KAYPMAZ
  --  05/01/2004
  --
  --   Change History:
  --   15 Jan 2004, MK
  --    1. Loglama kisimlari degistirildi.
  --    2. J tablosu kaldiridi.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE ins_records_into_exp( p_ett_date       IN DATE
                                , p_process_name   IN VARCHAR2
                                , p_table_name        IN VARCHAR2
                                , p_table_owner    IN VARCHAR2
                                , p_index_columns  IN VARCHAR2
                                , p_adet          OUT NUMBER
                                , p_ex_table_owner  IN VARCHAR2 DEFAULT 'SCHDWH'
                                , p_ex_table_name IN VARCHAR2   DEFAULT 'TTA01SUBS_DUP_KEYS_EXP'
                               ) IS
 CURSOR cur IS
   SELECT data_type
         ,column_name
     FROM ALL_TAB_COLUMNS
    WHERE table_name = UPPER(p_table_name)
      AND owner = UPPER(p_table_owner);
 v_proc             VARCHAR2(30);          -- LOCAL name of tyhe current procedure

 BEGIN
   v_proc := c_pck || '.ins_records_into_exp';
   v_dyntask:=NULL;

   FOR CUR_REC IN CUR LOOP
         IF v_dyntask IS NOT NULL THEN
             v_dyntask := v_dyntask || '||'',''||';
         END IF;

         IF cur_rec.data_type = 'NUMBER' THEN
             v_dyntask := v_dyntask||'TO_CHAR('||cur_rec.column_name||')';
         ELSIF cur_rec.data_type = 'DATE' THEN
             v_dyntask := v_dyntask||'TO_CHAR('||cur_rec.column_name||',''DD/MM/YYYY HH24:MI:SS'') ';
         ELSIF cur_rec.data_type = 'LONG' THEN
             NULL;
         ELSE
             v_dyntask := v_dyntask||cur_rec.column_name;
         END IF;
   END LOOP;

   IF v_dyntask IS NOT NULL THEN
           v_dyntask := 'INSERT  INTO ' || p_ex_table_owner ||'.' || p_ex_table_name  || ' p1
                                SELECT TO_DATE('''||TO_CHAR(p_ett_date,'YYYYMMDD')||''',''yyyymmdd'')
                                        ,'''||p_process_name||'''
                                        ,'''||p_table_owner||'.'||p_table_name||'''
                                        ,'||v_dyntask||
                                 ' FROM '||p_table_owner||'.'||p_table_name||'
                                      WHERE ROWID NOT IN ( SELECT /*+ hash_aj */ MAX(ROWID)
                                                          FROM '||p_table_owner||'.'||p_table_name||'
                                                             GROUP BY '||p_index_columns||'  ) ';
      EXECUTE IMMEDIATE v_dyntask ;
      p_adet := SQL%Rowcount;
      o_log.LOG(2, v_proc || c_delim || p_ex_table_owner ||'.' || p_ex_table_name, SQL%Rowcount,v_dyntask );
      COMMIT;
   END IF;

   EXCEPTION
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, v_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;

 END;


 -----------------------------------------------------------------------------------------------------------------
 --
 --  Duplicate records are deleted if duplicate record count is less the treshold
 --
 --  Ayça Ürün KAYPMAZ
 --  05/01/2004
 --
 -----------------------------------------------------------------------------------------------------------------
 PROCEDURE dup_sil              (   p_ett_date          IN DATE
                                ,p_process_name   IN VARCHAR2
                                ,p_table_name       IN VARCHAR2
                                ,p_table_owner       IN VARCHAR2
                                ,p_index_name       IN VARCHAR2
                                ,p_index_columns  IN VARCHAR2
                                ,p_storage_clause IN LONG
                                ,p_treshold          IN NUMBER
                                ,p_komut          IN LONG
                                ,p_dup_count     OUT NUMBER
                                ,p_rc               OUT NUMBER ) IS
 ln_adet                  INTEGER;
 v_proc                   VARCHAR2(30);          -- LOCAL name of tyhe current procedure
 BEGIN
    v_proc := c_pck || '.dup_sil';
    p_dup_count := 0 ;
  ------------------------------------------------------------------------
  -- E?er tablo üzerinde duplike kay?t varsa a?a??daki i?lemler yap?l?r --
  ------------------------------------------------------------------------
  o_log.LOG(22, v_proc || c_delim || 'Duplicate keys exist on '||p_table_owner||'.'||p_table_name);

  ------------------------------------------------------------------------
  -- Duplike kay?tlar  tta01subs_dup_keys tablosuna insert edilir.      --
  ------------------------------------------------------------------------
  INS_RECORDS_INTO_EXP(p_ett_date,p_process_name,p_table_name,p_table_owner,p_index_columns,ln_adet);
  o_log.LOG(1, 3, v_proc || c_delim || 'Duplicate keys inserted into tta01subs_dup_keys', 'tta01subs_dup_keys' );

  IF ln_adet >= p_treshold THEN
      ------------------------------------------------------------------------
      -- E?er duplike kay?t adedi, parametre olarak verilen treshold'dan    --
      -- büyükse, i?lemler yap?lmaz                                         --
      ------------------------------------------------------------------------
      p_rc := 1 ;
  ELSE
      ------------------------------------------------------------------------
      -- Duplike kay?t'lar tablodan silinerek, unique index yarat?l?r.      --
      ------------------------------------------------------------------------
      v_dyntask := 'DELETE /*+ parallel(p1,16) */ '||p_table_owner||'.'||p_table_name||'  p1
                                   WHERE ROWID NOT IN ( SELECT /*+ hash_aj */ MAX(ROWID)
                                                    FROM '||p_table_owner||'.'||p_table_name||'
                                                                            GROUP BY '||p_index_columns||'  ) ';
    EXECUTE IMMEDIATE v_dyntask;
    o_log.LOG (3, v_proc || c_delim || p_table_owner||'.'||p_table_name, SQL%rowcount, v_dyntask );
       COMMIT;
    p_rc := 0 ;
  END IF;
  p_dup_count := ln_adet ;

  EXCEPTION
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, v_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;

 END;

  -----------------------------------------------------------------------------------------------------------------
  --
  --  Unique_Index Creation
  --
  --  Ayça Ürün KAYPMAZ
  --  02/01/2004
  --
  --  Creates unique index on a given table. If duplicate records exist and count is less than given treshold then
  --  deletes duplicate records and create the unique index
  --
  --   Change History:
  --   15 Jan 2004, MK
  --    1. Loglama kisimlari degistirildi.
  --
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE create_unique_index ( p_ett_date       IN DATE
                                   , p_process_name   IN VARCHAR2
                                , p_table_name        IN VARCHAR2
                                , p_table_owner    IN VARCHAR2
                                , p_index_name        IN VARCHAR2
                                , p_index_columns  IN VARCHAR2
                                , p_storage_clause IN LONG
                                , p_treshold       IN NUMBER
                                , p_rc            OUT NUMBER ) IS
  table_not_found  EXCEPTION  ;
  index_exists     EXCEPTION  ;
  treshold_greater EXCEPTION  ;
  treshold_too_big EXCEPTION  ;
  DUP_KEYS         EXCEPTION;
  PRAGMA           EXCEPTION_INIT(DUP_KEYS, -1452);
  PAR_QU           EXCEPTION;
  PRAGMA             EXCEPTION_INIT(PAR_QU, -12801);
  ln_rc               INTEGER;
  ln_adet            INTEGER;
  lv_dyntask         VARCHAR2(32000); -- v_dyntask procedurlerin icinde kullanildiginda
                                    -- local variable yarattim

 BEGIN

    gv_proc := c_pck || '.create_unique_index';
    p_rc := 0 ;

    --------------------------------------------------------------
    -- Treshold de?eri 1000000'dan fazlaysa hata verir ..       --
    --------------------------------------------------------------
    IF p_treshold > 1000000 THEN
          RAISE treshold_too_big;
    END IF;

    --------------------------------------------------------------
    -- Tablo'nun database'de var olup olmad??? kontrol edilir.. --
    --------------------------------------------------------------
    IF NOT(table_exist( p_table_owner,p_table_name)) THEN
          RAISE table_not_found;
    END IF;

    --------------------------------------------------------------
    -- Tablo üzerinde ayn? adl? index varsa drop edilir         --
    --------------------------------------------------------------
    SELECT COUNT(1)
      INTO ln_adet
      FROM ALL_INDEXES
     WHERE table_name = UPPER(p_table_name)
       AND table_owner = UPPER(p_table_owner)
       AND index_name = UPPER(p_index_name) ;

    IF ln_adet <> 0 THEN
          o_log.LOG(1,  3, gv_proc || c_delim || 'Index exists on table, so dropping it..');
          v_dyntask := 'DROP INDEX ' || p_index_name;
          EXECUTE IMMEDIATE v_dyntask;
          o_log.LOG(10, p_index_name, NULL, v_dyntask);
    END IF;

    lv_dyntask := 'CREATE UNIQUE INDEX '||p_index_name||' ON '||p_table_owner||'.'||p_table_name||
                            '( '||p_index_columns||' ) '||p_storage_clause;
    BEGIN
        --------------------------------------------------------------
        -- Unique index yarat?l?r                                   --
        --------------------------------------------------------------
         EXECUTE IMMEDIATE lv_dyntask ;
         o_log.LOG(9, p_index_name, NULL, lv_dyntask);

         EXCEPTION
         WHEN dup_keys THEN
                        DUP_SIL(  p_ett_date
                                , p_process_name
                                , p_table_name
                                , p_table_owner
                                , p_index_name
                                , p_index_columns
                                , p_storage_clause
                                , p_treshold
                                , lv_dyntask
                                , ln_adet
                                , ln_rc  )        ;
            IF ln_rc = 1 THEN
                RAISE treshold_greater;
            END IF;
             --index i yaratmayi bir daha dene
            p_rc := c_error ;
            EXECUTE IMMEDIATE lv_dyntask ;
            o_log.LOG(9, p_index_name, NULL, lv_dyntask);
            p_rc := c_normal ;
         WHEN PAR_QU THEN
                    IF INSTR(SQLERRM,'ORA-01452')<>0 THEN
                        DUP_SIL    ( p_ett_date
                                , p_process_name
                                , p_table_name
                                , p_table_owner
                                , p_index_name
                                , p_index_columns
                                , p_storage_clause
                                , p_treshold
                                , lv_dyntask
                                , ln_adet
                                , ln_rc  )        ;
                  IF ln_rc = 1 THEN
                       RAISE treshold_greater;
                  END IF;
                     --index i yaratmayi bir daha dene
                    p_rc := c_error ;
                    EXECUTE IMMEDIATE lv_dyntask ;
                    o_log.LOG(9, p_index_name, NULL, lv_dyntask);
                    p_rc := c_normal ;
                    ELSE
                        RAISE;
                    END IF;
        END;
  EXCEPTION
       WHEN table_not_found THEN
                    o_log.LOG(1,  1, gv_proc || c_delim || p_table_owner||'.'||p_table_name||' does not exist ');
                p_rc := 1 ;
       WHEN treshold_too_big THEN
                    o_log.LOG(1,  1, gv_proc || c_delim || 'treshold must be smaller THAN 1000000 : '||TO_CHAR(p_treshold));
                p_rc := 1 ;
       WHEN treshold_greater THEN
                    o_log.LOG(1,  1, gv_proc || c_delim || 'Duplicate records are more than treshold.');
                    o_log.LOG(1,  1, gv_proc || c_delim || 'Treshold :'||p_treshold);
                    o_log.LOG(1,  1, gv_proc || c_delim || 'Dup records :'||ln_adet);
                p_rc := 1 ;
     WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
 END;

  -----------------------------------------------------------------------------------------------------------------
  --
  --  make_not_null_wtrashold
  --
  --
  --   Change History:
  --
  --   20 Jan 2004, Created, MK
  --
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE make_not_null_wtrashold(
                                  p_ett_date      IN DATE
                                   , p_process_name  IN VARCHAR2
                                , p_table_owner   IN VARCHAR2
                                , p_table_name       IN VARCHAR2
                                , p_table_column  IN VARCHAR2
                                , p_treshold      IN NUMBER
                                , p_rc            OUT NUMBER
                                , p_ex_table_owner  IN VARCHAR2 DEFAULT 'SCHDWH'
                                , p_ex_table_name IN VARCHAR2   DEFAULT 'TTA01SUBS_DUP_KEYS_EXP'
                                 ) IS
  table_not_found  EXCEPTION  ;
  treshold_greater EXCEPTION  ;
  treshold_too_big EXCEPTION  ;
  already_not_null EXCEPTION  ;
  ln_rc               INTEGER;
  ln_adet           INTEGER;
  v_nullable          VARCHAR2(1);

 BEGIN

    gv_proc := c_pck || '.make_not_null_wtrashold';
    p_rc := 0 ;

    --------------------------------------------------------------
    -- Treshold de?eri 1000000 dan fazlaysa hata verir ..       --
    --------------------------------------------------------------
    IF NVL(p_treshold,0) > 1000000 THEN
          RAISE treshold_too_big;
    END IF;

    --------------------------------------------------------------
    -- Tablo'nun database'de var olup olmad??? kontrol edilir.. --
    --------------------------------------------------------------
    IF NOT(table_exist( p_table_owner, p_table_name)) THEN
          RAISE table_not_found;
    END IF;

    --------------------------------------------------------------
    -- Ky?t halihaz?rda NOT NULL mi ? Oyle ise cikiyoruz                                                                                       --
    --------------------------------------------------------------

    v_dyntask := ' SELECT NULLABLE
                     FROM ALL_TAB_COLUMNS
                      WHERE
                         OWNER = '''||p_table_owner||'''
                        AND TABLE_NAME = '''||p_table_name||'''
                        AND COLUMN_NAME = '''||p_table_column||'''';

     EXECUTE IMMEDIATE v_dyntask INTO v_nullable; -- execute the above statement

     IF v_nullable <> 'Y' THEN
        RAISE already_not_null;
     END IF;

    --------------------------------------------------------------
    --  Not Null yapilacak Kolonu NULL olan kac kayit var       --
    --------------------------------------------------------------
    v_dyntask := ' SELECT /*+ parallel(t1,16) */ COUNT(1)
                      FROM ' || p_table_owner || '.' ||p_table_name || ' t1
                     WHERE ' || p_table_column || ' IS NULL';

    EXECUTE IMMEDIATE v_dyntask
            INTO ln_adet;

    IF ln_adet > 0 THEN
       IF  ln_adet > p_treshold  THEN
           -- threshold degerinden fazla kayidin
           -- ilgili kolunu Null
           RAISE treshold_greater;
       ELSE

            o_log.LOG(1,  3, gv_proc || c_delim || p_table_owner||'.'||p_table_name||' ait ' || p_table_column ||' kolonunda NULL degerler var. ');
            o_log.LOG(1,  3, gv_proc || c_delim ||'Null degerli kayit sayisi : ' || ln_adet || ' Threshold : ' || p_treshold );
            o_log.LOG(1,  3, gv_proc || c_delim ||'Exception tablosuna atma islemi basladi ..');

          v_dyntask :=  NULL;
          FOR crec IN (   SELECT data_type
                               , column_name
                            FROM ALL_TAB_COLUMNS
                           WHERE table_name = UPPER(p_table_name)
                             AND owner = UPPER(p_table_owner)
                       )
           LOOP

               IF v_dyntask IS NOT NULL THEN
                     v_dyntask := v_dyntask || '||'',''||';
              END IF;

               IF crec.data_type = 'NUMBER' THEN
                     v_dyntask := v_dyntask||'TO_CHAR('||crec.column_name||')';
                ELSIF crec.data_type = 'DATE' THEN
                     v_dyntask := v_dyntask||'TO_CHAR('||crec.column_name||',''DD/MM/YYYY HH24:MI:SS'') ';
                ELSIF crec.data_type = 'LONG' THEN
                       NULL;
                   ELSE
                       v_dyntask := v_dyntask||crec.column_name;
                  END IF;
           END LOOP;

           IF v_dyntask IS NOT NULL THEN
                 v_dyntask := 'INSERT  INTO ' || p_ex_table_owner ||'.' || p_ex_table_name  || ' t1
                                SELECT TO_DATE('''||TO_CHAR(p_ett_date,'YYYYMMDD')||''',''yyyymmdd'')
                                        , '''||p_process_name||'''
                                        , '''||p_table_owner||'.'||p_table_name||'''
                                        , '||v_dyntask||
                                 ' FROM '||p_table_owner||'.'||p_table_name||'
                                      WHERE '|| p_table_column || ' IS NULL ';

              EXECUTE IMMEDIATE v_dyntask;
              o_log.LOG (2, gv_proc || c_delim || p_ex_table_owner ||'.' || p_ex_table_name, SQL%rowcount, v_dyntask );

                 v_dyntask := 'DELETE FROM '||p_table_owner||'.'||p_table_name||'
                                      WHERE '|| p_table_column || ' IS NULL ';
              EXECUTE IMMEDIATE v_dyntask;
              o_log.LOG (3, gv_proc || c_delim ||p_ex_table_owner ||'.' || p_ex_table_name, SQL%rowcount, v_dyntask );

              COMMIT;
                  o_log.LOG(1,  3, gv_proc || c_delim || 'Exception tablosuna atma islemi tamamlandi ..');

              -- Artik NOT NULL operasyonunu yapabiliriz
              v_dyntask := 'ALTER TABLE '|| p_table_owner ||'.'||p_table_name||' MODIFY ('||p_table_column||' NOT NULL)';
                  EXECUTE IMMEDIATE v_dyntask; -- execute the above statement

          END IF;
       END IF;
    ELSE -- IF ln_adet = 0 then ( kolonu null degeri iceren  kayit yok )
         -- Artik NOT NULL operasyonunu yapabiliriz
        v_dyntask := 'ALTER TABLE '|| p_table_owner ||'.'||p_table_name||' MODIFY ('||p_table_column||' NOT NULL)';
        EXECUTE IMMEDIATE v_dyntask; -- execute the above statement
    END IF;

    p_rc := c_normal ;


  EXCEPTION
       WHEN table_not_found THEN
                    o_log.LOG(1,  1, gv_proc || c_delim || p_table_owner||'.'||p_table_name||' does not exist ');
                p_rc := c_error ;
       WHEN treshold_too_big THEN
                    o_log.LOG(1,  1, gv_proc || c_delim || 'treshold must be smaller THAN 1000000 : '||TO_CHAR(p_treshold));
                p_rc := c_error ;
       WHEN treshold_greater THEN
                    o_log.LOG(1,  1, gv_proc || c_delim || ' Null records are more than treshold.');
                    o_log.LOG(1,  1, gv_proc || c_delim ||' Treshold :'||p_treshold);
                    o_log.LOG(1,  1, gv_proc || c_delim ||' Null records :'||ln_adet);
                p_rc := c_error ;
       WHEN already_not_null THEN
                   o_log.LOG(1,  3, gv_proc || c_delim || p_table_owner ||'.'||p_table_name||' in ' ||p_table_column || ' kolonu zaten NOT NULL..');
                p_rc := c_normal ;

    WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
 END;

  -----------------------------------------------------------------------------------------------------------------
  --
  --  set_trace
  --
  --
  --   Change History:
  --
  --   21 Jan 2004, Created, MK (Request from Burak Karatepe)
  --
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE set_trace (p_trc_identifier IN VARCHAR2)
  IS
  BEGIN

     gv_proc := c_pck || '.set_trace';

     v_dyntask := 'alter session set sql_trace=TRUE';
     EXECUTE IMMEDIATE v_dyntask;
     v_dyntask := 'alter session set max_dump_file_size=unlimited';
     EXECUTE IMMEDIATE v_dyntask;
     v_dyntask := 'alter session set tracefile_identifier=' || p_trc_identifier;
     EXECUTE IMMEDIATE v_dyntask;
     v_dyntask :=
        'ALTER SESSION SET EVENTS ''10046 TRACE NAME CONTEXT FOREVER, LEVEL 12'' ';
     EXECUTE IMMEDIATE v_dyntask;
     v_dyntask := 'alter session set timed_statistics=TRUE ';
     EXECUTE IMMEDIATE v_dyntask;

  EXCEPTION
    WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;


  -----------------------------------------------------------------------------------------------------------------
  --
  --   sp_parameter_return
  --
  --
  --   Change History:
  --
  --   21 Jan 2004, Created, MK (Request from Burak Karatepe)
  --
  --  gets necessary YYYYMMDD and YYYYMM ett_date variables.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE ett_date_formatted (
     p_owner                    IN       VARCHAR2
   , p_ymd                      OUT      VARCHAR2
   , p_ymm                      OUT      VARCHAR2
   , p_ymm_1                    OUT      VARCHAR2
   , p_ymm_2                    OUT      VARCHAR2
   , p_ymm_3                    OUT      VARCHAR2
   , p_ymm_4                    OUT      VARCHAR2
   , p_vym                      OUT      VARCHAR2)
  IS
  DT DATE;
  BEGIN
     dt := Get_Ett_Date (p_owner);
     p_ymd := TO_CHAR (TO_DATE (DT, 'yyyymmdd'), 'yyyymmdd');
     p_ymm := TO_CHAR (TO_DATE (DT, 'yyyymmdd'), 'yyyymmdd');
     p_ymm_1 := TO_CHAR (TO_DATE (DT, 'yyyymmdd'), 'yyyymm');
     p_ymm_2 := TO_CHAR (ADD_MONTHS (TO_DATE (DT, 'yyyymmdd'), -1), 'yyyymm');
     p_ymm_3 := TO_CHAR (ADD_MONTHS (TO_DATE (DT, 'yyyymmdd'), -2), 'yyyymm');
     p_ymm_4 := TO_CHAR (ADD_MONTHS (TO_DATE (DT, 'yyyymmdd'), -3), 'yyyymm');
    -- p_ymm_5 := TO_CHAR (ADD_MONTHS (TO_DATE (DT, 'yyyymmdd'), -4), 'yyyymm');
     p_vym := TO_CHAR (ADD_MONTHS (TO_DATE (DT, 'yyyymmdd'), 1), 'yyyymm');
     o_log.LOG (1, 4
             , 'ETT_DATE:' || p_ymd || ' YYYYMM:' || p_ymm || ' YYYYMM-1:'
               || p_ymm_1 || ' YYYYMM-2:' || p_ymm_2 || ' YYYYMM-3:' || p_ymm_3
               || ' YYYYMM-4:' || p_ymm_4 || ' YYYYMM+1:' || p_vym);
  END ett_date_formatted;



  FUNCTION default_non_par (
     pin_tablespace_type        IN       VARCHAR2 DEFAULT 'T' -- T temporary, P permanent
   , pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
   , pin_parallel_degree        IN       NUMBER DEFAULT 16
   , pin_ini_trans              IN       NUMBER DEFAULT 1
   , pin_pct_free               IN       NUMBER DEFAULT 0
   , pin_pct_used               IN       NUMBER DEFAULT 99
   , pin_freelists              IN       NUMBER DEFAULT 1
   , pin_pct_increase           IN       NUMBER DEFAULT 0)
     RETURN LONG
  IS
     l_str   LONG;
     i       NUMBER;
     v_def_tablespace VARCHAR2(30);
     INVALID_PARAMETER EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(INVALID_PARAMETER, -20100);
  BEGIN
     gv_proc := c_pck || '.default_non_par';

     IF pin_tablespace_type = 'T'  THEN
        -- temporary
        v_def_tablespace := c_tmp_def_tablespace ;
     END IF;

     IF pin_tablespace_type = 'P' THEN
        -- permanent
        v_def_tablespace := c_prm_def_tablespace ;
     END IF;

     IF pin_tablespace_type = 'U' THEN
       -- kullanicinin istedigi bir tablespace
        v_def_tablespace := pin_default_tablespace ;
     END IF;

     -- kullanilacak TABLESPACE adi hala bilinmiyor ise hata
     IF v_def_tablespace IS NULL  THEN
           RAISE INVALID_PARAMETER;
     END IF;


     -- genel storage degerleri
     l_str :=
        c_crlf || ' PCTFREE ' || pin_pct_free || c_crlf || ' PCTUSED ' || pin_pct_used || c_crlf || ' INITRANS ' || pin_ini_trans
        || c_crlf || '  STORAGE ( PCTINCREASE ' || pin_pct_increase || c_crlf || '            FREELISTS ' || pin_freelists || '   ) '
        || c_crlf || '   NOCACHE ';

     -- parallel nologging eki
     l_str :=  l_str  || c_crlf || ' PARALLEL ' || pin_parallel_degree || ' NOLOGGING';
     -- tablonun tablespace' i
     l_str := l_str || c_crlf || ' TABLESPACE ' || v_def_tablespace;
     RETURN l_str;
  EXCEPTION
       WHEN INVALID_PARAMETER THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'INVALID PARAMETER VALUE');
       l_str := NULL;
       RAISE_APPLICATION_ERROR(-20100, 'INVALID PARAMETER VALUE');
     WHEN OTHERS
     THEN
        -- Assign values to the log variables, using built-in functions.
        Plib.o_log.LOG (SQLCODE, 1, gv_proc || c_delim || SQLERRM);
        l_str := NULL;
        RAISE;
  END default_non_par;


  FUNCTION default_hash (
     pin_hash_partition_columns IN       VARCHAR2
   , pin_part_cnt               IN       NUMBER DEFAULT 16
   , pin_tablespace_type        IN       VARCHAR2 DEFAULT 'T' -- T temporary, P permanent
   , pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
   , pin_tsnum_start            IN       NUMBER DEFAULT 1
   , pin_tsnum_end              IN       NUMBER DEFAULT 16
   , pin_spread                 IN       BOOLEAN  DEFAULT TRUE
   , pin_parallel_degree        IN       NUMBER DEFAULT 16
   , pin_ini_trans              IN       NUMBER DEFAULT 1
   , pin_pct_free               IN       NUMBER DEFAULT 0
   , pin_pct_used               IN       NUMBER DEFAULT 99
   , pin_freelists              IN       NUMBER DEFAULT 1
   , pin_pct_increase           IN       NUMBER DEFAULT 0)
     RETURN LONG
  IS
     l_str            LONG;
     i                NUMBER;
     v_def_tablespace VARCHAR2(30);
     part_sayac       NUMBER;
     INVALID_PARAMETER EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(INVALID_PARAMETER, -20100);
  BEGIN
     gv_proc := c_pck || '.default_hash';

    -- partition sayilari 16 ve katlari olmali
    IF MOD(pin_part_cnt,16) <> 0  THEN
           RAISE INVALID_PARAMETER;
     END IF;

     IF pin_tablespace_type = 'T'  THEN
        -- temporary
        v_def_tablespace := c_tmp_def_tablespace ;
     END IF;

     IF pin_tablespace_type = 'P' THEN
        -- permanent
        v_def_tablespace := c_prm_def_tablespace ;
     END IF;

     IF pin_tablespace_type = 'U' THEN
       -- kullanicinin istedigi bir tablespace
        v_def_tablespace := pin_default_tablespace ;
     END IF;

     -- kullanilacak TABLESPACE adi hala bilinmiyor ise hata
     IF v_def_tablespace IS NULL  THEN
           RAISE INVALID_PARAMETER;
     END IF;

     --  tablespace bitis numarasi, baslangictan kucuk olamaz
     IF pin_tsnum_start > pin_tsnum_end THEN
           RAISE INVALID_PARAMETER;
     END IF;

     -- genel storage degerleri
     l_str :=
        c_crlf || ' PCTFREE ' || pin_pct_free || c_crlf || ' PCTUSED ' || pin_pct_used || c_crlf || ' INITRANS ' || pin_ini_trans
        || c_crlf || '  STORAGE ( PCTINCREASE ' || pin_pct_increase || c_crlf || '            FREELISTS ' || pin_freelists || '   ) '
        || c_crlf || '   NOCACHE ';
     -- partition cumlesi baslar..
     l_str := l_str || c_crlf || ' PARTITION BY HASH(' || pin_hash_partition_columns || ') (';

     -- partitionlar
     part_sayac := pin_tsnum_start;
     FOR i IN 1 .. pin_part_cnt
     LOOP
        IF  pin_spread THEN -- PARTITIONLAR FARKLI TABLESPACE LERE DAGILACAK
           l_str := l_str || c_crlf || 'PARTITION P' || TO_CHAR (i, 'FM000') || ' tablespace ' || v_def_tablespace || '_' ||TO_CHAR(part_sayac, 'fm00') || ' ,';
        ELSE
           l_str := l_str || c_crlf || 'PARTITION P' || TO_CHAR (i, 'FM000') || ' tablespace ' || v_def_tablespace || ' ,';
        END IF;
        part_sayac := part_sayac + 1;
        IF part_sayac > pin_tsnum_end  THEN
           part_sayac := pin_tsnum_start;
        END IF;
     END LOOP;

     -- parallel nologging eki
     l_str := SUBSTR (l_str, 1, LENGTH (l_str) - 2) || c_crlf || ') PARALLEL ' || pin_parallel_degree || ' NOLOGGING';
     -- tablonun kendisinin tablespace i de ayni yer olsun..
     IF  pin_spread THEN
       l_str := l_str || c_crlf || ' TABLESPACE ' || v_def_tablespace || '_' || TO_CHAR(pin_tsnum_start,'fm00');
     ELSE -- spread false ise default tablespace e numara atanmam?? olsun
       l_str := l_str || c_crlf || ' TABLESPACE ' || v_def_tablespace ;
     END IF;

     RETURN l_str;
  EXCEPTION
       WHEN INVALID_PARAMETER THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'INVALID PARAMETER VALUE');
       l_str := NULL;
       RAISE_APPLICATION_ERROR(-20100, 'INVALID PARAMETER VALUE');
     WHEN OTHERS
     THEN
        -- Assign values to the log variables, using built-in functions.
        Plib.o_log.LOG (SQLCODE, 1, gv_proc || c_delim || SQLERRM);
        l_str := NULL;
        RAISE;
  END default_hash;


    ----------------------------------------------------------------
  --
  --   add_date_partition
  --
  --
  --   Created : 26/02/2004 MK
  --
  --   Bu prosedur standart isimlere sahip range ya da range+hash(comp)
  --   tablolarda (range gun, ay ya da yil olan) partition eklemek icin
  --   kullanilabilir.
  --   Partition name formati    : PYYYY, PYYYYMM, PYYYYMMDD
  --   Subpartition name formati : S<partition_name>_NNN (NNN 3 rakamli sayi)
  --   Parametreler
  --
  --  piv_owner                  :  Tablo sahibi
  --  piv_tablename              :  Tablo Adi
  --  max_date                   :  Sonucta Range partition'li tabloda en buyuk date
  --                                li partition ne olacak
  --  pin_tablespace_type        IN       VARCHAR2 DEFAULT 'NULL' -- NULL mevcut ts kullanilacak, T temporary, P permanent, U:user defined
  --  pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
  --------------------------------------------------------------
  PROCEDURE add_date_partition (
     piv_owner                  IN       VARCHAR2
   , piv_tablename              IN       VARCHAR2
   , max_date                   IN       DATE
   , pin_tablespace_type        IN       VARCHAR2 DEFAULT NULL -- NULL mevcutu kullan, T temporary, P permanent
   , pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
   , pin_spread                 IN       BOOLEAN  DEFAULT TRUE
   , pin_number_based           IN       BOOLEAN  DEFAULT FALSE
   , pin_tsnum_start            IN       NUMBER DEFAULT 1
   , pin_tsnum_end              IN       NUMBER DEFAULT 16
   , pin_part_prefix_replace    IN       VARCHAR2 DEFAULT 'P'
   , pin_dual_index                IN         BOOLEAN  DEFAULT FALSE
  )
  IS
--  Plib.add_date_partition('ANT','SUBSCRIBER_COST',TO_DATE(v_year_month,'YYYYMM'),NULL,NULL,TRUE,TRUE );

     -- degiskenler
     d                DATE;
     l_hv             LONG;
     v_def_tablespace VARCHAR2(30);
     n_subpart_cnt    NUMBER;
     f_date_range     BOOLEAN;
     pmax             VARCHAR2(80);
     pts              VARCHAR2(80);
     bastar           DATE;
     bittar           DATE;
     v_pname          VARCHAR2(80);
     v_pname_index      VARCHAR2(80);
     part_sayac       NUMBER;
     b_spread_ts      BOOLEAN;
     v_boundary       VARCHAR2(80);

     TYPE t_dual_values   IS TABLE OF VARCHAR2(100);
     v_dual_values t_dual_values;

     v_dual_val             VARCHAR2(100);
     v_dual_part      VARCHAR2(10);

     -- hata ile ilgili tanimlar
     INVALID_PARAMETER EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(INVALID_PARAMETER, -20100);
     INVALID_TS_NAME EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(INVALID_TS_NAME, -20101);
     INVALID_PART_NAME EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(INVALID_PART_NAME, -20102);

    ---------------------------------------------------------------------------------------
    -- alt fonksiyon
    FUNCTION sp_statement (piv_partition_name IN VARCHAR2, piv_ts_name IN VARCHAR2, pib_spread IN BOOLEAN
                         , pin_ts_start IN NUMBER , pin_ts_end IN NUMBER)
       RETURN LONG
    IS
       l_subpart   LONG;
       i           NUMBER;
       spart_sayac  NUMBER;
    BEGIN
       l_subpart :=  c_crlf  || ' ( ' ;
       IF n_subpart_cnt IS NOT NULL
       THEN
          spart_sayac := pin_ts_start;
          FOR i IN 1 .. n_subpart_cnt
          LOOP
                     IF pib_spread THEN
                            l_subpart :=
                         l_subpart || c_crlf|| ' SUBPARTITION S' || piv_partition_name ||'_' || TO_CHAR (i, 'fm000') || ' tablespace ' || piv_ts_name  || '_' ||TO_CHAR(MOD(spart_sayac-1, 16)+1 , 'fm00')
                         || ', ';
                     ELSE
                            l_subpart :=
                         l_subpart || c_crlf|| ' SUBPARTITION S' || piv_partition_name ||'_' || TO_CHAR (i, 'fm000') || ' tablespace ' || piv_ts_name
                         || ', ';
                    END IF;

            spart_sayac := spart_sayac +1;
            IF spart_sayac > pin_ts_end  THEN
                 spart_sayac := pin_ts_start;
            END IF;

          END LOOP;
       END IF;
       l_subpart := SUBSTR (l_subpart, 1, LENGTH (l_subpart) - 2) || ' ) ';
       RETURN l_subpart;
    END;
    ---------------------------------------------------------------------------------------
    FUNCTION ts_name (p_ts_name IN VARCHAR2, p_spread_ts IN BOOLEAN, p_number IN NUMBER )
       RETURN LONG
    IS
    BEGIN
       IF p_spread_ts THEN
          RETURN p_ts_name || '_' ||TO_CHAR(MOD(p_number-1, 16)+1, 'fm00');
        ELSE
          RETURN p_ts_name;
        END IF;
    END;
    ---------------------------------------------------------------------------------------

    -- add_date_partition burda baslar
    BEGIN

       gv_proc := c_pck || '.add_date_partition';

        SELECT MAX(partition_name), MAX(tablespace_name), MAX(SUBPARTITION_COUNT)
          INTO pmax, pts, n_subpart_cnt
          FROM ALL_TAB_PARTITIONS
         WHERE table_owner = piv_owner
           AND table_name = piv_tablename ;

        -- eski part name ler icin kacis ....
        SELECT high_value, tablespace_name
          INTO  l_hv , pts
          FROM ALL_TAB_PARTITIONS
         WHERE table_name = piv_tablename
           AND table_owner = piv_owner
           AND partition_name = pmax ;

        pmax := REPLACE(pmax , pin_part_prefix_replace, 'P');


        IF pin_spread IS NULL THEN
           RAISE INVALID_PARAMETER;
        END IF;

        IF SUBSTR(pmax,1,1) <> 'P' OR LENGTH(pmax) > 9 THEN
               RAISE INVALID_PART_NAME;
        END IF;


            IF pin_tablespace_type = 'T'  THEN
             -- temporary
            -- T dedikten sonra tablespace adi verilemez
             IF pin_default_tablespace IS NOT NULL THEN
                RAISE INVALID_PARAMETER;
             END IF;
            v_def_tablespace := c_tmp_def_tablespace ;
            b_spread_ts := pin_spread;
         ELSIF pin_tablespace_type = 'P' THEN
             -- permanent
            -- P dedikten sonra tablespace adi verilemez
            IF pin_default_tablespace IS NOT NULL THEN
               RAISE INVALID_PARAMETER;
            END IF;
             v_def_tablespace := c_prm_def_tablespace ;
             b_spread_ts := pin_spread;
         ELSIF pin_tablespace_type = 'U' THEN
             -- kullanicinin istedigi bir tablespace
             v_def_tablespace := pin_default_tablespace ;
             b_spread_ts := pin_spread;
         ELSIF pin_tablespace_type IS NULL THEN
                -- TABLESPACE ISMININ SONU NASIL ? _01, _02 .. _16 mi ?
                -- oyle ise bizde partitionlari yayacagiz.
                -- mevcut partitionlarin yerlestigi ts yada tsler kullanilacak
                IF    ( SUBSTR(pts, LENGTH(pts), 1) >= '0' AND  SUBSTR(pts, LENGTH(pts), 1) <= '9' )
                  AND ( SUBSTR(pts, LENGTH(pts)-1, 1) >= '0' AND  SUBSTR(pts, LENGTH(pts)-1, 1) <= '9' )
                THEN
                  b_spread_ts := TRUE;
                  v_def_tablespace := SUBSTR(pts,1, LENGTH(pts)-3); -- tablonun mevcut TS inin sonudaki 3 karakteri  haric kullanilacak ..
                ELSE
                  b_spread_ts := FALSE;
                  v_def_tablespace := pts; -- tablonun mevcu TS i kullanilacak ..
                END IF;
         ELSE
            RAISE INVALID_PARAMETER;
         END IF;


         -- part_sayac kactan baslayacak ?
         IF b_spread_ts = TRUE THEN
           IF    ( SUBSTR(pts, LENGTH(pts), 1) >= '0' AND  SUBSTR(pts, LENGTH(pts), 1) <= '9' )
             AND ( SUBSTR(pts, LENGTH(pts)-1, 1) >= '0' AND  SUBSTR(pts, LENGTH(pts)-1, 1) <= '9' )
           THEN
             part_sayac := TO_NUMBER(SUBSTR(pts, LENGTH(pts)-1, 2))+1;
             IF part_sayac > pin_tsnum_end  THEN
                part_sayac := pin_tsnum_start;
             END IF;
           ELSE
             part_sayac := 1;
           END IF;
         ELSE
           part_sayac := NULL;
         END IF;


    IF pin_dual_index = TRUE THEN  --FOR 2 way index Partitions

          SELECT
              high_value
              BULK COLLECT INTO v_dual_values
          FROM ALL_TAB_PARTITIONS
          WHERE table_owner = piv_owner
                AND table_name = piv_tablename
                AND partition_name LIKE trim(SUBSTR(pmax, 0, INSTR(pmax,'_') -1 ))||'%' ; --burak

         IF LENGTH(pmax) = 11 THEN  -- PYYYYMMDD_X
                   bastar := TO_DATE( SUBSTR(pmax,2,8),'yyyymmdd') + 1; --TO_DATE( SUBSTR(pmax,2),'yyyymmdd') + 1; --burak
                -- ekleme olacak mi ?
                 IF (bastar > max_date ) THEN
                     Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten var. Eklmeye gerek yok.',  piv_owner  || '.' || piv_tablename , NULL,v_dyntask );
                 END IF;
                -- ekleme baslar
                 WHILE ( bastar <= max_date) LOOP

                       v_pname := 'P' ||  TO_CHAR( bastar, 'yyyymmdd') ;
                       v_pname := REPLACE(v_pname, 'P', pin_part_prefix_replace);
                       IF pin_number_based THEN
                          v_boundary :=  TO_CHAR( bastar,'YYYYMMDD');
                       ELSE
                          v_boundary := 'TO_DATE(''' || TO_CHAR( bastar,'DD/MM/YYYY') || ' 00:00:00'',''dd/mm/yyyy hh24:mi:ss'' )';
                       END IF;

                           FOR i IN v_dual_values.FIRST..v_dual_values.LAST LOOP

                                  v_dual_val  := trim(SUBSTR(v_dual_values(i), INSTR(v_dual_values(i),',')+1 )) ;
                               v_dual_part := TO_CHAR( TO_NUMBER( trim(SUBSTR(v_dual_values(i), INSTR(v_dual_values(i),',')+1 )) ) - 1 ) ;

                               v_pname_index     := v_pname||'_'|| v_dual_part ;


                               v_dyntask :='  ALTER TABLE ' || piv_owner || '.' || piv_tablename || c_crlf ||
                                         ' ADD PARTITION ' ||  v_pname_index ||' VALUES LESS THAN ('|| v_boundary ||','||v_dual_val||' ) ' || c_crlf
                                         || '   TABLESPACE '||  ts_name (v_def_tablespace, b_spread_ts, part_sayac ) ||  c_crlf
                                         || ( CASE WHEN n_subpart_cnt=0
                                                   THEN NULL
                                                   ELSE sp_statement(v_pname_index, v_def_tablespace, b_spread_ts, pin_tsnum_start, pin_tsnum_end )
                                              END) ;
                                                               EXECUTE IMMEDIATE v_dyntask;

                                  Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Add partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || v_pname_index , NULL,v_dyntask );
                           END LOOP;

                      bastar := bastar + 1;
                     part_sayac := part_sayac + 1;
                     IF part_sayac > pin_tsnum_end  THEN
                         part_sayac := pin_tsnum_start;
                     END IF;
                 END LOOP;
            ELSIF  LENGTH(pmax) = 9 THEN  -- PYYYYMM_X


               bastar := ADD_MONTHS(TO_DATE( SUBSTR(pmax,2,6) || '01','yyyymmdd'), 1); --ADD_MONTHS(TO_DATE( SUBSTR(pmax,2) || '01','yyyymmdd'), 1); --burak

               -- ekleme olacak mi ?
               IF ( bastar > max_date ) THEN
                     Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten var. Eklmeye gerek yok.',  piv_owner  || '.' || piv_tablename  , NULL,v_dyntask );
               END IF;
               -- ekleme baslar
               WHILE ( bastar <= max_date) LOOP

                  v_pname := 'P' ||  TO_CHAR( bastar, 'yyyymm') ;
                  v_pname := REPLACE(v_pname, 'P', pin_part_prefix_replace);
                  IF pin_number_based THEN
                        v_boundary :=  TO_CHAR( LAST_DAY(bastar),'YYYYMM');
                  ELSE
                        v_boundary := 'TO_DATE(''' || TO_CHAR( LAST_DAY(bastar),'DD/MM/YYYY') || ' 00:00:00'',''dd/mm/yyyy hh24:mi:ss'' )';
                  END IF;

                           FOR i IN v_dual_values.FIRST..v_dual_values.LAST LOOP

                                  v_dual_val := trim(SUBSTR(v_dual_values(i), INSTR(v_dual_values(i),',')+1 )) ;
                               v_dual_part := TO_CHAR( TO_NUMBER( trim(SUBSTR(v_dual_values(i), INSTR(v_dual_values(i),',')+1 )) ) - 1 ) ;

                               v_pname_index     := v_pname||'_'|| v_dual_part ;

                              v_dyntask := '  ALTER TABLE ' || piv_owner || '.' || piv_tablename || c_crlf ||
                                                 ' ADD PARTITION ' ||  v_pname_index  ||' VALUES LESS THAN ('|| v_boundary||','||v_dual_val||') ' || c_crlf
                                                 || '   TABLESPACE '||  ts_name (v_def_tablespace, b_spread_ts, part_sayac ) ||  c_crlf
                                                 || ( CASE WHEN n_subpart_cnt=0
                                                           THEN NULL
                                                           ELSE sp_statement(v_pname_index, v_def_tablespace, b_spread_ts, pin_tsnum_start, pin_tsnum_end )
                                                      END) ;
                              EXECUTE IMMEDIATE v_dyntask;

                                  Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Add partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || v_pname_index , NULL,v_dyntask );
                           END LOOP;


                    bastar := LAST_DAY(bastar )+ 1;
                  part_sayac := part_sayac + 1;
                  IF part_sayac > pin_tsnum_end  THEN
                      part_sayac := pin_tsnum_start;
                  END IF;
               END LOOP;

            ELSIF  LENGTH(pmax) = 7 THEN  -- PYYYY_X
               bastar := TO_DATE( TO_CHAR(TO_NUMBER(SUBSTR(pmax,2,4)) + 1,'fm9999') || '1231','yyyymmdd');
               bittar := TO_DATE( TO_CHAR(max_date,'yyyy') || '1231','yyyymmdd');
                -- ekleme olacak mi ?
               IF ( bastar > bittar ) THEN
                     Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten var. Eklmeye gerek yok.',  piv_owner  || '.' || piv_tablename  , NULL,v_dyntask );
               END IF;
               -- ekleme baslar
               WHILE ( bastar <= bittar) LOOP
                  v_pname := 'P' ||  TO_CHAR( bastar, 'yyyy') ;
                  v_pname := REPLACE(v_pname, 'P', pin_part_prefix_replace);
                  IF pin_number_based THEN
                        v_boundary :=  TO_CHAR( LAST_DAY(bastar),'YYYY');
                  ELSE
                        v_boundary := 'TO_DATE(''' || TO_CHAR(bastar,'dd/mm/yyyy') || ' 00:00:00'',''dd/mm/yyyy hh24:mi:ss'' )';
                  END IF;

                           FOR i IN v_dual_values.FIRST..v_dual_values.LAST LOOP

                                  v_dual_val := trim(SUBSTR(v_dual_values(i), INSTR(v_dual_values(i),',')+1 )) ;
                               v_dual_part := TO_CHAR( TO_NUMBER( trim(SUBSTR(v_dual_values(i), INSTR(v_dual_values(i),',')+1 )) ) - 1 ) ;

                               v_pname_index     := v_pname||'_'|| v_dual_part ;

                                  v_dyntask := '  ALTER TABLE ' ||  piv_owner || '.' ||  piv_tablename ||  c_crlf
                                                     || ' ADD PARTITION ' ||  v_pname_index ||' VALUES LESS THAN ('|| v_boundary||','||v_dual_val||') ' || c_crlf
                                                     || '   TABLESPACE '||  ts_name (v_def_tablespace, b_spread_ts, part_sayac ) ||  c_crlf
                                                     || ( CASE WHEN n_subpart_cnt=0
                                                               THEN NULL
                                                               ELSE sp_statement(v_pname_index , v_def_tablespace, b_spread_ts , pin_tsnum_start, pin_tsnum_end)
                                                          END) ;
                                  EXECUTE IMMEDIATE v_dyntask;
                                  Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Add partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || v_pname_index , NULL,v_dyntask );
                           END LOOP;

                    bastar := ADD_MONTHS(bastar, 12);
                  part_sayac := part_sayac + 1;
                  IF part_sayac > pin_tsnum_end  THEN
                     part_sayac := pin_tsnum_start;
                  END IF;
               END LOOP;
          END IF;

    ELSE  -- FOR 1 way indexed partitions

         IF LENGTH(pmax) = 9 THEN  -- PYYYYMMDD
                   bastar := TO_DATE( SUBSTR(pmax,2,8),'yyyymmdd') + 1; --TO_DATE( SUBSTR(pmax,2),'yyyymmdd') + 1; --burak
                -- ekleme olacak mi ?
                 IF (bastar > max_date ) THEN
                     Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten var. Eklmeye gerek yok.',  piv_owner  || '.' || piv_tablename , NULL,v_dyntask );
                 END IF;
                -- ekleme baslar
                 WHILE ( bastar <= max_date) LOOP

                       v_pname := 'P' ||  TO_CHAR( bastar, 'yyyymmdd') ;
                       v_pname := REPLACE(v_pname, 'P', pin_part_prefix_replace);
                       IF pin_number_based THEN
                          v_boundary :=  TO_CHAR( bastar+1,'YYYYMMDD');
                       ELSE
                          v_boundary := 'TO_DATE(''' || TO_CHAR( bastar+1,'DD/MM/YYYY') || ' 00:00:00'',''dd/mm/yyyy hh24:mi:ss'' )';
                       END IF;

                           v_dyntask :='  ALTER TABLE ' || piv_owner || '.' || piv_tablename || c_crlf ||
                                     ' ADD PARTITION ' ||  v_pname ||' VALUES LESS THAN ('|| v_boundary ||') ' || c_crlf
                                     || '   TABLESPACE '||  ts_name (v_def_tablespace, b_spread_ts, part_sayac ) ||  c_crlf
                                     || ( CASE WHEN n_subpart_cnt=0
                                               THEN NULL
                                               ELSE sp_statement(v_pname, v_def_tablespace, b_spread_ts, pin_tsnum_start, pin_tsnum_end )
                                          END) ;
                                                           EXECUTE IMMEDIATE v_dyntask;


                     Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Add partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || v_pname , NULL,v_dyntask );
                      bastar := bastar + 1;
                     part_sayac := part_sayac + 1;
                     IF part_sayac > pin_tsnum_end  THEN
                         part_sayac := pin_tsnum_start;
                     END IF;
                 END LOOP;

            ELSIF  LENGTH(pmax) = 7 THEN  -- PYYYYMM

               bastar := ADD_MONTHS(TO_DATE( SUBSTR(pmax,2,6) || '01','yyyymmdd'), 1); --ADD_MONTHS(TO_DATE( SUBSTR(pmax,2) || '01','yyyymmdd'), 1); --burak

               -- ekleme olacak mi ?
               IF ( bastar > max_date ) THEN
                     Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten var. Eklmeye gerek yok.',  piv_owner  || '.' || piv_tablename  , NULL,v_dyntask );
               END IF;
               -- ekleme baslar
               WHILE ( bastar <= max_date) LOOP

                  v_pname := 'P' ||  TO_CHAR( bastar, 'yyyymm') ;
                  v_pname := REPLACE(v_pname, 'P', pin_part_prefix_replace);
                  IF pin_number_based THEN
                        v_boundary :=  TO_CHAR( LAST_DAY(bastar)+1,'YYYYMM');
                  ELSE
                        v_boundary := 'TO_DATE(''' || TO_CHAR( LAST_DAY(bastar)+1,'DD/MM/YYYY') || ' 00:00:00'',''dd/mm/yyyy hh24:mi:ss'' )';
                  END IF;

                              v_dyntask := '  ALTER TABLE ' || piv_owner || '.' || piv_tablename || c_crlf ||
                                                 ' ADD PARTITION ' ||  v_pname ||' VALUES LESS THAN ('|| v_boundary|| ') ' || c_crlf
                                                 || '   TABLESPACE '||  ts_name (v_def_tablespace, b_spread_ts, part_sayac ) ||  c_crlf
                                                 || ( CASE WHEN n_subpart_cnt=0
                                                           THEN NULL
                                                           ELSE sp_statement(v_pname, v_def_tablespace, b_spread_ts, pin_tsnum_start, pin_tsnum_end )
                                                      END) ;
                              EXECUTE IMMEDIATE v_dyntask;


                  Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Add partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || v_pname , NULL,v_dyntask );
                    bastar := LAST_DAY(bastar )+ 1;
                  part_sayac := part_sayac + 1;
                  IF part_sayac > pin_tsnum_end  THEN
                      part_sayac := pin_tsnum_start;
                  END IF;

               END LOOP;

            ELSIF  LENGTH(pmax) = 5 THEN  -- PYYYY
               bastar := TO_DATE( TO_CHAR(TO_NUMBER(SUBSTR(pmax,2,4)) + 1,'fm9999') || '1231','yyyymmdd');
               bittar := TO_DATE( TO_CHAR(max_date,'yyyy') || '1231','yyyymmdd');
                -- ekleme olacak mi ?
               IF ( bastar > bittar ) THEN
                     Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten var. Eklmeye gerek yok.',  piv_owner  || '.' || piv_tablename  , NULL,v_dyntask );
               END IF;
               -- ekleme baslar
               WHILE ( bastar <= bittar) LOOP
                  v_pname := 'P' ||  TO_CHAR( bastar, 'yyyy') ;
                  v_pname := REPLACE(v_pname, 'P', pin_part_prefix_replace);
                  IF pin_number_based THEN
                        v_boundary :=  TO_CHAR( LAST_DAY(bastar)+1,'YYYY');
                  ELSE
                        v_boundary := 'TO_DATE(''' || TO_CHAR(bastar+1,'dd/mm/yyyy') || ' 00:00:00'',''dd/mm/yyyy hh24:mi:ss'' )';
                  END IF;


                          v_dyntask := '  ALTER TABLE ' ||  piv_owner || '.' ||  piv_tablename ||  c_crlf
                                             || ' ADD PARTITION ' ||  v_pname ||' VALUES LESS THAN ('|| v_boundary||') ' || c_crlf
                                             || '   TABLESPACE '||  ts_name (v_def_tablespace, b_spread_ts, part_sayac ) ||  c_crlf
                                             || ( CASE WHEN n_subpart_cnt=0
                                                       THEN NULL
                                                       ELSE sp_statement(v_pname, v_def_tablespace, b_spread_ts , pin_tsnum_start, pin_tsnum_end)
                                                  END) ;
                          EXECUTE IMMEDIATE v_dyntask;


                  Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Add partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || v_pname , NULL,v_dyntask );
                    bastar := ADD_MONTHS(bastar, 12);
                  part_sayac := part_sayac + 1;
                  IF part_sayac > pin_tsnum_end  THEN
                     part_sayac := pin_tsnum_start;
                  END IF;
               END LOOP;
          END IF;

    END IF ;

  EXCEPTION
       WHEN INVALID_PARAMETER THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'INVALID PARAMETER VALUE');
       RAISE_APPLICATION_ERROR(-20100, 'INVALID PARAMETER VALUE');
       WHEN INVALID_TS_NAME THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Partition'' lara ait Tablespace adlarinin sonu 2 rakam ile bitmeli.');
       RAISE_APPLICATION_ERROR(-20101, 'Partition'' lara ait Tablespace adlarinin sonu 2 rakam ile bitmeli.');
       WHEN INVALID_PART_NAME THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Tarih partitionlarinin isimleri PYYYY[MM][DD] formatinda olmalidir.');
       RAISE_APPLICATION_ERROR(-20102, 'Tarih partitionlarinin isimleri PYYYY[MM][DD] formatinda olmalidir.');
     WHEN OTHERS THEN
        -- Assign values to the log variables, using built-in functions.
        Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
        RAISE;
END;


  ----------------------------------------------------------------
  --
  --   drop_date_partition
  --
  --
  --   Created : 1/04/2004 MK
  --
  --   Bu prosedur standart isimlere sahip range ya da range+hash(comp)
  --   tablolardan (range gun, ay ya da yil olan) partition(lar) drop etmek icin
  --   kullanilabilir.
  --
  --   pid_date_less_eq tarihi dahil daha eski partitionlar drop edilir.
  --
  --   Tablo Partition name formati : PYYYY, PYYYYMM, PYYYYMMDD
  --
  --   Parametreler
  --  piv_owner                  :  Tablo sahibi
  --  piv_tablename              :  Tablo Adi
  --  pid_date_less_eq           :  Bu gun dahil daha eski tum partitionlar drop edilecek
  --------------------------------------------------------------
  PROCEDURE drop_date_partition (
     piv_owner                  IN       VARCHAR2
   , piv_tablename              IN       VARCHAR2
   , pid_date_less_eq           IN       DATE
   , pin_part_prefix_replace    IN       VARCHAR2 DEFAULT 'P'
   , pin_dual_index                IN         BOOLEAN  DEFAULT FALSE
  )
  IS
     -- degiskenler
     l_hv             LONG;
     pmin             VARCHAR2(80);
     min_pdate        DATE;
     vpname           VARCHAR2(50);

      TYPE t_dual_parts   IS TABLE OF VARCHAR2(100);
     v_dual_parts t_dual_parts;


     -- hata ile ilgili tanimlar
     PART_NAME_INVALID EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(PART_NAME_INVALID, -20101);
     TABLE_NOT_DATE_PARTITIONED EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(TABLE_NOT_DATE_PARTITIONED, -20102);
    -- drop_date_partition burda baslar
    BEGIN

       gv_proc := c_pck || '.drop_date_partition';

        SELECT MIN(partition_name)
          INTO pmin
          FROM ALL_TAB_PARTITIONS
         WHERE table_owner = piv_owner
           AND table_name = piv_tablename ;

        SELECT high_value
          INTO l_hv
          FROM ALL_TAB_PARTITIONS
         WHERE table_name = piv_tablename
           AND table_owner = piv_owner
           AND partition_name = pmin ;

          --  Number_based Date partiion li tablolada drop edebilmek
          --  icin kaldirildi mk 09.06.2004
          --         if substr(l_hv,1,7) <> 'TO_DATE' THEN
          --             RAISE TABLE_NOT_DATE_PARTITIONED;
          --         END if;

        pmin := REPLACE(pmin , pin_part_prefix_replace, 'P');

        --IF  NOT ( SUBSTR(pmin,1,5)  >= 'P1990' AND SUBSTR(pmin,1,5)  <= 'P2990' )  THEN
        --    RAISE PART_NAME_INVALID;
        --END IF;

        IF pin_dual_index = FALSE THEN

                    IF LENGTH(pmin) = 9 THEN  -- PYYYYMMDD
                              min_pdate := TO_DATE( SUBSTR(pmin,2),'yyyymmdd');
                             IF (min_pdate > pid_date_less_eq  ) THEN
                                 Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten olmadigindan drop edilme islemine gerek yok.. .',  piv_owner  || '.' || piv_tablename  , NULL,v_dyntask );
                             END IF;
                             WHILE ( min_pdate <= pid_date_less_eq ) LOOP
                                vpname := 'P' ||  TO_CHAR( min_pdate,'yyyymmdd');
                                vpname := REPLACE(vpname ,'P', pin_part_prefix_replace);
                                v_dyntask :='  ALTER TABLE ' || piv_owner || '.' || piv_tablename || c_crlf ||
                                                 ' DROP PARTITION ' || vpname;
                                EXECUTE IMMEDIATE v_dyntask;
                              Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Drop partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || vpname, NULL,v_dyntask );
                                  min_pdate := min_pdate + 1;
                             END LOOP;
                        ELSIF  LENGTH(pmin) = 7 THEN  -- PYYYYMM
                           min_pdate := TO_DATE( SUBSTR(pmin,2,6) || '01','yyyymmdd');
                           IF (min_pdate > pid_date_less_eq  ) THEN
                                 Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten olmadigindan drop edilme islemine gerek yok.. .',  piv_owner  || '.' || piv_tablename   , NULL,v_dyntask );
                           END IF;
                           WHILE ( min_pdate <= pid_date_less_eq) LOOP
                              vpname := 'P' ||  TO_CHAR( min_pdate,'yyyymm');
                              vpname := REPLACE(vpname ,'P', pin_part_prefix_replace);
                              v_dyntask := '  ALTER TABLE ' || piv_owner || '.' || piv_tablename || c_crlf ||
                                                 ' DROP PARTITION ' || vpname;
                              EXECUTE IMMEDIATE v_dyntask;
                              Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Drop partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || vpname, NULL,v_dyntask );
                                min_pdate := ADD_MONTHS(min_pdate, 1);
                           END LOOP;
                        ELSIF  LENGTH(pmin) = 5 THEN  -- PYYYY
                           min_pdate := TO_DATE( SUBSTR(pmin,2,4) || '0101','yyyymmdd');
                           IF (min_pdate > pid_date_less_eq  ) THEN
                               Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten olmadigindan drop edilme islemine gerek yok.. .',  piv_owner  || '.' || piv_tablename   , NULL,v_dyntask );
                           END IF;
                           WHILE ( min_pdate <= pid_date_less_eq) LOOP
                              vpname := 'P' ||  TO_CHAR( min_pdate,'yyyy');
                              vpname := REPLACE(vpname ,'P', pin_part_prefix_replace);
                              v_dyntask := '  ALTER TABLE ' || piv_owner || '.' || piv_tablename || c_crlf ||
                                                 ' DROP PARTITION ' || vpname;
                              EXECUTE IMMEDIATE v_dyntask;
                              Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Drop partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || vpname, NULL,v_dyntask );
                                min_pdate := ADD_MONTHS(min_pdate,12);
                           END LOOP;
                       END IF;

                ELSE


                    IF LENGTH(pmin) = 11 THEN  -- PYYYYMMDD_X
                              min_pdate := TO_DATE( SUBSTR(pmin,2,8),'yyyymmdd');
                             IF (min_pdate > pid_date_less_eq  ) THEN
                                 Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten olmadigindan drop edilme islemine gerek yok.. .',  piv_owner  || '.' || piv_tablename  , NULL,v_dyntask );
                             END IF;
                             WHILE ( min_pdate <= pid_date_less_eq ) LOOP

                                  SELECT
                                  partition_name
                                  BULK COLLECT INTO v_dual_parts
                                  FROM ALL_TAB_PARTITIONS
                                  WHERE table_owner = piv_owner
                                  AND table_name = piv_tablename
                                  AND partition_name LIKE 'P' ||  TO_CHAR( min_pdate,'yyyymmdd')||'%' ;

                                  FOR i IN v_dual_parts.FIRST..v_dual_parts.LAST LOOP

                                        vpname := v_dual_parts(i);
                                        vpname := REPLACE(vpname ,'P', pin_part_prefix_replace);
                                        v_dyntask :='  ALTER TABLE ' || piv_owner || '.' || piv_tablename || c_crlf ||
                                                         ' DROP PARTITION ' || vpname;
                                        EXECUTE IMMEDIATE v_dyntask;
                                      Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Drop partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || vpname, NULL,v_dyntask );

                                 END LOOP;

                                   min_pdate := min_pdate + 1;

                             END LOOP;
                        ELSIF  LENGTH(pmin) = 9 THEN  -- PYYYYMM_X
                           min_pdate := TO_DATE( SUBSTR(pmin,2,6) || '01','yyyymmdd');
                           IF (min_pdate > pid_date_less_eq  ) THEN
                                 Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten olmadigindan drop edilme islemine gerek yok.. .',  piv_owner  || '.' || piv_tablename   , NULL,v_dyntask );
                           END IF;
                           WHILE ( min_pdate <= pid_date_less_eq) LOOP

                                  SELECT
                                  partition_name
                                  BULK COLLECT INTO v_dual_parts
                                  FROM ALL_TAB_PARTITIONS
                                  WHERE table_owner = piv_owner
                                  AND table_name = piv_tablename
                                  AND partition_name LIKE 'P' ||  TO_CHAR( min_pdate,'yyyymm')||'%' ;

                                  FOR i IN v_dual_parts.FIRST..v_dual_parts.LAST LOOP

                                        vpname := v_dual_parts(i);
                                        vpname := REPLACE(vpname ,'P', pin_part_prefix_replace);
                                        v_dyntask :='  ALTER TABLE ' || piv_owner || '.' || piv_tablename || c_crlf ||
                                                         ' DROP PARTITION ' || vpname;
                                        EXECUTE IMMEDIATE v_dyntask;
                                      Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Drop partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || vpname, NULL,v_dyntask );

                                 END LOOP;

                                min_pdate := ADD_MONTHS(min_pdate, 1);

                           END LOOP;
                        ELSIF  LENGTH(pmin) = 7 THEN  -- PYYYY_X
                           min_pdate := TO_DATE( SUBSTR(pmin,2,4) || '0101','yyyymmdd');
                           IF (min_pdate > pid_date_less_eq  ) THEN
                               Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Istenen tarihlere ait partitionlar zaten olmadigindan drop edilme islemine gerek yok.. .',  piv_owner  || '.' || piv_tablename   , NULL,v_dyntask );
                           END IF;
                           WHILE ( min_pdate <= pid_date_less_eq) LOOP

                                  SELECT
                                  partition_name
                                  BULK COLLECT INTO v_dual_parts
                                  FROM ALL_TAB_PARTITIONS
                                  WHERE table_owner = piv_owner
                                  AND table_name = piv_tablename
                                  AND partition_name LIKE 'P' ||  TO_CHAR( min_pdate,'yyyy')||'%' ;

                                  FOR i IN v_dual_parts.FIRST..v_dual_parts.LAST LOOP

                                        vpname := v_dual_parts(i);
                                        vpname := REPLACE(vpname ,'P', pin_part_prefix_replace);
                                        v_dyntask :='  ALTER TABLE ' || piv_owner || '.' || piv_tablename || c_crlf ||
                                                         ' DROP PARTITION ' || vpname;
                                        EXECUTE IMMEDIATE v_dyntask;
                                      Plib.o_log.LOG(1, 4,  gv_proc || c_delim || 'Drop partition islemi OK.',  piv_owner  || '.' || piv_tablename || '.' || vpname, NULL,v_dyntask );

                                 END LOOP;

                                min_pdate := ADD_MONTHS(min_pdate,12);
                           END LOOP;
                       END IF;

                END IF ;


  EXCEPTION
       WHEN PART_NAME_INVALID THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Partition adlari PYYYYMMDD formatinda olmali.');
       RAISE_APPLICATION_ERROR(-20101, 'PART_NAME_INVALID : Partition adlari PYYYYMMDD formatinda olmali.');
       WHEN TABLE_NOT_DATE_PARTITIONED THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Tablo Tarih partition''li degil.');
       RAISE_APPLICATION_ERROR(-20102, 'TABLE_NOT_DATE_PARTITIONED : Tablo Tarih partition''li degil.');
     WHEN OTHERS THEN
        -- Assign values to the log variables, using built-in functions.
        -- son partition drop edildiginde asagidaki hata aliniyor. Bu durumda
        -- o partition' i truncate edelim
        IF SQLCODE = -14083 THEN
             v_dyntask := REPLACE(v_dyntask,'DROP','TRUNCATE');
             EXECUTE IMMEDIATE v_dyntask;
             Plib.o_log.LOG(1, 3,  gv_proc || c_delim || 'Son partition drop edielemez o nedenle truncate islemi yapildi.',  piv_owner  || '.' || piv_tablename || '.' || 'PXXXXXXXX'  , NULL,v_dyntask );
        ELSE
             Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
             RAISE;
        END IF;
END;


  ----------------------------------------------------------------
  --
  --   add_date_partition
  --
  --
  --   Created : 26/02/2004 MK
  --
  --   Bu prosedur standart isimlere sahip range ya da range+hash(comp)
  --   tablolarda (range gun, ay ya da yil olan) en son max_date gunune ait
  --   partition olmak kaydi ile son pin_window_size partition kalacak sekilde
  --   duzenleme yapar ( partition ekler ya da cikarir)
  --
  --   Mevcut en buyuk tarihli partitiondan pid_max_date e kadar partition eklenir
  --   pin_window_size in disinda kalan eski tarihli partitionlar cikarilir
  --
  --   Partition name formati    : PYYYY, PYYYYMM, PYYYYMMDD
  --   Subpartition name formati : S<partition_name>_NNN (NNN 3 rakamli sayi)
  --   Parametreler
  --
  --  piv_owner                  :  Tablo sahibi
  --  piv_tablename              :  Tablo Adi
  --  pid_max_date               :  Sonucta Range partition'li tabloda en buyuk date
  --  pin_window_size            :  Tablo en fazla kac parititon li olacak
  --                                li partition ne olacak
  --
  --  pin_tablespace_type        IN       VARCHAR2 DEFAULT 'NULL' -- NULL mevcut ts kullanilacak, T temporary, P permanent, U:user defined
  --  pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
  --------------------------------------------------------------
  PROCEDURE window_date_partitions (
     piv_owner                  IN       VARCHAR2
   , piv_tablename              IN       VARCHAR2
   , pid_max_date               IN       DATE
   , pin_window_size            IN       NUMBER
   , pin_tablespace_type        IN       VARCHAR2 DEFAULT NULL -- NULL mevcutu kullan, T temporary, P permanent
   , pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
   , pin_spread                 IN       BOOLEAN  DEFAULT TRUE
   , pin_number_based           IN       BOOLEAN  DEFAULT FALSE
   , pin_tsnum_start            IN       NUMBER DEFAULT 1
   , pin_tsnum_end              IN       NUMBER DEFAULT 16
   , pin_part_prefix_replace    IN       VARCHAR2 DEFAULT 'P'
   , pin_dual_index                IN         BOOLEAN  DEFAULT FALSE
  )
  IS
     v_date_less_eq   DATE;
     d_maxtar         DATE;
     d_mintar         DATE;
     pmax             VARCHAR2(80);
     pmin             VARCHAR2(80);
     l_hv             LONG;
     v_window_size    NUMBER;
     PART_NAME_INVALID EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(PART_NAME_INVALID, -20101);

     TABLE_NOT_DATE_PARTITIONED EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(TABLE_NOT_DATE_PARTITIONED, -20102);

     NON_LOGICAL_OPERATION EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(NON_LOGICAL_OPERATION, -20103);

     CAN_NOT_DROP_MORE_THAN_LIMIT EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(NON_LOGICAL_OPERATION, -20104);

  BEGIN

        gv_proc := c_pck || '.window_date_partitions';

        -- mantiksal kontroller yapalim
        SELECT  MAX(partition_name), MIN(partition_name)
          INTO  pmax, pmin
          FROM ALL_TAB_PARTITIONS
         WHERE table_owner = piv_owner
           AND table_name = piv_tablename ;

        SELECT high_value
          INTO l_hv
          FROM ALL_TAB_PARTITIONS
         WHERE table_name = piv_tablename
           AND table_owner = piv_owner
           AND partition_name = pmax ;


                        pmax := REPLACE(pmax , pin_part_prefix_replace, 'P');
                        -- tablo date range partitionli mi?
                        IF pin_number_based = FALSE AND SUBSTR(l_hv,1,7) <> 'TO_DATE' THEN
                            RAISE TABLE_NOT_DATE_PARTITIONED;
                        END IF;
                        -- partition isimleri standarda uyuyor mu
                        --IF  NOT ( SUBSTR(pmax,1,5)  >= 'P1990' AND SUBSTR(pmax,1,5)  <= 'P2990' )  THEN
                        --    RAISE PART_NAME_INVALID;
                        --END IF;

                        pmax := REPLACE(pmax , 'P', pin_part_prefix_replace);

        IF pin_dual_index = FALSE THEN


                       IF LENGTH(pmax) = 9 THEN  -- PYYYYMMDD gunluk
                           d_maxtar := TO_DATE( SUBSTR(pmax,2),'yyyymmdd');
                           d_mintar := TO_DATE( SUBSTR(pmin,2),'yyyymmdd');
                           -- eklediginden window size ciktiginda mevcut maximum date den buyuk olmamali
                           IF ( pid_max_date - pin_window_size) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 7 den fazla gunluk partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size + 6) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                            v_window_size := pin_window_size;
                       ELSIF LENGTH(pmax) = 7 THEN  -- PYYYYMM aylik
                           d_maxtar := TO_DATE( SUBSTR(pmax,2,6) || '01' ,'yyyymmdd') ;
                           d_mintar := TO_DATE( SUBSTR(pmin,2,6) || '01' ,'yyyymmdd') ;
                           IF  ADD_MONTHS(pid_max_date, (-1) * pin_window_size) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 3 den fazla aylik partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size *30 + 90) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                           v_window_size := pid_max_date - ADD_MONTHS(pid_max_date, (-1) * pin_window_size);
                           --v_window_size := pin_window_size *30;
                       ELSIF LENGTH(pmax) = 5 THEN  -- PYYYY yillik
                           d_maxtar := TO_DATE( SUBSTR(pmax,2,4) || '0101','yyyymmdd');
                           d_mintar := TO_DATE( SUBSTR(pmin,2,4) || '0101','yyyymmdd');
                           IF  ADD_MONTHS(pid_max_date, (-1) * pin_window_size*12) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 3 den fazla yillik partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size *30*12 + 360*3) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                           v_window_size := pid_max_date - ADD_MONTHS(pid_max_date, (-1) * pin_window_size*12);
                           --v_window_size := pin_window_size *30*12;
                         END IF;

                     -- BURDA OPERASYON BASLAR ...
                     add_date_partition(piv_owner, piv_tablename, pid_max_date, pin_tablespace_type,pin_default_tablespace, pin_spread,  pin_number_based, pin_tsnum_start, pin_tsnum_end ,pin_part_prefix_replace,pin_dual_index );
                     v_date_less_eq := pid_max_date - v_window_size;
                     drop_date_partition(piv_owner, piv_tablename, v_date_less_eq );

            ELSE


                       IF LENGTH(pmax) = 11 THEN  -- PYYYYMMDD_X gunluk
                           d_maxtar := TO_DATE( SUBSTR(pmax,2,8),'yyyymmdd');
                           d_mintar := TO_DATE( SUBSTR(pmin,2,8),'yyyymmdd');
                           -- eklediginden window size ciktiginda mevcut maximum date den buyuk olmamali
                           IF ( pid_max_date - pin_window_size) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 7 den fazla gunluk partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size + 6) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                            v_window_size := pin_window_size;
                       ELSIF LENGTH(pmax) = 9 THEN  -- PYYYYMM_X aylik
                           d_maxtar := TO_DATE( SUBSTR(pmax,2,6) || '01' ,'yyyymmdd') ;
                           d_mintar := TO_DATE( SUBSTR(pmin,2,6) || '01' ,'yyyymmdd') ;
                           IF  ADD_MONTHS(pid_max_date, (-1) * pin_window_size) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 3 den fazla aylik partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size *30 + 90) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                           v_window_size := pid_max_date - ADD_MONTHS(pid_max_date, (-1) * pin_window_size);
                           --v_window_size := pin_window_size *30;
                       ELSIF LENGTH(pmax) = 7 THEN  -- PYYYY_X yillik
                           d_maxtar := TO_DATE( SUBSTR(pmax,2,4) || '0101','yyyymmdd');
                           d_mintar := TO_DATE( SUBSTR(pmin,2,4) || '0101','yyyymmdd');
                           IF  ADD_MONTHS(pid_max_date, (-1) * pin_window_size*12) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 3 den fazla yillik partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size *30*12 + 360*3) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                           v_window_size := pid_max_date - ADD_MONTHS(pid_max_date, (-1) * pin_window_size*12);
                           --v_window_size := pin_window_size *30*12;
                         END IF;

                     -- BURDA OPERASYON BASLAR ...
                     add_date_partition(piv_owner, piv_tablename, pid_max_date, pin_tablespace_type,pin_default_tablespace, pin_spread,  pin_number_based, pin_tsnum_start, pin_tsnum_end ,pin_part_prefix_replace,pin_dual_index );
                     v_date_less_eq := pid_max_date - v_window_size;
                     drop_date_partition(piv_owner, piv_tablename, v_date_less_eq ,pin_part_prefix_replace,pin_dual_index);

            END IF ;

  EXCEPTION

       WHEN PART_NAME_INVALID THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Partition adlari PYYYYMMDD formatinda olmali.');
       RAISE_APPLICATION_ERROR(-20101, 'PART_NAME_INVALID : Partition adlari PYYYYMMDD formatinda olmali.');

       WHEN TABLE_NOT_DATE_PARTITIONED THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Tablo Tarih partition''li degil.');
       RAISE_APPLICATION_ERROR(-20102, 'TABLE_NOT_DATE_PARTITIONED : Tablo Tarih partition''li degil.');

       WHEN NON_LOGICAL_OPERATION THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Verilen en buyuk tarihten window size cikinca, mevcut tablonun en buyuk tarihli partition''indan buyuk olamaz (Tablonun mevcut tum verisi yok olacak).');
       RAISE_APPLICATION_ERROR(-20103, 'NON_LOGICAL_OPERATION : Verilen en buyuk tarihten window size cikinca, mevcut tablonun en buyuk tarihli partition''indan buyuk olamaz (Tablonun mevcut verisi yok olacak).');

       WHEN CAN_NOT_DROP_MORE_THAN_LIMIT THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Verilen window'' a gore 7 den fazla gunluk veya 3 ten fazla aylik veya 3 den fazla yillik partition drop edilmek isteniyor. Islem iptal edildi. Islemi kontrol ediniz.');
       RAISE_APPLICATION_ERROR(-20104, 'CAN_NOT_DROP_PART_MORE_THAN_LIMIT : Verilen window'' a gore 7 den fazla gunluk veya 3 ten fazla aylik veya 3 den fazla yillik partition drop edilmek isteniyor. Islem iptal edildi. Islemi kontrol ediniz.');

  END;
-------------------------------------------------------------------------------------------------------------------
-- Olmus sessionlarin yarattiklari locklari temizler
-------------------------------------------------------------------------------------------------------------------
  PROCEDURE control_lock IS
  l_tokens token_list;
  v_token_cnt NUMBER;
  lb_alive BOOLEAN:=FALSE;
  BEGIN
         FOR crec IN (  SELECT pname,pvalue2,in_use
                            FROM GPARAMS
                           WHERE NVL(in_use_cnt,0) > 0
                             AND ptip = 'FLAG'
                             AND pname NOT IN ('CALL_SUBSCRIBER_SYN','CALL_INSERT_RUNNING')-- added by Asli Sahin 08.12.2005,Nezih 30.12.2005
                               ) LOOP
          -- session lar canli mi bak
          tokenize(crec.pvalue2,l_tokens,v_token_cnt,'-',FALSE);
          FOR i IN 1..v_token_cnt LOOP
             BEGIN
                lb_alive:=dbms_session.is_session_alive(l_tokens(i));
             EXCEPTION WHEN OTHERS THEN
                IF SQLCODE <> -22 THEN -- ivalid sid
                   lb_alive:=FALSE;
                ELSE
                   RAISE;
                END IF;
             END;
             IF NOT lb_alive THEN
             -- session olmus unlock etmek lazim
                lb_alive:=undo_lock(crec.pname,crec.in_use,l_tokens(i));
             END IF;
            END LOOP;
        END LOOP;
--          plib.o_log.LOG (1, 3, gv_proc || c_delim ||  p_name || ' icin kilit islemi yapilamadi, '|| to_char(c_sleep_time)||' saniye bekleyip tekrar deneniyor.', 'gparams');
  END;





  ----------------------------------------------------------------
  --
  --   send_mail
  --
  --
  --   Created : 26/03/2004 MK
  --
  --   This procedure uses the UTL_SMTP package to send an email message.
  --   Up to three file names may be specified as attachments.
  --   Parameters are:
  --     1) from_name (varchar2)
  --     2) to_name   (varchar2)
  --     3) subject   (varchar2)
  --     4) message   (varchar2)
  --   eg.
  --     mail_files( from_name => 'sender' ,
  --                 to_name   => 'receiver@turkcell.com.tr' ,
  --                 subject   => 'A test',
  --                 message   => 'A test message');
  --    Most of the parameters are self-explanatory. "message" is a varchar2
  --   parameter, up to 32767 bytes long which contains the text of the message
  --   to be placed in the main body of the email.
  --
  --------------------------------------------------------------
  PROCEDURE send_mail ( p_from_name VARCHAR2,
                        p_to_name VARCHAR2,
                        p_subject VARCHAR2,
                        p_message VARCHAR2
  ) IS

    c_smtp_server      VARCHAR2(14) := '10.1.240.133';
    c_smtp_server_port NUMBER  := 25;
    max_size           NUMBER  := 32767;
    mesg               VARCHAR2(32767);
    conn               UTL_SMTP.CONNECTION;
  BEGIN

   gv_proc := c_pck || '.send_mail';

   -- Open the SMTP connection ...
   -- ------------------------
   conn:= utl_smtp.open_connection( c_smtp_server, c_smtp_server_port );
   -- Initial handshaking ...
   -- -------------------
   utl_smtp.helo( conn, c_smtp_server );
   utl_smtp.mail( conn, p_from_name );
   utl_smtp.rcpt( conn, p_to_name );
   utl_smtp.open_data ( conn );
   -- build the start of the mail message ...
   -- -----------------------------------
   mesg:= 'Date: ' || TO_CHAR( SYSDATE, 'dd Mon yy hh24:mi:ss' ) ||c_crlf    ||
          'From: ' || p_from_name ||c_crlf    ||
          'Subject: ' || p_subject || c_crlf ||
          'To: ' || p_to_name || c_crlf ||
          'Mime-Version: 1.0' || c_crlf ||
          'Content-Type: multipart/mixed; boundary="DMW.Boundary.605592468"' || c_crlf ||
          '' || c_crlf ||
          'This is a Mime message, which your current mail reader may not' || c_crlf ||
          'understand. Parts of the message will appear as text. If the remainder' || c_crlf ||
          'appears as random characters in the message body, instead of as' || c_crlf ||
          'attachments, then you''ll have to extract these parts and decode them' || c_crlf ||
          'manually.' || c_crlf ||
          '' || c_crlf ||
          '--DMW.Boundary.605592468' || c_crlf ||
          'Content-Type: text/plain; name="message.txt"; charset=US-ASCII' || c_crlf ||
          'Content-Disposition: inline; filename="message.txt"' || c_crlf ||
          'Content-Transfer-Encoding: 7bit' || c_crlf ||
          '' || c_crlf ||
          SUBSTR(p_message, 1, max_size) || c_crlf ;
   utl_smtp.write_data ( conn, mesg );
   -- append the final boundary line ...
   mesg := c_crlf || '--DMW.Boundary.605592468--' || c_crlf;
   utl_smtp.write_data ( conn, mesg );
   -- and close the SMTP connection  ...
   utl_smtp.close_data( conn );
   utl_smtp.quit( conn );
   Plib.o_log.LOG (1, 4, gv_proc || c_delim || ' Mesaj gonderildi. Alici :  ' || p_to_name );

  EXCEPTION
     WHEN OTHERS THEN
        -- Assign values to the log variables, using built-in functions.
        Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, NULL);
        RAISE;

END;


  ----------------------------------------------------------------
  --
  --   sch_mail
  --
  --
  --   Created : 26/03/2004 MK
  --
  --  ilgili cadm_ett_sch_jobdesc tablosunda job icin tanimli(job_person1_info alaninda)
  --  e-mail adresine mesaj gonderir. Bu alan bos ise ya da job bulunamaz ise hata
  --  doner.
  --------------------------------------------------------------
  PROCEDURE sch_mail ( p_package_owner  IN VARCHAR2,
                       p_package_name  IN VARCHAR2,
                       p_procedure_name IN  VARCHAR2,
                       p_message IN VARCHAR2
  ) IS
  v_receiver1 VARCHAR2(50);
  v_receiver2 VARCHAR2(50);
  RECEIVER_NOT_FOUND EXCEPTION  ;
  PRAGMA EXCEPTION_INIT(RECEIVER_NOT_FOUND, -20100);
  JOB_DETAILS_MISSING EXCEPTION  ;
  PRAGMA EXCEPTION_INIT(JOB_DETAILS_MISSING, -20101);
  BEGIN

     gv_proc := c_pck || '.sch_mail';

    EXECUTE IMMEDIATE
    ' select job_person1_info, job_person2_info
       FROM ' || p_package_owner || '.cadm_ett_sch_jobdesc
     WHERE UPPER(job_owner) = UPPER(''' || p_package_owner || ''')
       AND UPPER(job_package) = UPPER(''' || p_package_name || ''')
       AND UPPER(job_procedure) =  UPPER(''' || p_procedure_name  || ''')'
      INTO v_receiver1, v_receiver2 ;

       v_receiver1 := NVL(v_receiver1,v_receiver2);
       IF v_receiver1 IS NULL THEN
          Plib.o_log.LOG (1, 3, gv_proc || c_delim || p_package_owner || '.' || p_package_name || '.' ||  p_procedure_name || ' isi icin sorumlu kisinin e-mail bilgisi eksik : (job_person1_info) ',  p_package_owner || '.CADM_ETT_SCH_JOBDESC');
          RAISE_APPLICATION_ERROR(-20100, gv_proc || c_delim || p_package_owner || '.' || p_package_name || '.' ||  p_procedure_name || ' isi icin sorumlu kisinin e-mail bilgisi eksik : (job_person1_info) ');
       END IF;
       send_mail ( p_package_owner , v_receiver1,  ' *** Message from Schedular '|| p_package_owner || ' (' || TO_CHAR( SYSDATE, 'dd.Mon.yyyy hh24:mi:ss' ) ||') ***' ,  p_package_name || '.' || p_procedure_name || ' :' || c_crlf || p_message);
  EXCEPTION
     WHEN NO_DATA_FOUND THEN
          Plib.o_log.LOG (1, 3, gv_proc || c_delim || p_package_owner || '.' || p_package_name || '.' ||  p_procedure_name || ' isine ait tanim bulunamadi. ',  p_package_owner || '.CADM_ETT_SCH_JOBDESC');
          RAISE_APPLICATION_ERROR(-20101, gv_proc || c_delim || p_package_owner || '.' || p_package_name || '.' ||  p_procedure_name || ' isine ait tanim bulunamadi. ');
  END;

  ----------------------------------------------------------------
  --
  --   get_last_partition
  --
  --
  --   Created : 14/04/2004
  --
  --   Nezih' ten gelen istek
  --   Bu prosedur pin_date ile verilen tarihten kucuk
  --   partition adi XXYYYYMM formatindaki en buyuk partiton'in ismini donduruyor
  --
  --------------------------------------------------------------
 FUNCTION get_last_partition (pin_table_name IN VARCHAR2, pin_owner IN VARCHAR2, pin_date IN DATE)
   RETURN VARCHAR2
  IS
   v_partition_name   ALL_TAB_PARTITIONS.partition_name%TYPE;
  BEGIN

    gv_proc := c_pck || '.get_last_partition';

      SELECT MAX (partition_name) max_partition
        INTO v_partition_name
        FROM ALL_TAB_PARTITIONS
       WHERE table_name = pin_table_name
         AND table_owner = pin_owner
         AND (SUBSTR (partition_name, 3)) <= (TO_CHAR (TRUNC (pin_date), 'YYYYMM'));

   RETURN v_partition_name;

   EXCEPTION
    WHEN NO_DATA_FOUND THEN
         RETURN NULL;
    WHEN OTHERS THEN
         -- Assign values to the log variables, using built-in functions.
         Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, NULL);
         RAISE;
  END;

  PROCEDURE index_operation ( p_operation IN VARCHAR2
                            , p_OWNER IN VARCHAR2
                            , piv_table_name IN VARCHAR2
                            , piv_index_name IN VARCHAR2 DEFAULT NULL
                            , piv_part_name IN VARCHAR2 DEFAULT NULL
                            , piv_parallel IN NUMBER DEFAULT 16
                            ) IS
  v_composite VARCHAR2(10);
  v_status    VARCHAR2(30);
  v_cmd_start VARCHAR2(30);
  v_proc VARCHAR2(30);
  BEGIN

    v_proc  := c_pck || '.index_operation';

    FOR CREC IN ( SELECT INDEX_NAME, PARTITIONED, STATUS, DEGREE
                    FROM ALL_INDEXES
                   WHERE TABLE_NAME  =  UPPER(piv_table_name)
                     AND table_owner = UPPER(p_OWNER)
                     AND index_name = NVL(UPPER(piv_index_name),index_name)
                 )
    LOOP
       IF CREC.PARTITIONED = 'YES' THEN

            FOR crec2 IN ( SELECT SUBPARTITION_NAME
                                   FROM ALL_IND_SUBPARTITIONS
                                  WHERE index_name = crec.index_name
                                    AND index_owner = p_owner
                                    AND partition_name = NVL(UPPER(piv_part_name),partition_name)
                                    AND (
                                          ( status = 'USABLE' AND p_operation =  'U' AND INDEX_NAME NOT LIKE '%SNAP$%')
                                          OR
                                          ( status <> 'USABLE' AND p_operation = 'R')
                                         )
                                   )
             LOOP
                      IF p_operation = 'R' THEN
                         v_dyntask :=  'ALTER INDEX '|| p_OWNER || '.' || CREC.INDEX_NAME || ' REBUILD  SUBPARTITION ' ||  CREC2.subPARTITION_NAME || '  PARALLEL ' || CREC.DEGREE;
                         EXECUTE IMMEDIATE v_dyntask;
                          o_log.LOG (11, 4, v_proc || c_delim || UPPER(piv_table_name) || ' Tablosunun  ' || CREC.INDEX_NAME || ' indeksi ' ||
                                            CREC2.SUBPARTITION_NAME  || ' SUBpartition'' i icin REBUILD edildi.',CREC.INDEX_NAME, NULL, v_dyntask );
                      ELSE
                          v_dyntask :=  'ALTER INDEX ' || p_OWNER || '.' || CREC.INDEX_NAME || ' modify  SUBPARTITION ' ||  CREC2.subPARTITION_NAME || '  UNUSABLE ';
                          EXECUTE IMMEDIATE v_dyntask;
                          o_log.LOG (12, 4, v_proc || c_delim || UPPER(piv_table_name) || ' Tablosunun  ' || CREC.INDEX_NAME || ' indeksi ' ||
                                            CREC2.SUBPARTITION_NAME  || ' SUBpartition'' i icin UNUSABLE edildi.',CREC.INDEX_NAME, NULL, v_dyntask );
                      END IF;


          END LOOP;

            FOR CREC1 IN ( SELECT COMPOSITE, PARTITION_NAME, status
                           FROM ALL_IND_PARTITIONS
                          WHERE index_name = crec.index_name
                               AND index_owner = p_owner
                            AND COMPOSITE = 'NO'
                            AND partition_name = NVL(UPPER(piv_part_name),partition_name)
                            AND ( ( status = 'USABLE' AND p_operation =  'U' AND INDEX_NAME NOT LIKE '%SNAP$%')
                                   OR
                                  ( status <> 'USABLE' AND p_operation = 'R')
                                 )
                        )
             LOOP
                      IF p_operation = 'R' THEN
                          v_dyntask :=  'ALTER INDEX '|| p_OWNER || '.' || CREC.INDEX_NAME || ' REBUILD PARTITION ' ||  CREC1.PARTITION_NAME ||  '  PARALLEL ' || CREC.DEGREE ;
                          EXECUTE IMMEDIATE v_dyntask;
                          o_log.LOG (11, 4, v_proc || c_delim ||  UPPER(piv_table_name) || ' Tablosunun  ' || CREC.INDEX_NAME || ' indeksi ' ||
                                            CREC1.PARTITION_NAME  || ' partition'' i icin REBUILD edildi.',CREC.INDEX_NAME, NULL, v_dyntask );
                      ELSE
                          v_dyntask :=  'ALTER INDEX ' || p_OWNER || '.' || CREC.INDEX_NAME || ' modify  PARTITION ' ||  CREC1.PARTITION_NAME || '  UNUSABLE ';
                          EXECUTE IMMEDIATE v_dyntask;
                          o_log.LOG (12, 4, v_proc || c_delim ||  UPPER(piv_table_name) || ' Tablosunun  ' || CREC.INDEX_NAME || ' indeksi ' ||
                                            CREC1.PARTITION_NAME  || ' partition'' i icin UNUSABLE edildi.',CREC.INDEX_NAME, NULL, v_dyntask );
                      END IF;
           END LOOP;
       ELSE
           IF ( ( crec.status = 'VALID' AND p_operation =  'U' AND crec.INDEX_NAME NOT LIKE '%SNAP$%')
                OR
                ( crec.status <> 'VALID' AND p_operation = 'R') )
           THEN
              IF p_operation =  'R' THEN
                 v_dyntask :=  'ALTER INDEX '|| p_OWNER || '.' || CREC.INDEX_NAME || ' REBUILD  PARALLEL ' || CREC.DEGREE;
                   EXECUTE IMMEDIATE v_dyntask;
                 o_log.LOG (11, 4,  v_proc || c_delim || UPPER(piv_table_name) || ' Tablosunun  ' || CREC.INDEX_NAME || ' indeksi ' ||
                                ''' i REBUILD edildi.', CREC.INDEX_NAME, NULL, v_dyntask );
              ELSE
                 v_dyntask :=  'ALTER INDEX '|| p_OWNER || '.' || CREC.INDEX_NAME || ' UNUSABLE ';
                   EXECUTE IMMEDIATE v_dyntask;
                 o_log.LOG (12, 4,  v_proc || c_delim ||  UPPER(piv_table_name) || ' Tablosunun  ' || CREC.INDEX_NAME || ' indeksi ' ||
                                ''' i UNUSABLE edildi.', CREC.INDEX_NAME, NULL, v_dyntask );
                 END IF;
           END IF;
       END IF;
    END LOOP;

    EXCEPTION
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, v_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;

  END;

  --------------------------------------------------------------
  --
  --   make_indexes_unusuable
  --
  --   make all of the indexes of a table (p_owner.p_table_name) unusable
  --
  --   make unusable all indexes of a table (normal, partitioned or subpartitioned)
  --     e.g
  --          piv_OWNER        : 'ANT'
  --          piv_table_name   : 'TEST'
  --   make unusable all indexes of a table for a partition/subparttion
  --     e.g
  --          piv_OWNER        : 'ANT'
  --          piv_table_name   : 'TEST'
  --          piv_index_name   :  NULL
  --          piv_part_name    : 'PART1'
  --   make unusable an index of a table (normal, partitioned or subpartitioned)
  --     e.g
  --          piv_OWNER        : 'ANT'
  --          piv_table_name   : 'TEST'
  --          piv_index_name   : 'IDX1'
  --   make unusable an index of a table for a partition/subparttion
  --     e.g
  --          piv_OWNER        : 'ANT'
  --          piv_table_name   : 'TEST'
  --          piv_index_name   : 'IDX1'
  --          piv_part_name    : 'PART1'
  --
  ----------------------------------------------------------------------------------------------------------------------------

  PROCEDURE make_indexes_unusable ( piv_OWNER IN VARCHAR2
                                   , piv_table_name IN VARCHAR2
                                   , piv_index_name IN VARCHAR2 DEFAULT NULL
                                   , piv_part_name IN VARCHAR2 DEFAULT NULL)IS
  BEGIN
      gv_proc  := c_pck || '.make_indexes_unusuable';
      index_operation ( 'U', piv_OWNER, piv_table_name, piv_index_name, piv_part_name);

   EXCEPTION
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;

  END;

  --------------------------------------------------------------
  --
  --   make_indexes_rebuild
  --
  --   rebuild all indexes of a table (normal, partitioned or subpartitioned)
  --     e.g
  --          piv_OWNER        : 'ANT'
  --          piv_table_name   : 'TEST'
  --   rebuild all indexes of a table for a partition/subparttion
  --     e.g
  --          piv_OWNER        : 'ANT'
  --          piv_table_name   : 'TEST'
  --          piv_index_name   :  NULL
  --          piv_part_name    : 'PART1'
  --   rebuild an index of a table (normal, partitioned or subpartitioned)
  --     e.g
  --          piv_OWNER        : 'ANT'
  --          piv_table_name   : 'TEST'
  --          piv_index_name   : 'IDX1'
  --   rebuild an index of a table for a partition/subparttion
  --     e.g
  --          piv_OWNER        : 'ANT'
  --          piv_table_name   : 'TEST'
  --          piv_index_name   : 'IDX1'
  --          piv_part_name    : 'PART1'
  --
  --------------------------------------------------------------
  PROCEDURE make_indexes_rebuild ( piv_OWNER IN VARCHAR2
                                 , piv_table_name IN VARCHAR2
                                 , piv_index_name IN VARCHAR2 DEFAULT NULL
                                 , piv_part_name IN VARCHAR2 DEFAULT NULL
                                 , piv_parallel_degree IN NUMBER DEFAULT 8 )IS
  BEGIN
     gv_proc  := c_pck || '.make_indexes_rebuild';
     index_operation ( 'R', piv_OWNER, piv_table_name, piv_index_name, piv_part_name, piv_parallel_degree );

   EXCEPTION
      WHEN OTHERS THEN
       -- Assign values to the log variables, using built-in functions.
       Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
       RAISE;
  END;



  --------------------------------------------------------------
  --
  --   part_ind_rebuild_concurrent
  --  rebuilds the an indexe of a table concurrently
  --
  --------------------------------------------------------------
  PROCEDURE part_ind_rebuild_concurrent
                               (  p_tab_name       IN VARCHAR2 ,
                                  p_idx_name       IN VARCHAR2 ,
                                   p_jobs_per_batch IN NUMBER DEFAULT 1,
                                  p_procs_per_job  IN NUMBER DEFAULT 1,
                                  p_force_opt      IN BOOLEAN DEFAULT FALSE) IS
   BEGIN
        dbms_pclxutil.build_part_index(p_jobs_per_batch, p_procs_per_job
                                         ,p_tab_name , p_idx_name, p_force_opt);
   END;




PROCEDURE change_constraint_status (
      piv_action                 IN       VARCHAR2 -- DISABLE / ENABLE
    , piv_owner                  IN       VARCHAR2
    , piv_table_name             IN       VARCHAR2
    , piv_constr_type            IN       VARCHAR2 DEFAULT NULL -- C,U,P,..
    , piv_constr_name            IN       VARCHAR2 DEFAULT NULL) IS

BEGIN

     gv_proc := c_pck || '.change_constraint_status';

    for rec in (select constraint_name
                    from all_constraints
                    where       nvl(piv_constr_name, constraint_name ) =  constraint_name
                          AND   nvl(piv_constr_type, constraint_type ) =  constraint_type
                          AND   piv_owner = owner
                          AND   piv_table_name = table_name
                          AND   status = DECODE(UPPER(piv_action),'ENABLE', 'DISABLED' , 'ENABLED' ) )
    loop

        --alter table table-name disable constraint constraint-name;

        v_dyntask := 'ALTER TABLE ' || piv_owner || '.' || piv_table_name || ' ' || piv_action || ' NOVALIDATE CONSTRAINT ' || rec.constraint_name;

        EXECUTE IMMEDIATE v_dyntask;

        Plib.o_log.LOG(1, gv_proc || c_delim ||  piv_owner  || '.' || piv_table_name , NULL,v_dyntask );

    end loop;

 EXCEPTION
     WHEN OTHERS THEN

      Plib.o_log.LOG(SQLCODE, 1, gv_proc || c_delim || SQLERRM, NULL, NULL, v_dyntask);
      RAISE;
END;


-------------------------------------------------------------------------------------------------------------------
  ----------------------------------------------------------------
  --
  --   window_date_partition
  --
  --
  --   Created : 25/12/2004 Cem Zara
  --
  --   Bu prosedur standart isimlere sahip range ya da range+hash(comp)
  --   tablolarda (range gun, ay ya da yil olan) en son max_date gunune ait
  --   partition olmak kaydi ile son pin_window_size partition kalacak sekilde
  --   duzenleme yapar ( partition ekler ya da cikarir)
  --
  --   Mevcut en buyuk tarihli partitiondan pid_max_date e kadar partition eklenir
  --   pin_window_size in disinda kalan eski tarihli partitionlar cikarilir
  --
  --   Partition name formati    : PYYYY, PYYYYMM, PYYYYMMDD
  --   Subpartition name formati : S<partition_name>_NNN (NNN 3 rakamli sayi)
  --   Parametreler
  --
  --  piv_owner                  :  Tablo sahibi
  --  piv_tablename              :  Tablo Adi
  --  pid_max_date               :  Sonucta Range partition'li tabloda en buyuk date
  --  pin_window_size            :  Tablo en fazla kac parititon li olacak
  --                                li partition ne olacak
  --
  --  pin_tablespace_type        IN       VARCHAR2 DEFAULT 'NULL' -- NULL mevcut ts kullanilacak, T temporary, P permanent, U:user defined
  --  pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
  --------------------------------------------------------------
  PROCEDURE window_date_partitions_reeng (
     piv_owner                  IN       VARCHAR2
   , piv_tablename              IN       VARCHAR2
   , pid_max_date               IN       DATE
   , pin_window_size            IN       NUMBER
   , pin_tablespace_type        IN       VARCHAR2 DEFAULT NULL -- NULL mevcutu kullan, T temporary, P permanent
   , pin_default_tablespace     IN       VARCHAR2 DEFAULT NULL
   , pin_spread                 IN       BOOLEAN  DEFAULT TRUE
   , pin_number_based           IN       BOOLEAN  DEFAULT FALSE
   , pin_tsnum_start            IN       NUMBER DEFAULT 1
   , pin_tsnum_end              IN       NUMBER DEFAULT 16
   , pin_part_prefix_replace    IN       VARCHAR2 DEFAULT 'P'
   , pin_dual_index                IN         BOOLEAN  DEFAULT FALSE
  )
  IS
     v_date_less_eq   DATE;
     d_maxtar         DATE;
     d_mintar         DATE;
     pmax             VARCHAR2(80);
     pmin             VARCHAR2(80);
     pmax_part_num    NUMBER;
     pmin_part_num    NUMBER;
     l_hv             LONG;
     v_window_size    NUMBER;
     PART_NAME_INVALID EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(PART_NAME_INVALID, -20101);

     TABLE_NOT_DATE_PARTITIONED EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(TABLE_NOT_DATE_PARTITIONED, -20102);

     NON_LOGICAL_OPERATION EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(NON_LOGICAL_OPERATION, -20103);

     CAN_NOT_DROP_MORE_THAN_LIMIT EXCEPTION  ;
     PRAGMA EXCEPTION_INIT(NON_LOGICAL_OPERATION, -20104);

  BEGIN

        gv_proc := c_pck || '.window_date_partitions';

       EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_LANGUAGE = AMERICAN';

        -- mantiksal kontroller yapalim
        SELECT  MAX(partition_position), MIN(partition_position)
          INTO  pmax_part_num, pmin_part_num
          FROM ALL_TAB_PARTITIONS
         WHERE table_owner = piv_owner
           AND table_name = piv_tablename ;

        SELECT partition_name,high_value
          INTO pmax,l_hv
          FROM ALL_TAB_PARTITIONS
         WHERE table_name = piv_tablename
           AND table_owner = piv_owner
           AND partition_position = pmax_part_num ;

        SELECT partition_name
          INTO pmin
          FROM ALL_TAB_PARTITIONS
         WHERE table_name = piv_tablename
           AND table_owner = piv_owner
           AND partition_position = pmin_part_num ;


                        pmax := REPLACE(pmax , pin_part_prefix_replace, 'P');
                        -- tablo date range partitionli mi?
                        IF pin_number_based = FALSE AND SUBSTR(l_hv,1,7) <> 'TO_DATE' THEN
                            RAISE TABLE_NOT_DATE_PARTITIONED;
                        END IF;
                        -- partition isimleri standarda uyuyor mu
                        --IF  NOT ( SUBSTR(pmax,1,5)  >= 'P1990' AND SUBSTR(pmax,1,5)  <= 'P2990' )  THEN
                        --    RAISE PART_NAME_INVALID;
                        --END IF;

                        pmax := REPLACE(pmax , 'P', pin_part_prefix_replace);

        IF pin_dual_index = FALSE THEN


                       IF LENGTH(pmax) = 9 OR LENGTH(pmax) = 10 THEN  -- PYYYYMMDD gunluk
                           IF LENGTH(pmax) = 9 THEN
                             d_maxtar := TO_DATE( SUBSTR(pmax,2),'yyyymmdd');
                             d_mintar := TO_DATE( SUBSTR(pmin,2),'yyyymmdd');
                           ELSE
                             d_maxtar := TO_DATE( SUBSTR(pmax,2),'YYYYMONDD');
                             d_mintar := TO_DATE( SUBSTR(pmin,2),'YYYYMONDD');
                           END IF;
                           -- eklediginden window size ciktiginda mevcut maximum date den buyuk olmamali
                           IF ( pid_max_date - pin_window_size) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 7 den fazla gunluk partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size + 6) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                            v_window_size := pin_window_size;
                       ELSIF LENGTH(pmax) = 7 OR LENGTH(pmax) = 8 THEN  -- PYYYYMM aylik
                           IF LENGTH(pmax) = 7 THEN
                             d_maxtar := TO_DATE( SUBSTR(pmax,2,6) || '01' ,'yyyymmdd') ;
                             d_mintar := TO_DATE( SUBSTR(pmin,2,6) || '01' ,'yyyymmdd') ;
                           ELSE
                             d_maxtar := TO_DATE( SUBSTR(pmax,2,7) || '01' ,'YYYYMONDD') ;
                             d_mintar := TO_DATE( SUBSTR(pmin,2,7) || '01' ,'YYYYMONDD') ;
                           END IF;
                           IF  ADD_MONTHS(pid_max_date, (-1) * pin_window_size) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 3 den fazla aylik partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size *30 + 90) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                           v_window_size := pid_max_date - ADD_MONTHS(pid_max_date, (-1) * pin_window_size);
                           --v_window_size := pin_window_size *30;
                       ELSIF LENGTH(pmax) = 5 THEN  -- PYYYY yillik
                           d_maxtar := TO_DATE( SUBSTR(pmax,2,4) || '0101','yyyymmdd');
                           d_mintar := TO_DATE( SUBSTR(pmin,2,4) || '0101','yyyymmdd');
                           IF  ADD_MONTHS(pid_max_date, (-1) * pin_window_size*12) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 3 den fazla yillik partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size *30*12 + 360*3) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                           v_window_size := pid_max_date - ADD_MONTHS(pid_max_date, (-1) * pin_window_size*12);
                           --v_window_size := pin_window_size *30*12;
                         END IF;

                     -- BURDA OPERASYON BASLAR ...
                     add_date_partition_reeng(piv_owner, piv_tablename, pid_max_date, pin_tablespace_type,pin_default_tablespace, pin_spread,  pin_number_based, pin_tsnum_start, pin_tsnum_end ,pin_part_prefix_replace,pin_dual_index );
                     v_date_less_eq := pid_max_date - v_window_size;
                     drop_date_partition_reeng(piv_owner, piv_tablename, v_date_less_eq );

            ELSE


                       IF LENGTH(SUBSTR(pmax, 1, INSTR(pmax,'_') -1 )) = 9 OR LENGTH(SUBSTR(pmax, 1, INSTR(pmax,'_') -1 )) = 10 THEN  -- PYYYYMMDD_X gunluk
                           IF LENGTH(SUBSTR(pmax, 1, INSTR(pmax,'_') -1 )) = 9 THEN
                             d_maxtar := TO_DATE( SUBSTR(pmax,2,8),'yyyymmdd');
                             d_mintar := TO_DATE( SUBSTR(pmin,2,8),'yyyymmdd');
                           ELSE
                             d_maxtar := TO_DATE( SUBSTR(pmax,2,9),'YYYYMONDD');
                             d_mintar := TO_DATE( SUBSTR(pmin,2,9),'YYYYMONDD');
                           END IF;
                           -- eklediginden window size ciktiginda mevcut maximum date den buyuk olmamali
                           IF ( pid_max_date - pin_window_size) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 7 den fazla gunluk partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size + 6) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                            v_window_size := pin_window_size;
                       ELSIF LENGTH(SUBSTR(pmax, 1, INSTR(pmax,'_') -1 )) = 7 OR LENGTH(SUBSTR(pmax, 1, INSTR(pmax,'_') -1 )) = 8 THEN  -- PYYYYMM_X aylik
                           IF LENGTH(SUBSTR(pmax, 1, INSTR(pmax,'_') -1 )) = 7 THEN
                             d_maxtar := TO_DATE( SUBSTR(pmax,2,6) || '01' ,'yyyymmdd') ;
                             d_mintar := TO_DATE( SUBSTR(pmin,2,6) || '01' ,'yyyymmdd') ;
                           ELSE
                             d_maxtar := TO_DATE( SUBSTR(pmax,2,7) || '01' ,'YYYYMONDD') ;
                             d_mintar := TO_DATE( SUBSTR(pmin,2,7) || '01' ,'YYYYMONDD') ;
                           END IF;
                           IF  ADD_MONTHS(pid_max_date, (-1) * pin_window_size) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 3 den fazla aylik partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size *30 + 90) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                           v_window_size := pid_max_date - ADD_MONTHS(pid_max_date, (-1) * pin_window_size);
                           --v_window_size := pin_window_size *30;
                       ELSIF LENGTH(SUBSTR(pmax, 1, INSTR(pmax,'_') -1 )) = 5 THEN  -- PYYYY_X yillik
                           d_maxtar := TO_DATE( SUBSTR(pmax,2,4) || '0101','yyyymmdd');
                           d_mintar := TO_DATE( SUBSTR(pmin,2,4) || '0101','yyyymmdd');
                           IF  ADD_MONTHS(pid_max_date, (-1) * pin_window_size*12) > d_maxtar THEN
                               RAISE NON_LOGICAL_OPERATION;
                           END IF;
                           -- 3 den fazla yillik partition drop etmeyelim
                           IF (pid_max_date - d_mintar ) >  (pin_window_size *30*12 + 360*3) THEN
                               RAISE CAN_NOT_DROP_MORE_THAN_LIMIT;
                           END IF;
                           v_window_size := pid_max_date - ADD_MONTHS(pid_max_date, (-1) * pin_window_size*12);
                           --v_window_size := pin_window_size *30*12;
                         END IF;

                     -- BURDA OPERASYON BASLAR ...
                     add_date_partition_reeng(piv_owner, piv_tablename, pid_max_date, pin_tablespace_type,pin_default_tablespace, pin_spread,  pin_number_based, pin_tsnum_start, pin_tsnum_end ,pin_part_prefix_replace,pin_dual_index );
                     v_date_less_eq := pid_max_date - v_window_size;
                     drop_date_partition_reeng(piv_owner, piv_tablename, v_date_less_eq ,pin_part_prefix_replace,pin_dual_index);

            END IF ;

  EXCEPTION

       WHEN PART_NAME_INVALID THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Partition adlari PYYYYMMDD formatinda olmali.');
       RAISE_APPLICATION_ERROR(-20101, 'PART_NAME_INVALID : Partition adlari PYYYYMMDD formatinda olmali.');

       WHEN TABLE_NOT_DATE_PARTITIONED THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Tablo Tarih partition''li degil.');
       RAISE_APPLICATION_ERROR(-20102, 'TABLE_NOT_DATE_PARTITIONED : Tablo Tarih partition''li degil.');

       WHEN NON_LOGICAL_OPERATION THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Verilen en buyuk tarihten window size cikinca, mevcut tablonun en buyuk tarihli partition''indan buyuk olamaz (Tablonun mevcut tum verisi yok olacak).');
       RAISE_APPLICATION_ERROR(-20103, 'NON_LOGICAL_OPERATION : Verilen en buyuk tarihten window size cikinca, mevcut tablonun en buyuk tarihli partition''indan buyuk olamaz (Tablonun mevcut verisi yok olacak).');

       WHEN CAN_NOT_DROP_MORE_THAN_LIMIT THEN
       o_log.LOG(1,  1, gv_proc || c_delim || 'Verilen window'' a gore 7 den fazla gunluk veya 3 ten fazla aylik veya 3 den fazla yillik partition drop edilmek isteniyor. Islem iptal edildi. Islemi kontrol ediniz.');
       RAISE_APPLICATION_ERROR(-20104, 'CAN_NOT_DROP_PART_MORE_THAN_LIMIT : Verilen window'' a gore 7 den fazla gunluk veya 3 ten fazla aylik veya 3 den fazla yillik partition drop edilmek isteniyor. Islem iptal edildi. Islemi kontrol ediniz.');

  END;






--------------------------------------------------------------------------
--
--   PLIB MAIN
--
--------------------------------------------------------------------------
BEGIN
--    c_bscs        :=  get_system_id('ANT','BSCS');
--    c_prep        :=  get_system_id('ANT','PREPAID');
--    c_collection  :=  get_system_id('ANT','COLLECTION');
   NULL;

END ;
/

