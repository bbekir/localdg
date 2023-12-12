CREATE OR REPLACE PROCEDURE repocp.my_procedure( input integer)
AUTHID DEFINER
    IS
    cs varchar2(50);
BEGIN
select sys_context( 'userenv', 'current_schema' ) into cs from dual;
    dbms_output.put_line('Start test');
        dbms_output.put_line('gggg' || cs);

    dbms_output.put_line(user);
INSERT INTO test(c1) VALUES (input);
   dbms_output.put_line('End test');
END my_procedure;


CREATE OR REPLACE PROCEDURE my_proc( input integer)
AUTHID DEFINER
    IS

cs varchar2(50);
BEGIN

cs :='aaa';
select sys_context( 'userenv', 'current_schema' ) into cs from dual;

    dbms_output.put_line('Start test');
    dbms_output.put_line('sdfsdf' || cs);
    dbms_output.put_line(user);
-- INSERT INTO repocp.test(c1) VALUES (input);
   dbms_output.put_line('End test');
END my_proc;


begin
  repocp.MY_PROCEDURE(4);
  COMMIT ;
 end;




 begin
  system.MY_PROC(4);
  COMMIT ;
 end;

<

CREATE OR REPLACE FUNCTION fromexceldatetime(
                 acellvalue IN varchar2
                 )
             RETURN timestamp IS
    excel_base_date_time CONSTANT timestamp := to_timestamp( '12/31/1899', 'mm/dd/yyyy' );
    val CONSTANT                  number := to_number( nullif( trim( acellvalue ), '0' ) );
BEGIN

    RETURN excel_base_date_time + numtodsinterval( val - CASE WHEN val >= 60 THEN 1 ELSE 0 END, 'DAY' );
END;


CREATE OR REPLACE FUNCTION toexceldatetime(
             atimestamp IN timestamp
             ) RETURN varchar2 IS
    excel_base_date_time CONSTANT timestamp := to_timestamp( '12/31/1899', 'mm/dd/yyyy' );
    dif CONSTANT                  interval day(9) to second(9) := atimestamp - excel_base_date_time;
    days CONSTANT                 integer := extract( DAY FROM dif );
BEGIN
    RETURN CASE
               WHEN dif IS NULL THEN ''
               ELSE to_char( days + CASE WHEN days >= 60 THEN 1 ELSE 0 END + round( (extract( HOUR FROM dif ) +
                                                                                     (extract( MINUTE FROM dif ) + extract( SECOND FROM dif ) / 60) /
                                                                                     60) / 24, 4 ) )
           END;
END;

SELECT to_char AS fromexceldatetime('40999,7261') FROM dual


--
-- with temp AS (
-- select 1 as a, TO_DATE ('20140225', 'YYYYMMDD') as t from dual
-- union ALL
-- select 3 as a, TO_DATE ('-5000', 'YYYY') as t from dual
--  )
--  select * from temp;
--
-- SELECT   SYSDATE - TO_DATE ('-', 'YYYYMMDD')
--   FROM   DUAL;
--
-- select TO_DATE ('sf', 'YYYY') as t from dual;


-- select TO_DATE ('100004714', 'YYYY') as t from dual;


SELECT to_char(null) from dual

--p.table_name = 'SAMPLE_TAB_TEST'



