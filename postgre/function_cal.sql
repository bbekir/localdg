SET search_path = "public";


--=fonksiyon ile kayıt dönmmenin birkaç yolu

--1. SQL diliyle fonksiyon + out parameter

CREATE FUNCTION function_a(IN numeric, IN int, OUT noorder int) AS
/*
SELECT * FROM function_a( 2019, 2394 );
out:
noorder
-----
 3
*/
$$
SELECT COUNT(*)
FROM orders
WHERE EXTRACT(YEAR FROM "status_date") = $1
  AND "code" = $2
$$ LANGUAGE sql;


----=============================================================================---
--2.  plpgsql lang ile  return TABLE
CREATE OR REPLACE FUNCTION summary_orders()
    RETURNS table
            (
                retyear    numeric,
                order_desc text,
                percentage numeric
            )
AS
/* SELECT * FROM summary_orders( )
out: tablo formatı ayen  */
$$
DECLARE
    retyear numeric DEFAULT 0;
    descrip text DEFAULT 0;
    percent numeric DEFAULT 0;
BEGIN
    RETURN QUERY SELECT CAST(EXTRACT(YEAR FROM status_date) AS numeric)                       AS teayear
                      , MIN(descript)                                                         AS descrip
                      , ROUND(MAX(function_a(CAST(EXTRACT(YEAR FROM status_date) AS numeric), code))::decimal /
                              function_b(CAST(EXTRACT(YEAR FROM status_date) AS numeric)), 2) AS percent
                 FROM orders o
                 GROUP BY teayear
                 ORDER BY teayear ASC;
END;
$$ LANGUAGE plpgsql;

----================================================================================----

--3.-- return record. select into ile recordu doldurma. .select i çok bariz gelmeyebilir ama record olunca haliyle tipleri bir aşamada belirli yapmak lazım

CREATE OR REPLACE FUNCTION public.fnrecord(a text, b text) RETURNS record AS
/*
SELECT public.fnrecord('asd','dss')
out: (5,aa)
SELECT b ,c FROM public.fnrecord('foo','barbaz') AS (b integer, c text)
out: tablo gibi
*/
$$
DECLARE
    ret record;
BEGIN

    SELECT 5 AS col1, 'aa' AS col2 INTO ret;
    RETURN ret;
END;
$$ LANGUAGE plpgsql;
;



--------------------------------------------------------------------------
--4- return setof type . tablo varsa type yaratmaya gerek yok
CREATE OR REPLACE FUNCTION fn_set_of_table() RETURNS setof mtd_model
    LANGUAGE sql AS
    /* SELECT * from fn_set_of_table()
    out :tablo aynen
    */
$$
SELECT *
FROM mtd_model;
$$;
----------------------------------------------------------------------------
--5. 4. de tablo yoksa type yaratırız
CREATE TYPE biseyler AS
(
    col1 int4,
    col2 varchar(100),
);
-- burada setof biseyler veya setof tablo ismi. return ise rowtype ile tablo veya tanımlanan type olur.
-- her satırı teker teker loopla basıyor galiba.
CREATE OR REPLACE FUNCTION public.testsil() RETURNS setof odimtd.snp_lschema AS
/*Select * FROM public.testsil()
out : tablo aynen
*/
$BODY$
DECLARE
    res snp_lschema%rowtype;
BEGIN
    FOR res IN SELECT * FROM snp_lschema
        LOOP
            RETURN NEXT res;
        END LOOP;
END ;
$BODY$ LANGUAGE 'plpgsql'
;

SELECT *
FROM public.testsil();

SELECT *
FROM function_a(2019, 2394);

SELECT *
FROM function_b(2019);

SELECT *
FROM summary_orders();