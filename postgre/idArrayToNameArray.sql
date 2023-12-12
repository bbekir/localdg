SET SEARCH_PATH = odimtd;

/*
odimtd üzerinde
Amaç:  id arrayî şeklinde tutulan bilgiyi aynı sırada name vs alanına dönüştürme.
Bunun için nümerik dizi( burada integer[] ) türündeki alanı unnest ile açıp taşka bir tabloyla joinleyip ,
aynı sırada olmak koşuluyla  diğer tablodaki başka bilgieri tekrar array şeklinde vermek gerekiyor

Dizi, integer[] şeklinde olabilir veya metin alandan  string_to_array ile üretilebilir veya kullanıcı tanımlı bir tipin dizisi de olabilir
Burada kullanıcı tanımlı ( euid --> ecit int,eid int) dizinin arrayı ile ve tek boyutlu array ile örnek bulunuyor
2 boyutlu arrayda performans oldukça zayıf

*/

--veriyi görelim
SELECT a.id_val, a.idx, a.ordinality, l.lvl, l.euq_path, l.root_qname, l.root_qcd
FROM mtd_col_lineage2 l
   , UNNEST(euq_path) WITH ORDINALITY AS a( id_val, idx )
WHERE 1 = 1
--   AND (l.root_qcd).eid = 313042
--   AND l.root_qcd = ROW (98,306844)::euid
  AND lvl = 2;

/*1. tek boyutlu array. 35 sec*/
SELECT c.mapp_name
     , c.obj_unq_path
     , c.obj_name_path
     , (SELECT ARRAY_AGG(e.qname ORDER BY idx)
        FROM UNNEST(c.obj_unq_path) WITH ORDINALITY AS a( obj, idx )
                 LEFT JOIN mtds_entity e ON e.fqcd = a.obj) AS namepath
FROM mtd_col_lineage_t c
WHERE 1 = 1;

/*üsttekinde array_agg  kullanmazsak.. 33 sn*/
SELECT c.mapp_name
     , c.obj_unq_path
     , c.obj_name_path
     , ARRAY(SELECT e.qname
             FROM UNNEST(c.obj_unq_path) WITH ORDINALITY AS a( obj, idx )
                      LEFT JOIN mtds_entity e ON e.fqcd = a.obj
             ORDER BY idx) AS namepath
FROM mtd_col_lineage_t c
WHERE 1 = 1

/*2d array ( type). 49 sn */

SELECT c.root_qname
     , c.root_qcd
     , c.mappid_path
     , c.qname_path
     , (SELECT ARRAY_AGG(e.name ORDER BY idx)
        FROM UNNEST(c.euq_path) WITH ORDINALITY AS a( cecid, ceid, idx )
                 LEFT JOIN odimtd.mtds_entity2 e ON (e.euq).ecid = a.cecid AND (e.euq).eid = a.ceid) AS namepath
FROM mtd_col_lineage2 c
WHERE 1 = 1
-- and lvl=3
-- and obj_unq_path='98.106>98.46>98.160>98.121'
-- and mapp_id_path='288>287>296'

/*2d- üsttekine denk.49 sn*/

SELECT c.root_qname
     , c.root_qcd
     , c.mappid_path
     , c.qname_path
     , ARRAY(SELECT e.name
             FROM UNNEST(c.euq_path) WITH ORDINALITY AS a( cecid, ceid, idx )
                      LEFT JOIN odimtd.mtds_entity2 e ON (e.euq).ecid = a.cecid AND (e.euq).eid = a.ceid
             ORDER BY idx) AS namepath
FROM mtd_col_lineage2 c
WHERE 1 = 1

--> Burada entity tablosunda euid türünde kolona gerke yok. fn yazmak için yukarıdaki sorgunun hemen hemen aynısı, şunu seçtim
SELECT *
FROM mtds_entity e
         JOIN UNNEST(ARRAY [ ROW (98,106)::euid, ROW (98,46)::euid, ROW (98,160)::euid, ROW (98,121)::euid ]::euid[]) WITH ORDINALITY AS r( ecid, eid, ordinality )
              ON e.eid = r.eid AND e.ecid = r.ecid;

/*2d-- unnest i sub dışına. ana sorguya. 39 sn ye düştü*/

SELECT d.euq
     , d.root_qname
     , r.e1
     , r.e2
     , idx
     , ARRAY(SELECT e.name FROM mtds_entity2 e WHERE (e.euq).eid = r.e1 AND (e.euq).ecid = r.e2) AS name
FROM mtd_col_lineage2 d
   , LATERAL UNNEST(d.euq_path) WITH ORDINALITY AS r( e1, e2, idx );

/*2d- unnest gene sonda ama  select ile tablo gibi kullanalım. güzel olabilir
:( 1dk 7 sn */

SELECT d.euq, d.root_qname, p.k, ARRAY(SELECT e.name FROM mtds_entity2 e WHERE e.euq = p.k) AS name
FROM mtd_col_lineage2 d
   , LATERAL (SELECT UNNEST(d.euq_path) AS k ) p;



