SET SEARCH_PATH = public;
create table public.hy2
(
    id      integer,
    parid   integer,
    name    varchar,
    src     varchar,
    parname varchar
);

INSERT INTO public.hy2 (id, parid, name, src, parname) VALUES (2, 3, 'b', 'm1', 'c');
INSERT INTO public.hy2 (id, parid, name, src, parname) VALUES (2, 5, 'b', 'm1', 'e');
INSERT INTO public.hy2 (id, parid, name, src, parname) VALUES (3, 4, 'c', 'm2', 'd');
INSERT INTO public.hy2 (id, parid, name, src, parname) VALUES (4, 1, 'd', 'm3', 'a');


WITH
    RECURSIVE
    src   AS (
              SELECT 'AA'
                   , 1
                   , h1.parid
                   , h1.id
                   , h1.name
                   , h1.src
                   , CASE
                         WHEN NOT EXISTS (
                                             SELECT 1
                                             FROM hy hi
                                             WHERE hi.parid = h1.id)
                             THEN TRUE
                         ELSE FALSE END AS is_leaf
                   , ARRAY [h1.id]::int[]
                   , ARRAY [h1.name]::varchar[]
                   , ARRAY [h1.src]::varchar[]
              FROM hy2 h1
              WHERE 1 = 1
              UNION ALL
              SELECT DISTINCT 'AA'
                            , 1
                            , NULL::integer
                            , h2.parid
                            , h2.parname
                            , h2.src
                            , FALSE is_leaf
                            , ARRAY [parid]::int[]
                            , ARRAY [parname]::varchar[]
                            , NULL::varchar[]
              FROM hy2 h2
              WHERE 1 = 1
                AND NOT EXISTS(
                                  SELECT 1
                                  FROM hy2 hi
                                  WHERE hi.id = h2.parid))
  , rh( c, lvl, parid, id, name, src, is_leaf, idpath, namepath, srcpath ) AS (
          SELECT *
          FROM src
          UNION ALL
          SELECT 'AA'
               , lvl + 1
               , h.parid
               , h.id
               , h.name
               , h.src
               , CASE
                     WHEN NOT EXISTS (
                                         SELECT 1
                                         FROM hy hi
                                         WHERE hi.parid = h.id)
                         THEN TRUE
                     ELSE FALSE END AS is_leaf
               , ARRAY_APPEND(r.idpath, h.id)
               , ARRAY_APPEND(r.namepath, h.name)
               , ARRAY_APPEND(r.srcpath, h.src)
          FROM hy2 h
                   JOIN rh r ON h.parid = r.id)
SELECT *
FROM rh r
WHERE 1 = 1
--     and       r.id=2
-- and idpath[lvl]=
  AND idpath[1] = 1


--* idpath 1 için idpath(lvl)=trg, fazladan kendisi de gelir ( ağaç görünütüsü oluşur). tek bir kayıt leaf olur.
--   lvl(1) kendisi olur!!
--*  mutlak hedef ise mapp sayısı kadar kendi çıkar. (lvl=1)
--trg
--*idpath(lvl).  mutlak src için tek kayıt dönebilir sadece. o da ref =null olandan gelir . kendisidir
-- idpath(1) ve lvl>1 kaynakları verir. lvl(1) kendisidir (kaynaktan hedefe şşeklinde ağaç verir gene

SELECT DISTINCT h2.parid AS pid, h2.parname, h2.src
FROM hy2 h2
WHERE 1 = 1
  AND NOT EXISTS(
                    SELECT 1
                    FROM hy2 hi
                    WHERE hi.id = h2.parid)