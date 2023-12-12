SET SEARCH_PATH = public
/*en tepedeki kayıtları p-c ilişkisi olmas da ref i boş olarak eklersek.. . Bu durumda parname in tabloda olması elzem.*/
create table public.hy
(
    id      integer,
    parid   integer,
    name    varchar,
    src     varchar,
    parname varchar
);

INSERT INTO public.hy (id, parid, name, src, parname) VALUES (1, null, 'a', null, null);
INSERT INTO public.hy (id, parid, name, src, parname) VALUES (5, null, 'e', null, null);
INSERT INTO public.hy (id, parid, name, src, parname) VALUES (2, 3, 'b', 'm1', 'c');
INSERT INTO public.hy (id, parid, name, src, parname) VALUES (2, 5, 'b', 'm1', 'e');
INSERT INTO public.hy (id, parid, name, src, parname) VALUES (3, 4, 'c', 'm2', 'd');
INSERT INTO public.hy (id, parid, name, src, parname) VALUES (4, 1, 'd', 'm3', 'a');
INSERT INTO public.hy (id, parid, name, src, parname) VALUES (6, 1, 'f', 'm4', 'a');
INSERT INTO public.hy (id, parid, name, src, parname) VALUES (7, 6, 'g', 'm5', 'f');




WITH
    RECURSIVE
    rh( c, lvl, parid, id, name, src, is_trgleaf,is_srcleaf, sibord,revsibord, idpath, namepath, srcpath ) AS (
          SELECT 'AA'
               , 1
               , h.parid
               , h.id
               , h.name
               , h.src
               , CASE WHEN NOT EXISTS ( SELECT 1 FROM hy hi  WHERE hi.parid = h.id)
                         THEN TRUE
                    ELSE FALSE END AS is_trgleaf
              , CASE WHEN NOT EXISTS ( SELECT 1 FROM hy hi  WHERE hi.id = h.parid)
                         THEN TRUE
                    ELSE FALSE END AS is_srcleaf
               ,ROW_NUMBER() OVER (PARTITION BY parid ORDER BY id)::varchar AS sibord
               ,ROW_NUMBER() OVER (PARTITION BY id ORDER BY parid)::varchar AS revsibord
               , ARRAY [id]::int[]
               , ARRAY [name]::varchar[]
               , ARRAY [src]::varchar[]
          FROM hy h
          WHERE 1 = 1
          UNION ALL
          SELECT 'AA'
               , r.lvl + 1
               , h.parid
               , h.id
               , h.name
               , h.src
               , CASE WHEN NOT EXISTS ( SELECT 1 FROM hy hi  WHERE hi.parid = h.id)
                         THEN TRUE
                    ELSE FALSE END AS is_trgleaf
              , r.is_srcleaf
               , sibord ||'.'|| ROW_NUMBER() OVER (PARTITION BY h.parid ORDER BY h.id) A
               , ROW_NUMBER() OVER (PARTITION BY h.id ORDER BY h.parid)::varchar||'.'||  revsibord as      revsibord
               , ARRAY_APPEND(r.idpath, h.id)
               , ARRAY_APPEND(r.namepath, h.name)
               , ARRAY_APPEND(r.srcpath, h.src)
          FROM hy h
                   JOIN rh r ON h.parid = r.id)
SELECT *
FROM rh r
WHERE 1 = 1
--     and       r.id=1
--  and idpath[lvl]=2
and idpath[1]=1
ORDER BY sibord

--* idpath 1 için idpath(lvl)=trg, fazladan kendisi de gelir ( ağaç görünütüsü oluşur). tek bir kayıt leaf olur.
--   lvl(1) kendisi olur!!
--*  mutlak hedef ise mapp sayısı kadar kendi çıkar. (lvl=1)
--trg
--*idpath(lvl).  mutlak src için tek kayıt dönebilir sadece. o da ref =null olandan gelir . kendisidir
-- idpath(1) ve lvl>1 kaynakları verir. lvl(1) kendisidir (kaynaktan hedefe şşeklinde ağaç verir gene

--isleaf mutlak trg ile biten kayıtları verir. yani aslında src kaydı olmayan kayıtları ayıklayarak isleaf i bulabiliriz
--mutlaksrc ile başlayan kayıtlar da bunun alt kümesidir aslında ??değildir amam böyle daha iyi
--src zincirinde leaf olan kayıt tam zincir olur. diğerleri ara zincir


--trg zinciri için bunun simtetriği lazım  == mutlak src ile başlayan kayıtlar yani ( burada 2 için 1 ve 5 )