----- scen text i parse deneyelim

WITH a AS (SELECT s.scen_no                                                                        AS parent_scen_no
                , c.scen_name                                                                      AS parent_scen_name
                , CASE
                      WHEN instr( s.def_txt, '"', 1, 1 ) > 0
                          THEN to_char( substr( s.def_txt, 26, instr( s.def_txt, 'SCEN_VERSION', 1, 1 ) - 30 ) )
                      ELSE to_char( substr( s.def_txt, 25, instr( s.def_txt, 'SCEN_VERSION', 1, 1 ) - 27 ) )
                  END                                                                              AS sub_scen_name
                , to_char( substr( s.def_txt, instr( s.def_txt, 'SCEN_VERSION', 1, 1 ) + 13, 3 ) ) AS sub_scen_version
           FROM snp_scen_task s, snp_scen c
           WHERE 1 = 1
             AND s.scen_no = c.scen_no
             AND s.def_txt LIKE 'OdiStartScen%')
   , pc_tb AS (SELECT a.parent_scen_no
                    , a.parent_scen_name
                    , c.scen_no       AS scen_no
                    , a.sub_scen_name AS scen_name
               FROM a, snp_scen c
               WHERE a.sub_scen_name = c.scen_name(+)
                 AND a.sub_scen_version = c.scen_version(+)
               UNION ALL
               SELECT cast( NULL AS number )        AS parent_scen_no
                    , cast( NULL AS varchar2(100) ) AS parent_scen_name
                    , c.scen_no
                    , c.scen_name
               FROM snp_scen c
               WHERE 1 = 1 --          AND NOT EXISTS( SELECT 1 FROM pc_hy b WHERE b.scen_no=c.scen_no)
)
SELECT rownum                                      AS rn
     , CONNECT_BY_ROOT (p.scen_no)                 AS parent_root_cd
     , CONNECT_BY_ROOT (p.scen_name)               AS parent_root
     , lpad( ' ', 5 * (level - 1) ) || p.scen_name AS hy_scen
     , level                                       AS lvl
     , p.scen_no   --tüm senaryoların no ları
     , p.scen_name --tüm senaryoların isimleri
     , p.parent_scen_no
     , p.parent_scen_name
     , sys_connect_by_path( p.scen_name, '/' )     AS path
FROM pc_tb p
START WITH parent_scen_no IS NULL
CONNECT BY PRIOR scen_no = parent_scen_no
ORDER SIBLINGS BY scen_name;


