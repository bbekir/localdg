

WITH
    RECURSIVE
    colhy( dep_group_id
        , lvl
        , recs_uid
        , par_uid
        , uid
        , cti_uid
        , qname
        , cti_qname
        , recs_uid_path
        , uid_path
        , cti_uid_path
        , recs_qname_path
        , qname_path
        , cti_qname_path ) AS (
        SELECT DISTINCT b.dep_group_id
                      , 0                         AS lvl
                      , NULL::odimtd.euid         AS rec_src_uid
                      , NULL::odimtd.euid         AS par_uid
                      , b.ref_uid                 AS uid
                      , b.ref_cti_uid             AS cti_uid
                      , b.ref_qname               AS qname
                      , b.ref_cti_qname           AS cti_qname
                      , NULL::odimtd.euid[]       AS recs_uid_path
                      , ARRAY [b.ref_uid]         AS uid_path
                      , ARRAY [b.ref_cti_uid]     AS cti_uid_path
                      , NULL::character varying[] AS recs_qname_path
                      , ARRAY [b.ref_qname]       AS qname_path
                      , ARRAY [b.ref_cti_qname]   AS cti_qname_path
        FROM odimtd.mtd2_object_dep_base b
        WHERE 1 = 1
          AND NOT (EXISTS ( --bu koşul doldurulan tabloları devre dışı bırakır. en baştaki kaynakları verir böylece.. kaldırdım
                              SELECT 1
                              FROM odimtd.mtd2_object_dep_base t
                              WHERE t.dep_group_id = b.dep_group_id
                                AND t.ecid = b.ref_ecid
                                AND t.eid = b.ref_eid))
        UNION ALL
        SELECT b.dep_group_id
             , h_1.lvl + 1                                        AS lvl
             , b.rec_src_uid
             , b.ref_uid                                          AS par_uid
             , b.uid
             , b.cti_uid
             , b.qname
             , b.cti_qname
             , ARRAY_APPEND(h_1.recs_uid_path, b.rec_src_uid)     AS recs_uid_path
             , ARRAY_APPEND(h_1.uid_path, b.uid)                  AS uid_path
             , ARRAY_APPEND(h_1.cti_uid_path, b.cti_uid)          AS cti_uid_path
             , ARRAY_APPEND(h_1.recs_qname_path, b.rec_src_qname) AS recs_qname_path
             , ARRAY_APPEND(h_1.qname_path, b.qname)              AS qname_path
             , ARRAY_APPEND(h_1.cti_qname_path, b.cti_qname)      AS cti_qname_path
        FROM odimtd.mtd2_object_dep_base b
                 JOIN colhy h_1 ON h_1.uid = b.ref_uid AND h_1.dep_group_id = b.dep_group_id)
SELECT h.dep_group_id
     , h.lvl
     , h.uid_path[1]   AS first_uid
     , h.qname_path[1] AS first_qname
     , h.cti_qname_path[1] AS firs_cti_qname
     , h.recs_uid_path[1] AS root_recs_uid
     , h.recs_qname_path[1] AS root_recs_qname
     , h.recs_uid  --last_recs_uind ile aynı
     , h.recs_uid_path[lvl+1] AS last_recs_uid
     , h.par_uid --last -1
     , h.uid --last_uid
     , h.qname
     , h.cti_uid
     , h.cti_qname
     , h.recs_uid_path
     , h.recs_qname_path
     , h.uid_path
     , entity_col_from_id_array(h.uid_path,'pqname')
     , h.qname_path
     , h.cti_uid_path
     , h.cti_qname_path
FROM colhy h
WHERE h.qname_path[1] = 'ADVENTURE.EmailAddress'