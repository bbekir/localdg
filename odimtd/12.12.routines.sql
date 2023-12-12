create or replace procedure odimtd.load_base()
    language plpgsql
as
$$
DECLARE
BEGIN
    SET TIMEZONE = 'Europe/Istanbul';
    DELETE FROM odimtd.mtd_loc_repw;
    INSERT INTO mtd_loc_repw (
                                   id
                                 , rep_timestamp
                                 , rep_name
                                 , rep_type
                                 , rep_version
                                 , min_exe_version
                                 , version_timestamp
                                 , global_id )
    SELECT rep_short_id AS id
         , rep_timestamp
         , rep_name
         , rep_type
         , rep_version
         , min_exe_version
         , version_timestamp
         , global_id
    FROM snp_loc_repw;

    DELETE FROM mtds_entity_class
    WHERE 1=1;
--     and  rec_type = 'S';


    INSERT INTO odimtd.mtds_entity_class (id, par_id, rec_type, name, source_table, mtd_table, class, obj_tab_id, config) VALUES (1501, 1001, 'M', 'ReusableMappingAsTable', 'snp_mapping', null, null, null, null);
    INSERT INTO odimtd.mtds_entity_class (id, par_id, rec_type, name, source_table, mtd_table, class, obj_tab_id, config) VALUES (1502, 1001, 'M', 'MapCompAsTable', 'snp_map_comp', null, null, null, null);
    INSERT INTO odimtd.mtds_entity_class (id, par_id, rec_type, name, source_table, mtd_table, class, obj_tab_id, config, props_template, props2_template) VALUES (1503, null, 'M', '1501 child', null, null, null, null, null, null, null);
    INSERT INTO odimtd.mtds_entity_class (id, par_id, rec_type, name, source_table, mtd_table, class, obj_tab_id, config, props_template, props2_template) VALUES (1504, null, 'M', '1502 child', null, null, null, null, null, null, null);
    INSERT INTO odimtd.mtds_entity_class (id, par_id, rec_type, name, source_table, mtd_table, class, obj_tab_id, config, props_template, props2_template) VALUES (1505, null, 'M', null, null, null, null, null, null, null, null);
    INSERT INTO odimtd.mtds_entity_class (id, par_id, rec_type, name, source_table, mtd_table, class, obj_tab_id, config, props_template, props2_template) VALUES (1506, null, 'M', 'KM', null, null, null, null, null, null, null);
    INSERT INTO odimtd.mtds_entity_class (id, par_id, rec_type, name, source_table, mtd_table, class, obj_tab_id, config, props_template, props2_template) VALUES (1507, null, 'M', 'Proc', null, null, null, null, null, null, null);


    INSERT INTO mtds_entity_class( id, rec_type, name, source_table, mtd_table, class, obj_tab_id )
    SELECT e.i_entity AS id
         , 'S'
         , split_part( e.heading, '.', 1 ) AS name
         , lower( e.name ) AS source_table
         , concat( 'mtd_', lower( substr( e.name, 5 ) ) ) AS mtd_table
         , split_part( e.class_name, '.', 5 ) AS class
         , o.i_objects AS obj_tab_id
    FROM snp_entity e
             LEFT JOIN snp_object o ON e.class_name = o.int_java_name;
END ;
$$;

alter procedure odimtd.load_base() owner to odimtd_admin;

create or replace procedure odimtd.load_dep1()
    language plpgsql
as
$$
DECLARE
BEGIN
    SET TIMEZONE = 'Europe/Istanbul';
    TRUNCATE TABLE odimtd.stg_attr_dep;
    TRUNCATE TABLE odimtd.mid_attr_dep;
    TRUNCATE TABLE odimtd.mid_attr_obj_rel;
    TRUNCATE TABLE odimtd.mid_col_lin_base;
    TRUNCATE TABLE odimtd.mtd_col_lineage_s;
    TRUNCATE TABLE odimtd.mtd_col_lineage_t;
    TRUNCATE TABLE odimtd.mtd2_object_dep_base;

/*--stg_attr_dep----------------------------------------------------------------------------------------------------*/

    INSERT INTO stg_attr_dep (
                                   mapp_id
                                 , comp_id
                                 , comp_name
                                 , comp_type_id
                                 , attr_ecid
                                 , attr_eid
                                 , attr_name
                                 , ref_comp_id
                                 , ref_comp_name
                                 , ref_comp_type_id
                                 , ref_attr_ecid
                                 , ref_attr_eid
                                 , ref_attr_name )
        WITH extdep AS (SELECT adapter_inf, ecid
                        FROM (VALUES ( 'IVariable', 187 ), ( 'ISequence', 156 ), ( 'ITrt', 176 )) AS oft(adapter_inf, ecid))
        SELECT m.i_mapping AS mapp_id
             , mco.i_map_comp AS comp_id
             , mco.name AS comp_name
             , mco.i_map_comp_type AS comp_type_id
             , 229 AS attr_ecid
             , matt.i_map_attr AS attr_eid
             , matt.name AS attr_name
             , mco2.i_map_comp AS ref_comp_id
             , mco2.name AS ref_comp_name
             , CASE
                   WHEN mer.i_ref_map_ref IS NULL THEN mco2.i_map_comp_type
                   ELSE 100
               END AS ref_comp_type_id -- proc-seq-var --gerekli de değil. buraya komponent var mı  flag koyabilirdik ayırmak için
             , CASE
                   WHEN mer.i_ref_map_attr IS NOT NULL THEN 229
                   ELSE (SELECT e.ecid FROM extdep e WHERE e.adapter_inf = mre2.adapter_intf_type)
               END AS ref_attr_ecid
             , coalesce( matt2.i_map_attr, mer.i_ref_map_ref ) AS ref_attr_eid
             , coalesce( matt2.name, mre2.qualified_name ) AS ref_attr_name
        FROM odimtd.snp_mapping m
                 JOIN      odimtd.snp_map_comp mco ON m.i_mapping = mco.i_owner_mapping
                 JOIN      odimtd.snp_map_cp mcp ON mcp.i_owner_map_comp = mco.i_map_comp
                 JOIN      odimtd.snp_map_attr matt ON matt.i_owner_map_cp = mcp.i_map_cp
                 JOIN      odimtd.snp_map_expr mex ON mex.i_owner_map_attr = matt.i_map_attr
                 LEFT JOIN odimtd.snp_map_expr_ref mer ON mer.i_owner_map_expr = mex.i_map_expr
                 LEFT JOIN odimtd.snp_map_attr matt2 ON mer.i_ref_map_attr = matt2.i_map_attr
                 LEFT JOIN odimtd.snp_map_cp mcp2 ON mcp2.i_map_cp = matt2.i_owner_map_cp
                 LEFT JOIN odimtd.snp_map_comp mco2 ON mco2.i_map_comp = mcp2.i_owner_map_comp
                 LEFT JOIN odimtd.snp_map_ref mre2 ON mre2.i_map_ref = mer.i_ref_map_ref
        WHERE 1 = 1;

/*--mid_attr_dep----------------------------------------------------------------------------------------------------*/

    INSERT INTO odimtd.mid_attr_dep (
        mapp_id, attr_ecid, attr_eid, comp_type_id, ref_attr_ecid, ref_attr_eid, ref_comp_type_id )
     WITH RECURSIVE dims( lvl, mapp_id, id, id_src, comp_type_id, par_id, parid_src, par_comp_type_id, id_path
                           , type_id_path, reftype_path) AS
                           (SELECT 0 AS lvl
                                  , b.mapp_id
                                  , b.attr_eid AS id
                                  , b.attr_ecid
                                  , b.comp_type_id
                                  , b.ref_attr_eid AS par_id
                                  , b.ref_attr_ecid AS par_src
                                  , b.ref_comp_type_id AS par_comp_type_id
                                  , ARRAY [b.attr_eid , b.ref_attr_eid]::integer[] id_path
                                  , ARRAY[b.comp_type_id, b.ref_comp_type_id ]::integer[] AS type_id_path
                                  , ARRAY [b.attr_ecid, b.ref_attr_ecid ]::integer[] AS reftype_path
                             FROM odimtd.stg_attr_dep b
                             WHERE 1 = 1
                               AND (b.ref_comp_type_id <> ALL (ARRAY [1, 9, 10]))
                               AND b.ref_comp_type_id IS NOT NULL
                               AND (b.comp_type_id = ANY (ARRAY [1, 9, 10]))
                             UNION ALL
                             SELECT d.lvl + 1 AS lvl
                                  , c.mapp_id
                                  , c.attr_eid
                                  , c.attr_ecid
                                  , c.comp_type_id
                                  , c.ref_attr_eid
                                  , c.ref_attr_ecid
                                  , c.ref_comp_type_id
                                  , array_append(d.id_path ,c.ref_attr_eid)
                                  , array_append(d.type_id_path ,c.ref_comp_type_id)
                                  , array_append (d.reftype_path, c.ref_attr_ecid)
                             FROM odimtd.stg_attr_dep c
                                      JOIN dims d ON d.par_id = c.attr_eid AND d.parid_src = c.attr_ecid
                             WHERE 1 = 1)
        SELECT d.mapp_id
             , d.reftype_path[1] AS attr_ecid
             , d.id_path[1] AS attr_eid
             , d.type_id_path[1] AS comp_type_id
             , d.parid_src AS ref_attr_ecid
             , d.par_id AS ref_attr_eid
             , d.par_comp_type_id AS ref_comp_type_id
        FROM dims d
        WHERE 1 = 1
          AND (d.par_comp_type_id = ANY (ARRAY [1, 9, 100]))
        UNION ALL
        SELECT b.mapp_id
             , b.attr_ecid
             , b.attr_eid
             , b.comp_type_id
             , b.ref_attr_ecid
             , b.ref_attr_eid
             , b.ref_comp_type_id
        FROM odimtd.stg_attr_dep b
        WHERE 1 = 1
          AND (b.comp_type_id = ANY (ARRAY [1, 9]))
          AND (b.ref_comp_type_id = ANY (ARRAY [1, 10, 100]));

/*--mid_attr_obj_rel------------------------------------------------------------------------------------------------*/

    INSERT INTO odimtd.mid_attr_obj_rel(
                                             mapp_id
                                           , attr_ecid
                                           , attr_eid
                                           , attr_comp_type_id
                                           , attr_comp_id
                                           , obj_ecid
                                           , obj_eid
                                           , obj_qname
                                           , cti_ecid
                                           , cti_eid
                                           , cti_qname
                                           )
    SELECT m.i_mapping AS mapp_id
         , 229 AS attr_ecid
         , matt.i_map_attr AS attr_eid
         , mco.i_map_comp_type AS attr_comp_type_id
         , mco.i_map_comp AS attr_comp_id
         , 98 AS obj_ecid
         , cl.id AS obj_eid
         , cl.qname
         , 173 AS cti_ecid
         , cl.table_id AS cti_eid
         , tref.qualified_name AS cti_qname
    FROM odimtd.snp_mapping m
             JOIN odimtd.snp_map_comp mco ON m.i_mapping = mco.i_owner_mapping
             JOIN odimtd.snp_map_cp mcp ON mcp.i_owner_map_comp = mco.i_map_comp
             JOIN odimtd.snp_map_attr matt ON matt.i_owner_map_cp = mcp.i_map_cp
             JOIN odimtd.snp_map_ref cref ON cref.i_map_ref = matt.i_map_ref
             JOIN odimtd.snp_map_ref tref ON tref.i_map_ref = mco.i_map_ref
             JOIN odimtd.mtd_col cl ON cl.name::text = cref.qualified_name::text AND cl.tab_qname=tref.qualified_name
    WHERE 1 = 1
      AND mco.i_map_comp_type = 1
    UNION ALL
 SELECT m.id AS mapp_id  --Reusable mapping cols
         , 229 AS attr_ecid
         , matt.i_map_attr AS attr_eid
         , mco.i_map_comp_type AS attr_comp_type_id
         , mco.i_map_comp AS attr_comp_id
         , 1503 AS obj_ecid
         , matt.i_map_attr AS obj_eid
         , concat(m.name,matt.name) AS obj_qname
         , 1501 AS cti_ecid
         , m.id AS cti_eid
         , m.fqname AS cti_qname
    FROM odimtd.mtd_mapping m -- sadece cti_qname için.o da çok gerekli değil alsında
             JOIN odimtd.snp_map_comp mco ON m.id = mco.i_owner_mapping
             JOIN odimtd.snp_map_cp mcp ON mcp.i_owner_map_comp = mco.i_map_comp
             JOIN odimtd.snp_map_attr matt ON matt.i_owner_map_cp = mcp.i_map_cp
    WHERE 1 = 1
      AND mco.i_map_comp_type = 9
    UNION ALL
    SELECT m.i_mapping AS mapp_id
         , 229 AS attr_ecid
         , matt.i_map_attr AS attr_eid
         , mco.i_map_comp_type AS attr_comp_type_id
         , mco.i_map_comp AS attr_comp_id
         , 1503 AS obj_ecid
         , cref.i_ref_id AS obj_eid
         , concat(m2.name,'.', matt2.name) obj_qname
         , 1501 AS cti_ecid
         , tref.i_ref_id AS cti_eid
         , tref.qualified_name AS cti_qname
    FROM odimtd.snp_mapping m
             JOIN odimtd.snp_map_comp mco ON m.i_mapping = mco.i_owner_mapping
             JOIN odimtd.snp_map_cp mcp ON mcp.i_owner_map_comp = mco.i_map_comp
             JOIN odimtd.snp_map_attr matt ON matt.i_owner_map_cp = mcp.i_map_cp
             JOIN odimtd.snp_map_ref cref ON cref.i_map_ref = matt.i_map_ref
             JOIN odimtd.snp_map_attr matt2 ON matt2.i_map_attr = cref.i_ref_id
             JOIN odimtd.snp_map_ref tref ON tref.i_map_ref = mco.i_map_ref
             JOIN odimtd.snp_mapping m2 ON m2.i_mapping = tref.i_ref_id
    WHERE 1 = 1
      AND mco.i_map_comp_type = 10
    UNION ALL
    SELECT m.id AS mapp_id
         , 229 AS attr_ecid
         , matt.i_map_attr AS attr_eid
         , mco.i_map_comp_type AS attr_comp_type_id
         , mco.i_map_comp AS attr_comp_id
         , 1504 AS obj_ecid
         , matt.i_map_attr AS obj_eid
         , concat(m.name,'.', mco.name, '.',matt.name)AS obj_qname
         , 1502 AS cti_ecid
         , mco.i_map_comp AS cti_eid
         , concat(m.fqname,mco.name) AS cti_qname
    FROM odimtd.mtd_mapping m
             JOIN odimtd.snp_map_comp mco ON m.id = mco.i_owner_mapping
             JOIN odimtd.snp_map_cp mcp ON mcp.i_owner_map_comp = mco.i_map_comp
             JOIN odimtd.snp_map_attr matt ON matt.i_owner_map_cp = mcp.i_map_cp
    WHERE 1 = 1
      AND (mco.i_map_comp_type = ANY (ARRAY [3, 7, 11, 13, 14, 15, 16, 17, 19, 20, 21, 24, 25, 26]))
    UNION ALL
    SELECT mref.i_owner_mapping AS mapp_id
             , 234 AS attr_ecid             --- map ref -- var fn vs için
             , mref.i_map_ref AS attr_eid
             , 100 AS attr_comp_type_id
             , NULL::integer AS attr_comp_id
             , lkp.ecid AS obj_ecid
             , mref.i_ref_id AS obj_eid
             , mref.qualified_name AS obj_qname
             , 138 AS cti_ecid
             , m.project_id AS cti_eid
             , m.project_name
        FROM odimtd.snp_map_ref mref LEFT JOIN (SELECT adapter_inf, ecid
                    FROM (VALUES ( 'IVariable', 187 ), ( 'ISequence', 156 ), ( 'ITrt', 176 )) AS oft(adapter_inf, ecid)) lkp
                  on mref.adapter_intf_type=lkp.adapter_inf
                  join odimtd.mtd_mapping m on mref.i_owner_mapping=m.id
        WHERE 1 = 1
          AND (mref.adapter_intf_type::text = ANY (ARRAY ['IVariable'::text, 'ISequence'::text, 'IFunction'::text]));

/*--mid_col_lin_base------------------------------------------------------------------------------------------------*/

    INSERT INTO odimtd.mid_col_lin_base(
                  rec_src_ecid, rec_src_eid, rec_src_qname, ecid, eid, qname, cti_ecid,cti_eid,cti_qname
                , ref_type_id, ref_ecid, ref_eid, ref_qname,ref_cti_ecid,ref_cti_eid,ref_cti_qname
                , attr_ecid, attr_eid, attr_comp_type_id, attr_comp_id
               , ref_attr_ecid, ref_attr_eid, ref_attr_comp_type_id, ref_attr_comp_id )
    SELECT  224 AS rec_src_ecid
         , d.mapp_id AS rec_src_eid
         , m.fqname AS rec_src_name
         , c.obj_ecid AS ecid
         , c.obj_eid AS eid
         , c.obj_qname AS qname
         , c.cti_ecid
         , c.cti_eid
         , c.cti_qname
         , 1 AS ref_type_id
         , c2.obj_ecid AS ref_ecid
         , c2.obj_eid AS ref_eid
         , c2.obj_qname AS ref_qname
         , c2.cti_ecid
         , c2.cti_eid
         , c2.cti_qname
         , d.attr_ecid
         , d.attr_eid
         , c.attr_comp_type_id
         , c.attr_comp_id
         , d.ref_attr_ecid
         , d.ref_attr_eid
         , c2.attr_comp_type_id AS ref_attr_comp_type_id
         , c2.attr_comp_id AS ref_attr_comp_id
    FROM odimtd.mid_attr_dep d
             JOIN      odimtd.mtd_mapping m ON d.mapp_id = m.id
             LEFT JOIN odimtd.mid_attr_obj_rel c
                       ON d.mapp_id = c.mapp_id AND d.attr_eid = c.attr_eid AND d.attr_ecid = c.attr_ecid
             LEFT JOIN odimtd.mid_attr_obj_rel c2
                       ON d.mapp_id = c2.mapp_id AND d.ref_attr_eid = c2.attr_eid AND d.ref_attr_ecid = c2.attr_ecid;

/*--mtd2_object_dep_base--------------------------------------------------------------------------------------------*/


    INSERT INTO odimtd.mtd2_object_dep_base
                (dep_group_id, rec_type, recs_ecid, recs_eid, recs_name,  recs_qname, recs_pqname
                , ecid, eid, name, qname, pqname, ref_type_id, ref_ecid, ref_eid, ref_name, ref_qname, ref_pqname
                , created_at, created_by
                )
    SELECT 2, 1, b.rec_src_ecid, b.rec_src_eid, rs.name, rs.qname, rs.pqname
    , b.ecid, b.eid, e.name, e.qname, e.pqname, 1, b.ref_ecid, b.ref_eid, re.name, re.qname, re.pqname
    , current_timestamp, user
    FROM odimtd.mid_col_lin_base b
             LEFT JOIN odimtd.mtds_entity rs ON b.rec_src_ecid = rs.ecid AND b.rec_src_eid = rs.eid
             LEFT JOIN odimtd.mtds_entity e ON b.ecid = e.ecid AND b.eid = e.eid
             LEFT JOIN odimtd.mtds_entity re ON b.ref_ecid = re.ecid AND b.ref_eid = re.eid;
END ;
$$;

alter procedure odimtd.load_dep1() owner to odimtd_admin;

create or replace procedure odimtd.load_dep2()
    language plpgsql
as
$$
DECLARE
BEGIN
    SET TIMEZONE = 'Europe/Istanbul';
    /* sırayla gitmeli. üstten alta bağ. var*/
    TRUNCATE TABLE odimtd.mid_comp_lookup;
    TRUNCATE TABLE odimtd.stg_comp_usage;
    TRUNCATE TABLE odimtd.mid_node_path;
    TRUNCATE TABLE odimtd.mid_comp_tab_rel;
    TRUNCATE TABLE odimtd.stg_tab_dep_lookup;
    TRUNCATE TABLE odimtd.mid_tab_dep_base;
    DELETE FROM odimtd.mtd2_object_dep_base WHERE dep_group_id = 1;
    TRUNCATE TABLE odimtd.mtd2_dependency1;
    /*--mid_comp_lookup-------------------------------------------------------------------------------------------------------------------*/

    INSERT
    INTO odimtd.stg_comp_usage ( mapp_id, mapp_name, i_map_comp, comp_name, comp_type, target_flag, source_flag
                               , lookup_flag)
    SELECT m.i_mapping                                                  AS mapp_id
         , m.name                                                       AS mapp_name
         , mco.i_map_comp
         , mco.name                                                     AS comp_name
         , mco.type_name                                                AS comp_type
         , (
               SELECT CASE WHEN COUNT(imcp.i_owner_map_comp) = 0 THEN FALSE ELSE TRUE END AS tgtflg
               FROM odimtd.snp_map_cp imcp
                        JOIN odimtd.snp_map_conn imcnt
                             ON imcp.i_map_cp = imcnt.i_end_map_cp AND imcp.direction::text = 'I'::text AND
                                mco.i_map_comp = imcp.i_owner_map_comp) AS target_flag
         , (
               SELECT CASE WHEN COUNT(imcp.i_owner_map_comp) = 0 THEN FALSE ELSE TRUE END AS srcflg
               FROM odimtd.snp_map_cp imcp
                        JOIN odimtd.snp_map_conn imcns
                             ON imcp.i_map_cp = imcns.i_start_map_cp AND imcp.direction::text = 'O'::text AND
                                mco.i_map_comp = imcp.i_owner_map_comp) AS source_flag
         , FALSE                                                        AS lookup_flag
    FROM odimtd.snp_mapping m
             JOIN odimtd.snp_map_comp mco
                  ON m.i_mapping = mco.i_owner_mapping AND (mco.i_map_comp_type = ANY (ARRAY [1, 10]))
    WHERE 1 = 1
      AND NOT (EXISTS
        (
            SELECT 1 FROM odimtd.mid_comp_lookup d WHERE d.mapp_id = m.i_mapping AND mco.i_map_comp = d.lkp_comp_id))
      AND NOT (EXISTS
        (
            SELECT 1 FROM odimtd.mid_comp_lookup d WHERE d.mapp_id = m.i_mapping AND mco.i_map_comp = d.src_comp_id))
    UNION ALL
    SELECT ld.mapp_id
         , ld.mapp_name
         , ld.lkp_comp_id                 AS i_map_comp
         , ld.lkp_comp                    AS comp_name
         , 'DATASTORE'::character varying AS comp_type
         , FALSE                          AS target_flag
         , FALSE                          AS source_flag
         , TRUE                           AS lookup_flag
    FROM odimtd.mid_comp_lookup ld
    UNION ALL
    SELECT ld.mapp_id
         , ld.mapp_name
         , ld.src_comp_id                 AS i_map_comp
         , ld.src_comp                    AS comp_name
         , 'DATASTORE'::character varying AS comp_type
         , FALSE                          AS target_flag
         , TRUE                           AS source_flag
         , FALSE                          AS lookup_flag
    FROM odimtd.mid_comp_lookup ld;

    /*--stg_comp_usage-------------------------------------------------------------------------------------------------------------------*/

    INSERT
    INTO odimtd.mid_node_path( mapp_id, lvl, comp_id, comp_name, comp_id_path, comp_path, km_id_path, km_path
                             , last_comp_id, last_comp, node_path)
    WITH
        RECURSIVE
        nodehy( lvl, nodeid, parnodeid, name, cpid, compid, compname, mappid, km, kmid, namepath, compnamepath
              , compidpath, nodeidpath, kmpath, kmidpath ) AS (
                                                                  SELECT 1                               AS lv
                                                                       , n.i_phy_node
                                                                       , n.i_par_phy_node
                                                                       , n.name
                                                                       , n.i_map_cp
                                                                       , n.i_map_comp
                                                                       , c.name::text                    AS comp_name
                                                                       , c.i_owner_mapping
                                                                       , km.trt_name
                                                                       , n.i_tgt_comp_km
                                                                       , ARRAY [n.name ]::varchar[]      AS namepath
                                                                       , ARRAY [c.name ]::varchar[]      AS compnamepath
                                                                       , ARRAY [c.i_map_comp]            AS compidpath
                                                                       , ARRAY [n.i_phy_node]            AS nodeidpath
                                                                       , ARRAY [km.trt_name] ::varchar[] AS tkmpath
                                                                       , ARRAY [n.i_tgt_comp_km]
                                                                  FROM odimtd.snp_phy_node n
                                                                           JOIN snp_map_cp p ON p.i_map_cp = n.i_map_cp
                                                                           JOIN snp_map_comp c ON p.i_owner_map_comp = c.i_map_comp
                                                                           LEFT JOIN snp_map_ref rt ON n.i_tgt_comp_km = rt.i_map_ref
                                                                           LEFT JOIN snp_trt km ON rt.i_ref_id = km.i_trt AND km.trt_type IN ( 'KL', 'KI' )
                                                                  WHERE 1 = 1
                                                                    AND c.i_map_comp_type IN ( 1, 9, 10 )
                                                                    AND n.node_type = 'B'
                                                                  UNION ALL
                                                                  SELECT 1 + d.lvl                                 AS lv
                                                                       , n.i_phy_node
                                                                       , n.i_par_phy_node
                                                                       , n.name
                                                                       , n.i_map_cp
                                                                       , n.i_map_comp
                                                                       , c.name                                    AS comp_name
                                                                       , c.i_owner_mapping
                                                                       , km.trt_name
                                                                       , n.i_tgt_comp_km
                                                                       --                     , concat( d.namepath, '>', n.name )
                                                                       , ARRAY_APPEND(d.namepath, n.name)
                                                                       , ARRAY_APPEND(d.compnamepath, c.name)
                                                                       , ARRAY_APPEND(d.compidpath, c.i_map_comp)
                                                                       , ARRAY_APPEND(d.nodeidpath, n.i_phy_node)
                                                                       , ARRAY_APPEND(d.kmpath, km.trt_name)       AS kmpath
                                                                       , ARRAY_APPEND(d.kmidpath, n.i_tgt_comp_km) AS kmidpath
                                                                  FROM odimtd.snp_phy_node n
                                                                           JOIN nodehy d ON d.parnodeid = n.i_phy_node
                                                                           JOIN snp_map_comp c ON n.i_map_comp = c.i_map_comp
                                                                           LEFT JOIN snp_map_ref rt ON n.i_tgt_comp_km = rt.i_map_ref
                                                                           LEFT JOIN snp_trt km ON rt.i_ref_id = km.i_trt AND km.trt_type IN ( 'KL', 'KI' )
                                                                  WHERE 1 = 1
                                                                  --
        )
    SELECT h.mappid       AS mapp_id
         , h.lvl
         , compidpath[1]  AS comp_id
         , compnamepath[1]   comp_name
         , h.compidpath   AS comp_id_path
         , h.compnamepath AS comp_path
         , h.kmidpath     AS km_id_path
         , h.kmpath       AS km_path
         , h.compid       AS last_comp_id
         , h.compname     AS last_comp
         , h.namepath     AS node_path
    --      , h.name AS last_node
    --      , h.tkmid AS last_km_id
    --      , h.tkm AS last_km
    --      , h.mapcpid AS last_cp_id
    --      , h.nodeidpath AS node_id_path
    FROM nodehy h
    WHERE 1 = 1
      AND parnodeid IS NULL;
    /*--mid_node_path-------------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mid_comp_lookup ( mapp_id, mapp_name, connector_comp_id, connector_comp, lkp_comp_id, lkp_comp
                                , src_comp_id, src_comp)
    WITH
        base AS (
                    SELECT mco.i_owner_mapping AS mapp_id
                         , m.name              AS mapp_name
                         , mco.i_map_comp      AS connector_comp_id
                         , mco.name            AS connector_comp
                         , mcp.i_map_cp        AS connector_cp
                         --      , mct.i_start_map_cp AS ctstart --obv=src_cp
                         --      , mct.i_end_map_cp AS ctend --obv=lkp_cp
                         , cpg.i_map_cp        AS ds_cp
                         , cmg.i_map_comp      AS src_comp_id
                         , cmg.name            AS src_comp
                         --      , cpg.direction-- obv.. O
                         , CASE
                               WHEN (
                                        SELECT COUNT(*)
                                        FROM snp_map_conn c2
                                        WHERE c2.i_start_map_cp = cpg.i_map_cp
                                          AND c2.i_end_map_cp <> mct.i_end_map_cp) = 0 THEN TRUE
                               ELSE FALSE END  AS lkp_flag
                    FROM snp_mapping m
                             JOIN snp_map_comp mco ON mco.i_owner_mapping = m.i_mapping
                             JOIN snp_map_cp mcp ON mco.i_map_comp = mcp.i_owner_map_comp
                             JOIN snp_map_conn mct ON mcp.i_map_cp = mct.i_end_map_cp
                             JOIN snp_map_cp cpg
                                  ON cpg.i_map_cp = mct.i_start_map_cp AND cpg.direction = 'O' --- lookup a output veren tüm cp ler
                             JOIN snp_map_comp cmg ON cpg.i_owner_map_comp = cmg.i_map_comp --lookup a output veren komponentler. b şaka output da varsa lookup değil
                    WHERE mco.i_map_comp_type = 14)
    SELECT b.mapp_id
         , b.mapp_name
         , b.connector_comp_id
         , b.connector_comp
         , b.src_comp_id AS lkp_comp_id
         , b.src_comp    AS lkp_comp
         , b2.src_comp_id
         , b2.src_comp
    FROM base b
             LEFT JOIN base b2 ON b.mapp_id = b2.mapp_id AND b.connector_comp_id = b2.connector_comp_id
    WHERE b.lkp_flag = TRUE
      AND b2.lkp_flag = FALSE;

    /*--mid_comp_tab_rel-------------------------------------------------------------------------------------------------------------------*/
    -- snp_map_comp da tanımlı komponentler ile tablo ilişkisi. Sadece tablo ile ilişkisi olanlar ve reusable mapping bulunur
    INSERT
    INTO odimtd.mid_comp_tab_rel( mapp_id
                                , folder_id
                                , project_id
                                , comp_id
                                , tab_ecid
                                , tab_eid
                                , tab_type
                                , tab_name
                                , tab_qname
                                , comp_type
                                , com_type_id
                                , org_comp_type_id
                                , target_flag
                                , source_flag
                                , lookup_flag
                                , comp_id_path
                                , comp_path
                                , km_id_path
                                , km_path)
    SELECT m.i_mapping         AS mapp_id
         , mf.id               AS folder_id
         , mf.project_id
         , mco.i_map_comp      AS comp_id
         , 173                 AS tab_ecid --snp_table
         , mt.id               AS tab_eid
         , 'T'::text           AS tab_type
         , mt.name             AS tab_name
         , mt.qname            AS tab_qname
         , mco.type_name       AS comp_type
         , mco.i_map_comp_type AS com_type_id
         , mco.i_map_comp_type AS org_comp_type_id
         , j.target_flag
         , j.source_flag
         , j.lookup_flag
         , np.comp_id_path
         , np.comp_path
         , np.km_id_path
         , np.km_path
    FROM odimtd.snp_mapping m
             JOIN odimtd.snp_map_comp mco ON m.i_mapping = mco.i_owner_mapping AND mco.i_map_comp_type = 1
             JOIN odimtd.snp_map_ref mre ON mco.i_map_ref = mre.i_map_ref
             JOIN odimtd.mtd_table mt ON mt.qname = mre.qualified_name::text
             JOIN odimtd.mtd_folder mf ON m.i_folder = mf.id
             LEFT JOIN odimtd.stg_comp_usage j ON j.mapp_id = j.mapp_id AND j.i_map_comp = mco.i_map_comp
             LEFT JOIN odimtd.mid_node_path np ON mco.i_map_comp = np.comp_id
    WHERE 1 = 1
    UNION ALL
    SELECT m.i_mapping         AS mapp_id
         , mf.id               AS folder_id
         , mf.project_id
         , mco.i_map_comp      AS comp_id
         , 1501                AS tab_ecid --reusable map as table. burada reusable mapping source olarak komponente bağlı
         , mref.i_ref_id       AS tab_eid
         , 'M'::text           AS tab_type
         , m2.name             AS tab_name
         , mref.qualified_name AS tab_qname
         , mco.type_name       AS comp_type
         , mco.i_map_comp_type AS com_type_id
         , mco.i_map_comp_type AS org_comp_type_id
         , FALSE               AS target_flag
         , TRUE                AS source_flag
         , FALSE               AS lookup_flag
         , np.comp_id_path
         , np.comp_path
         , np.km_id_path
         , np.km_path
    FROM odimtd.snp_mapping m
             JOIN odimtd.snp_map_comp mco ON m.i_mapping = mco.i_owner_mapping AND mco.i_map_comp_type = 10
             JOIN odimtd.snp_map_ref mref ON mco.i_map_ref = mref.i_map_ref
             JOIN odimtd.snp_mapping m2 ON m2.i_mapping = mref.i_ref_id
             JOIN odimtd.mtd_folder mf ON m.i_folder = mf.id
             LEFT JOIN odimtd.mid_node_path np ON mco.i_map_comp = np.comp_id
    WHERE 1 = 1
    UNION ALL
    SELECT m.i_mapping                                    AS mapp_id
         , mf.id                                          AS folder_id
         , mf.project_id
         , mco.i_map_comp                                 AS comp_id
         , 1501                                           AS tab_ecid ---reusable mapping'in hedefi olan output signature bir tablo
         , m.i_mapping                                    AS tab_eid
         , 'M'::text                                      AS tab_type
         , m.name                                         AS tab_name
         , (mf.fqname::text || '.'::text) || m.name::text AS tab_qname
         , mco.type_name --'REUSABLEMAPPING'::character varying AS comp_type --- @todo : mco.type_name=OUTPUTSIGNATURE olsa problem olur mu?
         , 10                                             AS com_type_id
         , mco.i_map_comp_type                            AS org_comp_type_id
         , TRUE                                           AS target_flag
         , FALSE                                          AS source_flag
         , FALSE                                          AS lookup_flag
         , NULL --reusable mapping de km yok
         , NULL
         , NULL
         , NULL
    FROM odimtd.snp_mapping m
             JOIN odimtd.snp_map_comp mco ON m.i_mapping = mco.i_owner_mapping AND mco.i_map_comp_type = 9
             JOIN odimtd.snp_map_cp mcp ON mco.i_map_comp = mcp.i_owner_map_comp
             JOIN odimtd.mtd_folder mf ON mf.id = m.i_folder
    WHERE 1 = 1;

    /*--stg_tab_dep_lookup-------------------------------------------------------------------------------------------------------------------*/

    INSERT
    INTO stg_tab_dep_lookup ( rec_src_ecid, rec_src_eid, rec_src_qname, tab_ecid, tab_eid, tab_qname, ref_tab_ecid
                            , ref_tab_eid, ref_tab_qname)
    SELECT DISTINCT b.rec_src_ecid
                  , b.rec_src_eid
                  , b.rec_src_qname
                  , r.tab_ecid
                  , r.tab_eid
                  , r.tab_qname
                  , b.ref_cti_ecid  AS ref_tab_ecid
                  , b.ref_cti_eid   AS ref_tab_eid
                  , b.ref_cti_qname AS ref_tab_qname
    FROM mid_comp_lookup l
             JOIN mid_col_lin_base b ON l.mapp_id = b.rec_src_eid AND l.src_comp_id = b.attr_comp_id
             JOIN mid_comp_tab_rel r ON l.mapp_id = r.mapp_id AND l.lkp_comp_id = r.comp_id;
    --@todo: mid_col_lin_base den cti bilgisini çıkardığımız içiz buraya ek join lazım

    /*--mid_tab_dep_base-------------------------------------------------------------------------------------------------------------------*/

    INSERT
    INTO odimtd.mid_tab_dep_base ( recs_ecid, recs_eid, rec_src_qname, ecid, eid, qname, ref_type_id, ref_ecid
                                 , ref_eid, ref_qname)
    SELECT ld.rec_src_ecid
         , ld.rec_src_eid
         , ld.rec_src_qname
         , ld.tab_ecid     AS ecid
         , ld.tab_eid      AS eid
         , ld.tab_qname    AS qname
         , 2               AS ref_type_id
         , ld.ref_tab_ecid AS ref_ecid
         , ld.ref_tab_eid  AS ref_eid
         , ld.ref_tab_qname
    FROM odimtd.stg_tab_dep_lookup ld
    UNION ALL
    SELECT DISTINCT b.rec_src_ecid
                  , b.rec_src_eid
                  , b.rec_src_qname
                  , b.cti_ecid  AS ecid
                  , b.cti_eid   AS eid
                  , b.cti_qname AS qname
                  , 1           AS ref_type_id
                  , b.ref_cti_ecid
                  , b.ref_cti_eid
                  , b.ref_cti_qname
    FROM odimtd.mid_col_lin_base b;

    /*--mtd2_obj_dep_base-------------------------------------------------------------------------------------------------------------------*/

    INSERT
    INTO odimtd.mtd2_object_dep_base ( dep_group_id, rec_type, recs_ecid, recs_eid, recs_name, recs_qname, recs_pqname
                                     , ecid, eid, name, qname, pqname, ref_type_id, ref_ecid, ref_eid, ref_name
                                     , ref_qname, ref_pqname, created_at, created_by)
    SELECT 1
         , 1
         , b.recs_ecid
         , b.recs_eid
         , rs.name
         , rs.qname
         , rs.pqname
         , b.ecid
         , b.eid
         , e.name
         , e.qname
         , e.pqname
         , b.ref_type_id
         , b.ref_ecid
         , b.ref_eid
         , re.name
         , re.qname
         , re.pqname
         , CURRENT_TIMESTAMP
         , USER
    FROM odimtd.mid_tab_dep_base b
             LEFT JOIN odimtd.mtds_entity rs ON b.recs_ecid = rs.ecid AND b.recs_eid = rs.eid
             LEFT JOIN odimtd.mtds_entity e ON b.ecid = e.ecid AND b.eid = e.eid
             LEFT JOIN odimtd.mtds_entity re ON b.ref_ecid = re.ecid AND b.ref_eid = re.eid;

    INSERT
    INTO mtd2_dependency1
    WITH
        RECURSIVE
        src AS (
                   SELECT DISTINCT b.dep_group_id
                                 , NULL::odimtd.euid   AS paruid
                                 , b.ref_uid           AS uid
                                 , b.ref_qname         AS name
                                 , ARRAY [b.ref_uid]   AS uidpth
                                 , ARRAY [b.ref_qname] AS namepth
                                 , NULL::odimtd.euid   AS recsuid
                                 , NULL::varchar       AS recsname
                                 , NULL::odimtd.euid[] AS recsuidpth
                                 , NULL::varchar[]     AS recsnamepth
                                 , CASE
                                       WHEN NOT EXISTS (
                                                           SELECT 1
                                                           FROM odimtd.mtd2_object_dep_base hi
                                                           WHERE hi.ref_uid = b.uid
                                                             AND hi.dep_group_id = b.dep_group_id) THEN TRUE
                                       ELSE FALSE END  AS is_trgleaf
                                 , CASE
                                       WHEN NOT EXISTS (
                                                           SELECT 1
                                                           FROM odimtd.mtd2_object_dep_base hi
                                                           WHERE hi.uid = b.ref_uid
                                                             AND hi.dep_group_id = b.dep_group_id) THEN TRUE
                                       ELSE FALSE END  AS is_srcleaf
                   FROM odimtd.mtd2_object_dep_base b
                   WHERE 1 = 1
                     AND NOT EXISTS(
                                       SELECT 1
                                       FROM odimtd.mtd2_object_dep_base exc
                                       WHERE exc.uid = b.ref_uid AND exc.dep_group_id = b.dep_group_id)
                   AND NOT EXISTS(
                    SELECT 1
                    FROM mtds_entity_extend e
                    WHERE e.props_sys ->> 'ExcFlag' = 'true' AND (e.uid = b.uid OR e.uid = b.ref_uid))
                   UNION ALL
                   SELECT b.dep_group_id
                        , b.ref_uid                         AS paruid
                        , b.uid                             AS uid
                        , b.qname                           AS name
                        , ARRAY [b.uid]                     AS uidpth
                        , ARRAY [b.qname]                   AS namepth
                        , b.recs_uid                        AS recsuid
                        , b.recs_qname                      AS recsname
                        , ARRAY [b.recs_uid]::odimtd.euid[] AS recsuidpth
                        , ARRAY [b.recs_qname]::varchar[]   AS recsnamepth
                        , CASE
                              WHEN NOT EXISTS (
                                                  SELECT 1
                                                  FROM odimtd.mtd2_object_dep_base hi
                                                  WHERE hi.ref_uid = b.uid AND hi.dep_group_id = b.dep_group_id)
                                  THEN TRUE
                              ELSE FALSE END                AS is_trgleaf
                        , CASE
                              WHEN NOT EXISTS (
                                                  SELECT 1
                                                  FROM odimtd.mtd2_object_dep_base hi
                                                  WHERE hi.uid = b.ref_uid AND hi.dep_group_id = b.dep_group_id)
                                  THEN TRUE
                              ELSE FALSE END                AS is_srcleaf
                   FROM odimtd.mtd2_object_dep_base b
                   WHERE 1 = 1
                      AND NOT EXISTS(
                                SELECT 1
                                FROM mtds_entity_extend e
                                WHERE e.props_sys ->> 'ExcFlag' = 'true' AND (e.uid = b.uid OR e.uid = b.ref_uid))


                   )
      , hy ( depgroup
           , lvl
           , paruid
           , uid
           , name
           , uidpth
           , namepth
           , recsuid
           , recsname
           , recsuidpth
           , recsnamepth
           , istrgleaf
           , issrcleaf
           , trgsibord
           , srcsibord
        )   AS (
                   SELECT s.dep_group_id
                        , 1
                        , s.paruid
                        , s.uid
                        , s.name
                        , s.uidpth
                        , s.namepth
                        , s.recsuid
                        , s.recsname
                        , s.recsuidpth
                        , s.recsnamepth
                        , s.is_trgleaf
                        , s.is_srcleaf
                        , ROW_NUMBER() OVER (PARTITION BY s.dep_group_id,s.paruid ORDER BY s.uid)::varchar  AS trgsibord
                        , ROW_NUMBER() OVER (PARTITION BY s.dep_group_id, s.uid ORDER BY s.paruid)::varchar AS srcsibord
                   FROM src s
                   UNION ALL
                   SELECT b.dep_group_id
                        , h.lvl + 1                                                                       AS lvl
                        , b.ref_uid                                                                       AS paruid
                        , b.uid
                        , b.qname
                        , ARRAY_APPEND(h.uidpth, b.uid)                                                   AS uidpth
                        , ARRAY_APPEND(h.namepth, b.qname)                                                AS namepth
                        , b.recs_uid
                        , b.recs_qname
                        , ARRAY_APPEND(h.recsuidpth, b.recs_uid)                                          AS recsuidpth
                        , ARRAY_APPEND(h.recsnamepth, b.recs_qname)                                       AS recsrc_qname_path
                        , CASE
                              WHEN NOT EXISTS (
                                                  SELECT 1
                                                  FROM odimtd.mtd2_object_dep_base hi
                                                  WHERE hi.ref_uid = b.uid AND hi.dep_group_id = b.dep_group_id)
                                  THEN TRUE
                              ELSE FALSE END                                                              AS istrgleaf
                        , h.issrcleaf
                        , h.trgsibord || '.' || ROW_NUMBER() OVER (PARTITION BY b.ref_uid ORDER BY b.uid) AS trgsibord
                        , ROW_NUMBER() OVER (PARTITION BY b.dep_group_id,b.uid ORDER BY b.ref_uid)::varchar || '.' ||
                          h.srcsibord                                                                     AS srcsibord
                   FROM odimtd.mtd2_object_dep_base b
                            JOIN hy h ON h.uid = b.ref_uid AND h.depgroup = b.dep_group_id)
    SELECT h.depgroup
         , h.lvl
         , h.uidpth[1]                               AS first_uid
         , h.namepth[1]                              AS first_name
         , h.recsuidpth[1]                           AS first_recs_uid
         , h.recsnamepth[1]                          AS first_recs_name
         , h.paruid                                  AS last_par_uid
         , h.uid                                     AS last_uid
         , h.name                                    AS last_name
         , h.recsuid                                 AS last_recs_uid
         , h.recsname                                AS last_recs_name
         , h.uidpth
         , h.namepth
         , h.recsuidpth
         , h.recsnamepth
         , h.istrgleaf                               AS is_trg_leaf
         , issrcleaf                                 AS is_src_leaf
         , trgsibord                                 AS trg_sibl_order
         , srcsibord                                 AS src_sibl_order
         , CONCAT(REPEAT('  ', lvl), h.namepth[lvl]) AS trg_name_tree
         , CONCAT(REPEAT('  ', lvl), h.namepth[1])   AS src_name_tree
    FROM hy h
    WHERE 1 = 1;

END ;
$$;

alter procedure odimtd.load_dep2() owner to odimtd_admin;

create or replace procedure odimtd.load_entity()
    language plpgsql
as
$$
DECLARE
BEGIN
    /* @TODO: sondaki date-user alanlarını kaldırdım da onlar sanki lazım gibi. kayıt ekleme tarihi değil odi daki kayıtlardı. bir an kafam gitmiş
     kalsın bakalım böyle. işlevsiz ise bu 4 kolonu düşürebiliriz
        */
    SET TIMEZONE = 'Europe/Istanbul';
    DELETE FROM mtds_entity WHERE src_type_id = 1;
    /*----coll----------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtds_entity ( ecid, eid, name, qname, pqname, type, cti_ecid, cti_eid, cti_type, props, description
                            , global_id)
    SELECT 1504
         , matt.i_map_attr
         , matt.name
         , m.fqname || '.' || mco.name || '.' || matt.name AS qname
         , m.fqname || '.' || mco.name || '.' || matt.name AS pqname
         , 'Col.Virtual.Attr'
         , 1502                                            AS cti_ecid -- map comp
         , mco.i_map_comp                                  AS cti_eid
         , 'Tab.Virtual.Comp.' || mco.type_name
         , JSON_BUILD_OBJECT('data_type', SPLIT_PART(mref.qualified_name, '.', 2), 'length', matt.length, 'scale',
                             matt.scale, 'mandatory_flag', matt.is_required::boolean)::jsonb
         , matt.descript
         , matt.global_id
    FROM odimtd.mtd_mapping m
             JOIN odimtd.snp_map_comp mco ON m.id = mco.i_owner_mapping
             JOIN odimtd.snp_map_cp mcp ON mcp.i_owner_map_comp = mco.i_map_comp
             JOIN odimtd.snp_map_attr matt ON matt.i_owner_map_cp = mcp.i_map_cp
             JOIN odimtd.snp_map_ref mref ON matt.i_data_map_ref = mref.i_map_ref
    WHERE 1 = 1
      --       AND (mco.i_map_comp_type = ANY (ARRAY [3, 7, 11, 14, 16])) --expression,set,aggregate,lookup,distinct
      AND mco.i_map_comp_type = ANY (ARRAY [3, 7, 11, 13, 14, 15, 16, 17, 19, 20, 21, 24, 25, 26])
    UNION ALL
    SELECT 1503
         , matt.i_map_attr
         , matt.name
         , m.fqname || '.' || matt.name AS qname
         , m.fqname || '.' || matt.name AS pqname
         , 'Col.Virtual.Attr'
         , 1501                         AS cti_ecid --mapping . reusable as table olunca
         , m.id
         , 'Tab.Virtual.Reus.' || mco.type_name
         , JSON_BUILD_OBJECT('data_type', SPLIT_PART(mref.qualified_name, '.', 2), 'length', matt.length, 'scale',
                             matt.scale, 'mandatory_flag', matt.is_required::boolean)::jsonb
         , matt.descript
         , matt.global_id
    FROM odimtd.mtd_mapping m
             JOIN odimtd.snp_map_comp mco ON m.id = mco.i_owner_mapping
             JOIN odimtd.snp_map_cp mcp ON mcp.i_owner_map_comp = mco.i_map_comp
             JOIN odimtd.snp_map_attr matt ON matt.i_owner_map_cp = mcp.i_map_cp
             JOIN odimtd.snp_map_ref mref ON matt.i_data_map_ref = mref.i_map_ref
    WHERE 1 = 1
      AND mco.i_map_comp_type = 9
    UNION ALL
    SELECT 98
         , c.id
         , c.name
         , c.qname
         , c.pqname
         , 'Col'
         , 173       AS cti_ecid --tablo
         , t.i_table AS cti_eid
         , 'Tab'
         , c.props
         , c.description
         , c.global_id
    FROM odimtd.mtd_col c
             JOIN odimtd.snp_table t ON c.table_id = t.i_table;
    /*--table-----------------------------------------------------------------------------------*/
    INSERT
    INTO mtds_entity (ecid, eid, name, qname, pqname, type, cti_ecid, cti_eid, cti_type, props, description, global_id)
    SELECT 173
         , t.id
         , t.name
         , t.qname
         , t.pqname
         , 'Tab'
         , 125   AS cti_ecid --model
         , t.model_id
         , 'Mod' AS cti_type
         , t.props --flat file özellikleri gitti. ?
         , t.description
         , t.global_id
    FROM odimtd.mtd_table t
    UNION ALL
    SELECT 1501
         , m.id
         , m.name
         , m.fqname
         , m.fqname
         , 'Tab.Virtual'
         , 105         AS cti_ecid --@todo: folderyerine 1505 mi koysam. eid de 0 olur . tablonun konteynerı şemaya gitmek için lazım. bu karıştırabilir. fiziksel ismi çıkarırken belli olur
         , m.folder_id AS cti_eid
         , 'Folder'    AS cti_type
         , NULL
         , m.description
         , m.global_id
    FROM odimtd.mtd_mapping m
             JOIN odimtd.mtd_folder f ON m.folder_id = f.id
    WHERE m.reusable_flag = 'Y'
    UNION ALL
    SELECT 1502
         , mco.i_map_comp
         , mco.name
         , mf.fqname::text || m.name::text || mco.name::text
         , mf.fqname::text || m.name::text || mco.name::text
         , 'Tab.Virtual.Comp.' || mco.type_name
         , 224         AS cti_ecid
         , m.i_mapping AS cti_eid
         , 'Mapping'
         , JSON_BUILD_OBJECT('table_type', 'V', 'comp_type', mco.type_name, 'folder', mf.fqname)
         , m.descript
         , mco.global_id
    FROM odimtd.snp_mapping m
             LEFT JOIN odimtd.snp_map_comp mco ON m.i_mapping = mco.i_owner_mapping
             LEFT JOIN odimtd.mtd_folder mf ON m.i_folder = mf.id
    WHERE 1 = 1
      AND mco.i_map_comp_type = ANY (ARRAY [3, 7, 11, 13, 14, 15, 16, 17, 19, 20, 21, 24, 25, 26]);
    /*--variables---------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO mtds_entity (ecid, eid, name, qname, type, cti_ecid, cti_eid, cti_type, props, description, global_id)
    SELECT 187
         , v.id
         , v.name
         , v.qname
         , 'mref'
         , 138
         , v.project_id
         , 'ProjectLevel'
         , JSON_BUILD_OBJECT('type', v.type, 'datatype', v.datatype, 'value', v.value, 'lschema', v.logical_schema,
                             'formula', v.formula, 'history_behaviour', v.history_behaviour, 'default_value',
                             v.default_value)
         , v.description
         , v.global_id
    FROM odimtd.mtd_var v;
    /*--km----------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO mtds_entity ( ecid, eid, name, qname, type, cti_ecid, cti_eid, cti_type, props, description, created_at
                     , created_by, updated_at, updated_by, global_id)
    SELECT 1506 --anlaşılır olsun diye sorguyu ikiye böldüm
         , k.id
         , k.name
         , k.qname
         , 'Trt'
         , CASE WHEN k.project_id IS NULL THEN 1000 ELSE 138 END
         , COALESCE(k.project_id, 0)
         , k.def_level
         , k.props
         , k.description
         , k.created_at
         , k.created_by
         , k.updated_at
         , k.updated_by
         , k.global_id
    FROM mtd_km k;

    /*--procedures----------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO mtds_entity ( ecid, eid, name, qname, type, cti_ecid, cti_eid, cti_type, props, description, created_at
                     , created_by, updated_at, updated_by, global_id)
    SELECT 1507
         , p.id
         , p.name
         , p.qname
         , 'Trt'
         , 105
         , p.folder_id
         , NULL
         , p.props || JSON_BUILD_OBJECT('steps', p.steps)::jsonb
         , p.description
         , p.created_at
         , p.created_by
         , p.updated_at
         , p.updated_by
         , p.global_id
    FROM mtd_procedure p;

    /*--mapping----------------------------------------------------------------------------------------------------------------*/

    INSERT
    INTO mtds_entity ( ecid, eid, name, qname, type, cti_ecid, cti_eid, cti_type, props, description, created_at
                     , created_by, updated_at, updated_by, global_id)
    SELECT 224
         , m.id
         , m.name
         , m.fqname
         , NULL::varchar
         , 105
         , m.folder_id
         , NULL
         , JSON_BUILD_OBJECT('reusable_flag', m.reusable_flag) -- project id/name koymalı mıyım? @todo
         , m.description
         , m.created_at
         , m.created_by
         , m.updated_at
         , m.updated_by
         , m.global_id
    FROM odimtd.mtd_mapping m;
    /*--folder----------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO mtds_entity (ecid, eid, name, qname, type, cti_ecid, cti_eid, cti_type, props, description, global_id)
    SELECT 105 --dizinleri seviye ayrıı olmaksızın proje altına koydum.
         , f.id
         , f.name
         , f.fqname
         , 'Container'
         , 138
         , f.project_id
         , NULL
         , JSON_BUILD_OBJECT('lvl', f.lvl, 'parent_id', f.parent_id, 'project_name', f.project_name, 'id_path',
                             f.id_path, 'name_path', f.name_path, 'order_path', f.order_path)
         , NULL
         , f.global_id
    FROM mtd_folder f;
    /*--sequence----------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO mtds_entity (ecid, eid, name, qname, type, cti_ecid, cti_eid, cti_type, props, description, global_id)
    SELECT 156
         , s.seq_id
         , s.seq_name
         , p.project_name || '.' || s.seq_name
         , 'Seq' || s.seq_type
         , CASE WHEN s.seq_type = 'P' THEN 138 ELSE 1000 END
         , COALESCE(s.i_project, 0)
         , NULL
         , JSON_BUILD_OBJECT('lschema', s.lschema_name)
         , NULL
         , s.global_id
    FROM snp_sequence s
             LEFT JOIN snp_project p ON s.i_project = p.i_project;
    /*--model----------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO mtds_entity (ecid, eid, name, qname, type, cti_ecid, cti_eid, cti_type, props, description, global_id)
    SELECT 125
         , m.id
         , m.name
         , m.code
         , 'Logicel' --pek anlamlı değil
         , 205
         , model_folder_id
         , NULL
         , props
         , m.description
         , m.global_id
    FROM mtd_model m;

END ;
$$;

alter procedure odimtd.load_entity() owner to odimtd_admin;

create or replace procedure odimtd.load_mtd1()
    language plpgsql
as
$$
DECLARE
BEGIN
    SET TIMEZONE = 'Europe/Istanbul';
    TRUNCATE TABLE odimtd.mtd_connection;
    TRUNCATE TABLE odimtd.mtd_schema_context;
    TRUNCATE TABLE odimtd.mtd_lschema;
    TRUNCATE TABLE odimtd.mtd_model_folder;
    TRUNCATE TABLE odimtd.mtd_folder;
    TRUNCATE TABLE odimtd.mtd_scen_folder;
    TRUNCATE TABLE odimtd.mtd_model;
    TRUNCATE TABLE odimtd.mtd_table;
    TRUNCATE TABLE odimtd.mtd_col;
    TRUNCATE TABLE odimtd.mtd_package;
    TRUNCATE TABLE odimtd.mtd_package_step;
    /*--mtd_connection-------------------------------------------------------------------------------------------------------------------*/
    /*alias user defined.*/
    INSERT
    INTO odimtd.mtd_connection ( id
                               , techno
                               , name
                               , server
                               , con_type
                               , conn_user
                               , props
                               , created_at
                               , created_by
                               , updated_at
                               , updated_by
                               , global_id)
    SELECT c.i_connect AS id
         , t.techno_name
         , c.con_name
         , c.dserv_name
         , c.connect_type
         , c.user_name
         , JSON_BUILD_OBJECT('server', c.dserv_name, 'on_connect', c.on_con_cmd, 'on_disconnect', c.on_dcn_cmd,
                             'batch_update_size', c.batch_update_size, 'jndi_flag', c.ind_jndi::boolean, 'jndi_res',
                             jndi_ressource, 'jagent_address', CASE
                                                                   WHEN jagent_host IS NULL THEN NULL
                                                                   ELSE jagent_host || ':' || jagent_port::varchar END)
         , c.first_date
         , c.first_user
         , c.last_date
         , c.last_user
         , c.global_id
    FROM odimtd.snp_connect c
             JOIN odimtd.snp_techno t ON c.i_techno = t.i_techno;
    /*--mtd_schema_context-------------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_schema_context ( context_id
                                   , lschema_id
                                   , pschema_id
                                   , context
                                   , lschema
                                   , pschema
                                   , catalog
                                   , db
                                   , server
                                   , pschema_qname
                                   , techno
                                   , created_at
                                   , created_by
                                   , updated_at
                                   , updated_by)
    SELECT sc.i_context    AS context_id
         , ls.i_lschema    AS lschema_id
         , sp.i_pschema    AS pschema_id
         , sc.context_name AS context
         , ls.lschema_name AS lschema
         , sp.schema_name  AS pschema
         , sp.catalog_name AS catalog
         , scn.con_name    AS db
         , scn.dserv_name  AS server
         , sp.ext_name     AS pschema_qname
         , tc.techno_name  AS techno
         , ls.first_date
         , ls.first_user
         , ls.last_date
         , ls.last_user
    FROM snp_lschema ls
             JOIN snp_techno tc ON ls.i_techno = tc.i_techno
             LEFT JOIN snp_pschema_cont spc ON ls.i_lschema = spc.i_lschema
             LEFT JOIN snp_context sc ON spc.i_context = sc.i_context
             LEFT JOIN snp_pschema sp ON spc.i_pschema = sp.i_pschema
             LEFT JOIN snp_connect scn ON sp.i_connect = scn.i_connect;
    /*--mtd_lschema-------------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_lschema ( id
                            , context
                            , lschema
                            , pschema_id
                            , pschema
                            , catalog
                            , db
                            , server
                            , pschema_qname
                            , techno
                            , created_at
                            , created_by
                            , updated_at
                            , updated_by
                            , global_id)
    SELECT i_lschema       AS id
         , p.context
         , lschema_name    AS lschema
         , pschema_id
         , pschema
         , catalog
         , db
         , server
         , pschema_qname
         , t.tech_int_name AS techno
         , l.first_date
         , l.first_user
         , l.last_date
         , l.last_user
         , l.global_id
    FROM snp_lschema l
             LEFT JOIN snp_techno t ON l.i_techno = t.i_techno
             LEFT JOIN odimtd.mtd_schema_context p ON l.i_lschema = p.lschema_id AND p.context_id = (
                                                                                                        SELECT c.value_int
                                                                                                        FROM mtds_config c
                                                                                                        WHERE c.code = 'DEFCTX');
    /*--mtd_model_folder-------------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_model_folder ( id
                                 , parent_id
                                 , lvl
                                 , name
                                 , fqname
                                 , id_path
                                 , name_path
                                 , description_id
                                 , description
                                 , created_at
                                 , created_by
                                 , updated_at
                                 , updated_by
                                 , global_id)
    WITH
        RECURSIVE
        dims( id, parent_id, lvl, name, id_path, name_path ) AS (
                                                                    SELECT f.i_mod_folder
                                                                         , f.par_i_mod_folder
                                                                         , 0 AS lvl
                                                                         , f.mod_folder_name
                                                                         , ARRAY [f.i_mod_folder]::integer[]
                                                                         , ARRAY [f.mod_folder_name]::varchar[]
                                                                    FROM snp_mod_folder f
                                                                    WHERE f.par_i_mod_folder IS NULL
                                                                    UNION ALL
                                                                    SELECT c.i_mod_folder
                                                                         , c.par_i_mod_folder
                                                                         , lvl + 1
                                                                         , c.mod_folder_name
                                                                         , ARRAY_APPEND(id_path, c.i_mod_folder)
                                                                         , ARRAY_APPEND(name_path, c.mod_folder_name)
                                                                    FROM snp_mod_folder c
                                                                             JOIN dims AS n ON n.id = c.par_i_mod_folder)
    SELECT d.id
         , d.parent_id
         , d.lvl
         , d.name
         , ARRAY_TO_STRING(d.name_path, '.')
         , d.id_path
         , d.name_path
         , f.i_txt_description
         , h.full_text
         , f.first_date
         , f.first_user
         , f.last_date
         , f.last_user
         , f.global_id
    FROM dims d
             JOIN snp_mod_folder f ON d.id = f.i_mod_folder
             LEFT JOIN snp_txt_header h ON f.i_txt_description = h.i_txt AND h.i_txt_orig = 106
    UNION ALL
    SELECT 0
         , NULL
         , 0
         , '/'
         , '.'
         , NULL
         , NULL
         , NULL
         , 'Model root'
         , CURRENT_DATE
         , USER
         , CURRENT_DATE
         , USER
         , NULL
    ORDER BY id_path;
    /*--mtd_folder-------------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO mtd_folder ( id
                    , parent_id
                    , lvl
                    , name
                    , fqname
                    , project_id
                    , project_name
                    , id_path
                    , name_path
                    , order_path
                    , created_at
                    , created_by
                    , updated_at
                    , updated_by
                    , global_id)
    WITH
        RECURSIVE
        dims( id, parent_id, name, lvl, id_path, name_path, order_path ) AS (
                                                                                SELECT f.i_folder
                                                                                     , f.par_i_folder
                                                                                     , f.folder_name
                                                                                     , 0::integer                       AS lvl
                                                                                     , ARRAY [f.i_folder ]::int4[]      AS id_path
                                                                                     , ARRAY [f.folder_name]::varchar[] AS name_path
                                                                                     , ARRAY [f.ord_folder]::int4[]     AS order_path
                                                                                FROM snp_folder f
                                                                                WHERE f.par_i_folder IS NULL
                                                                                UNION ALL
                                                                                SELECT c.i_folder
                                                                                     , c.par_i_folder
                                                                                     , c.folder_name
                                                                                     , lvl::integer + 1
                                                                                     , ARRAY_APPEND(id_path, c.i_folder)
                                                                                     , ARRAY_APPEND(name_path, c.folder_name)
                                                                                     , ARRAY_APPEND(order_path, c.ord_folder)
                                                                                FROM snp_folder c
                                                                                         JOIN dims AS n ON n.id = c.par_i_folder)
    SELECT d.id
         , d.parent_id
         , d.lvl
         , d.name
         , p.project_name || '.' || ARRAY_TO_STRING(d.name_path, '.')
         , f.i_project
         , p.project_name
         , d.id_path
         , d.name_path
         , d.order_path
         , f.first_date
         , f.first_user
         , f.last_date
         , f.last_user
         , f.global_id
    FROM dims d
             INNER JOIN snp_folder f ON d.id = f.i_folder
             INNER JOIN snp_project p ON f.i_project = p.i_project
    ORDER BY order_path;
    /*--mtd_scen_folder-------------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_scen_folder ( id
                                , parent_id
                                , lvl
                                , name
                                , qname
                                , id_path
                                , name_path
                                , description_id
                                , description
                                , created_at
                                , created_by
                                , updated_at
                                , updated_by
                                , global_id)
    WITH
        RECURSIVE
        dims( id, parent_id, lvl, name, id_path, name_path ) AS (
                                                                    SELECT f.i_scen_folder
                                                                         , f.par_i_scen_folder
                                                                         , 0 AS lvl
                                                                         , f.scen_folder_name
                                                                         , ARRAY [f.i_scen_folder]::integer[]
                                                                         , ARRAY [f.scen_folder_name]::varchar[]
                                                                    FROM snp_scen_folder f
                                                                    WHERE f.par_i_scen_folder IS NULL
                                                                    UNION ALL
                                                                    SELECT c.i_scen_folder
                                                                         , c.par_i_scen_folder
                                                                         , lvl + 1
                                                                         , c.scen_folder_name
                                                                         , ARRAY_APPEND(id_path, c.i_scen_folder)
                                                                         , ARRAY_APPEND(name_path, c.scen_folder_name)
                                                                    FROM snp_scen_folder c
                                                                             JOIN dims AS n ON n.id = c.par_i_scen_folder)
    SELECT d.id
         , d.parent_id
         , d.lvl
         , d.name
         , array_to_string(d.name_path,'.')
         , d.id_path
         , d.name_path
         , f.i_txt_description
         , h.full_text
         , f.first_date
         , f.first_user
         , f.last_date
         , f.last_user
         , f.global_id
    FROM dims d
             JOIN snp_scen_folder f ON d.id = f.i_scen_folder
             LEFT JOIN snp_txt_header h ON f.i_txt_description = h.i_txt --- dok da i_text_orig yok
    ORDER BY id_path;
    /*--mtd_model-------------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_model ( id
                          , code
                          , name
                          , fqname
                          , lschema
                          , server
                          , db
                          , catalog
                          , pschema
                          , pschema_qname
                          , techno
                          , model_folder_id
                          , model_folder_id_path
                          , folder_path
                          , props
                          , description
                          , created_at
                          , created_by
                          , updated_at
                          , updated_by
                          , global_id)
    SELECT m.i_mod                                        AS id
         , m.cod_mod                                      AS code
         , m.mod_name                                     AS name
         , ARRAY_TO_STRING(mf.name_path, '.', m.mod_name) AS fqname
         , m.lschema_name                                 AS lschema
         , ls.server
         , ls.db
         , ls.catalog
         , ls.pschema
         , ls.pschema_qname
         , ls.techno
         , COALESCE(m.i_mod_folder, 0)
         , mf.id_path                                     AS model_folder_id_path
         , mf.name_path
         , null::jsonb
         , th.full_text                                   AS descript
         , m.first_date
         , m.first_user
         , m.last_date
         , m.last_user
         , m.global_id
    FROM odimtd.snp_model m
             LEFT JOIN odimtd.mtd_lschema ls ON m.lschema_name = ls.lschema
             LEFT JOIN odimtd.mtd_model_folder mf ON m.i_mod_folder = mf.id
             LEFT JOIN odimtd.snp_txt_header th ON m.i_txt_mod = th.i_txt AND th.i_txt_orig = 104;
    /*--mtd_table-------------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_table ( id
                          , name
                          , qname
                          , pqname
                          , type
                          , lpath
                          , model_code
                          , model_id
                          , model
                          , lschema_id
                          , lschema
                          , server
                          , db
                          , pschema
                          , pschema_qname
                          , props
                          , description
                          , created_at
                          , created_by
                          , updated_at
                          , updated_by
                          , global_id)
    SELECT t.i_table                               AS id
         , t.table_name                            AS name
         , UPPER(mdl.code) || '.' || t.table_name  AS qname
         , mdl.pschema_qname || '.' || t.table_name
         , t.table_type                            AS type
         , ARRAY_APPEND(mdl.folder_path, mdl.code) AS lpath
         , mdl.code                                AS model_code
         , mdl.id
         , mdl.name                                AS model
         , lsm.i_lschema                           AS lschema_id
         , mdl.lschema
         , mdl.server
         , mdl.db
         , mdl.pschema
         , mdl.pschema_qname
         , JSON_BUILD_OBJECT('model_folder_id', mdl.id, 'submodel_id', t.i_sub_model, 'phy_name', t.res_name, 'alias',
                             t.table_alias, 'visible_flag', t.ind_show::bool, 'olap_type', t.olap_type, 'jrn_flag',
                             t.ind_jrn::bool, 'ws_flag', t.ind_ws::bool, 'ws_name', t.ws_name, 'ws_entity_name',
                             t.ws_entity_name)     AS props
         , h.full_text                             AS description
         , t.first_date
         , t.first_user
         , t.last_date
         , t.last_user
         , t.global_id
    FROM snp_table t
             LEFT JOIN mtd_model mdl ON t.i_mod = mdl.id
             LEFT JOIN snp_lschema lsm ON mdl.lschema = lsm.lschema_name
             LEFT JOIN snp_txt_header h ON t.i_txt_desc = h.i_txt AND h.i_txt_orig = 100;
    /*--mtd_col-------------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_col( id
                       , table_id
                       , name
                       , qname
                       , pqname
                       , tab_name
                       , tab_qname
                       , tab_pqname
                       , heading
                       , data_type
                       , col_no
                       , length
                       , scale
                       , def_value
                       , format
                       , null_if_error_act
                       , writable_flag
                       , mandatory_flag
                       , props
                       , description
                       , created_at
                       , created_by
                       , updated_at
                       , updated_by
                       , global_id)
    SELECT c.i_col               AS id
         , c.i_table             AS table_id
         , c.col_name
         , t.qname || '.' || c.col_name
         , t.pqname || '.' || c.col_name
         , t.name
         , t.qname
         , t.pqname
         , c.col_heading
         , c.source_dt           AS data_type
         , c.pos
         , c.longc
         , c.scalec
         , c.def_value
         , c.col_format
         , c.col_null_if_err::smallint
         , c.ind_write::bool     AS writable
         , c.col_mandatory::bool AS mandatory
         , JSON_BUILD_OBJECT('data_type', c.source_dt, 'length', c.longc, 'scale', c.scalec, 'mandatory_flag',
                             col_mandatory, 'null_if_error_act', col_null_if_err, 'writable_flag', ind_write,
                            'scd_col_type',c.scd_col_type,       'check_flow_flag', check_flow, 'check_stat_flag', check_stat, 'ws_select_flag',
                             ind_ws_select, 'ws_insert_flag', ind_ws_insert, 'ws_update_flag', ind_ws_update)
         , h.full_text           AS description
         , c.first_date
         , c.first_user
         , c.last_date
         , c.last_user
         , c.global_id
    FROM snp_col c
             LEFT JOIN snp_txt_header h ON (c.i_txt_col_desc = h.i_txt)
             LEFT JOIN mtd_table t ON c.i_table = t.id;
    /*--mtd_package-------------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_package
    SELECT p.i_package  AS id
         , p.pack_name  AS name
         , p.i_folder   AS folder_id
         , f.project_name
         , f.name_path
         , h.full_text  AS description
         , NULL::jsonb  AS props
         , p.first_date AS created_at
         , p.first_user AS created_by
         , p.last_date  AS updated_at
         , p.last_user  AS updated_by
         , p.global_id
    FROM odimtd.snp_package p
             JOIN mtd_folder f ON p.i_folder = f.id
             LEFT JOIN odimtd.snp_txt_header h ON p.i_txt_pack = h.i_txt;
    /*--mtd_step-------------------------------------------------------------------------------------------------------------------*/

    INSERT
    INTO odimtd.mtd_package_step( id, package_id, step_name, step_no, ecid, eid, step_type_code, step_type, scen_name
                        , scen_version, created_at, created_by, updated_at, updated_by, global_id)
    SELECT s.i_step                                                                  AS id
         , s.i_package                                                               AS package_id
         , s.step_name
         , s.nno                                                                     AS step_no
         , CASE
               WHEN s.i_mapping IS NOT NULL THEN 224
               WHEN s.i_trt IS NOT NULL THEN 1507-- proc -- 176:trt
               WHEN s.i_var IS NOT NULL THEN 187
               WHEN s.i_txt_action IS NOT NULL THEN 151--scen
               ELSE NULL END                                                         AS ecid
         , COALESCE(s.i_mapping, s.i_trt, s.i_var, s.i_txt_action)                   AS eid
         , s.step_type                                                               AS step_type_code
         , o.value                                                                   AS step_type
         , REPLACE(SUBSTRING(h.full_text FROM E'SCEN_NAME=([^''\s"]+)'), '"', '')    AS scen_name
         , REPLACE(SUBSTRING(h.full_text FROM E'SCEN_VERSION=([^''\s"]+)'), '"', '') AS scen_version
         , s.first_date                                                              AS created_at
         , s.first_user                                                              AS created_by
         , s.last_date                                                               AS updated_at
         , s.last_user                                                               AS updated_by
         , s.global_id
    FROM snp_step s
             LEFT JOIN snp_txt_header h ON s.i_txt_action = h.i_txt
             LEFT JOIN mtds_odi_enums o
                       ON s.step_type = o.code AND o.table_name = 'SNP_STEP' AND o.column_name = 'STEP_TYPE'
    WHERE 1 = 1;

END ;
$$;

alter procedure odimtd.load_mtd1() owner to odimtd_admin;

create or replace procedure odimtd.load_mtd2()
    language plpgsql
as
$$
DECLARE
BEGIN
    SET TIMEZONE = 'Europe/Istanbul';
    TRUNCATE TABLE odimtd.mtd_var;
    TRUNCATE TABLE odimtd.mtd_flat;
    TRUNCATE TABLE odimtd.mtd_km;
    TRUNCATE TABLE odimtd.mtd_procedure;
    TRUNCATE TABLE odimtd.mtd_procedure_step;
    TRUNCATE TABLE odimtd.mtd_mapping;
    TRUNCATE TABLE odimtd.mtd_scen;
    /*--mtd_var-----------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_var ( id
                        , name
                        , qname
                        , logical_schema
                        , project_id
                        , project
                        , description
                        , type
                        , formula
                        , history_behaviour
                        , datatype
                        , value
                        , default_value
                        , created_at
                        , created_by
                        , updated_at
                        , updated_by
                        , global_id)
    SELECT v.i_var                                            AS id
         , v.var_name
         , p.project_name || '.' || v.var_name                AS qname
         , v.lschema_name                                     AS logical_schema
         , v.i_project
         , p.project_name
         , h1.full_text                                       AS description
         , var_type --DEOCDE : P (Project) G (Global)
         , h2.full_text                                       AS formula
         , ind_store                                          AS history_behaviour --DECODE : N (Never tracked),L (Last Value),H (Historize), S (Not Persistant
         , var_datatype
         , COALESCE(def_n::varchar, def_v, def_date::varchar) AS var_value
         , h3.full_text                                       AS default_value
         , v.first_date
         , v.first_user
         , v.last_date
         , v.last_user
         , v.global_id
    FROM snp_var v
             LEFT OUTER JOIN snp_txt_header h1 ON v.i_txt_var = h1.i_txt AND h1.i_txt_orig = 108
             LEFT OUTER JOIN snp_txt_header h2 ON v.i_txt_var_in = h2.i_txt AND h2.i_txt_orig = 128
             LEFT OUTER JOIN snp_txt_header h3 ON v.i_txt_var_val_txt = h3.i_txt AND h3.i_txt_orig = 109
             LEFT JOIN snp_project p ON v.i_project = p.i_project;
    /*--mtd_flat----------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_flat( id
                        , mod_id
                        , submodel_id
                        , org_table_name
                        , table_name
                        , table_type
                        , visible
                        , file_format
                        , file_seperator
                        , txt_enclosing_field
                        , row_seperator
                        , file_first_row
                        , decimal_seperator
                        , olap_type
                        , ws_entity_name
                        , created_at
                        , created_by
                        , updated_at
                        , updated_by
                        , description)
    SELECT i_table          AS id
         , t.i_mod          AS mod_id
         , t.i_sub_model    AS submodel_id
         , t.res_name       AS org_table_name
         , t.table_name     AS table_name
         , t.table_type -- tablo tipi . DECODE (tb.table_type, 'T', 'Table', 'View')
         , t.ind_show::bool AS visible -- gizle göster indisi (1-0)
         , t.file_format -- tablo metin dosyası formatı (F:fixed, D:delimited).
         , t.file_sep_field AS file_seperator-- metin dosyası kolon ayracı.Hexadecimal  (Ön tanımlı olanlar--> Tab :\u0009,Space : \u0020 )
         , t.file_enc_field AS txt_enclosing_field-- metin dosyası metin alanlarını  açıp kapatmak için ullanılan karakter.
         , t.file_sep_row   AS row_seperator-- metin dosyası kayıt ayracı. Hexadecimal CRLF, CR, LF ...,( Öntanımlı olanlar --> MsDos :\u000D\u000A,Unix : \u000A, etc)
         , t.file_first_row -- dosyada kaydın başladığı ilk satır no. Başlık yoksa 0
         , t.file_dec_sep   AS decimal_seperator -- dosya ondalık ayraç
         , t.olap_type -- olap tipi ( DI:Dimension, DH:SCD, FA:Fact). Zorunlu değil
         , t.ws_entity_name -- Data servis olarak deploy edilirse görülecek entity ismi ( datastore ismi gibi)
         , t.first_date
         , t.first_user
         , last_date
         , last_user
         , h.full_text      AS description
    FROM snp_table t
             LEFT JOIN snp_txt_header h ON t.i_txt_desc = h.i_txt
    WHERE 1 = 1
      AND file_format IS NOT NULL;
    /*--mtd_km------------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO mtd_km ( id
                , name
                , qname
                , def_level
                , type
                , shown_order
                , src_lang
                , src_techno
                , trg_lang
                , trg_techno
                , project_id
                , project
                , proc_type
                , base_comp_km_id
                , preinst_flag
                , props
                , description
                , created_at
                , created_by
                , updated_at
                , updated_by
                , global_id)
    SELECT t.i_trt      AS id
         , t.trt_name   AS name
         , CASE WHEN t.i_project IS NULL THEN t.trt_name ELSE p.project_name || '.' || t.trt_name END
         , CASE WHEN t.i_project IS NULL THEN 'Global' ELSE 'Project' END
         , t.trt_type
         , t.ord_folder AS shown_order
         , t.km_src_lang
         , t.km_src_techno
         , t.km_lang
         , t.km_techno
         , t.i_project
         , p.project_name
         , t.proc_type
         , t.i_base_comp_km
         , t.is_seeded::boolean
         , JSON_BUILD_OBJECT('shown_order', t.ord_folder, 'km_type', t.trt_type, 'preinst_flag', t.is_seeded::boolean,
                             'src_techno', km_src_techno, 'src_lang', t.km_src_lang, 'trg_techno', km_techno,
                             'trg_lang', t.km_lang, 'proc_type', t.proc_type, 'base_comp_km_id', t.i_base_comp_km)
         , h.full_text
         , t.first_date
         , t.first_user
         , t.last_date
         , t.last_user
         , t.global_id
    FROM odimtd.snp_trt t
             LEFT JOIN snp_txt_header h ON t.i_txt_trt_txt = h.i_txt AND h.i_txt_orig = 107
             LEFT JOIN snp_project p ON t.i_project = p.i_project
    WHERE trt_type != 'U';
    /*--mtd_procedure-----------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_procedure( id
                             , name
                             , qname
                             , project_id
                             , project
                             , folder_id
                             , path
                             , cleanup_on_error_flag
                             , concurrent_flag
                             , preinst_flag
                             , steps
                             , props
                             , description
                             , created_at
                             , created_by
                             , updated_at
                             , updated_by
                             , global_id)
    WITH
        proptb AS (
                      SELECT l.sql_name         AS step_name
                           , l.ord_trt          AS exec_order
                           , l.def_techno       AS trg_tech
                           , l.def_context_code AS trg_foced_context
                           , l.def_lschema_name AS trg_lschema
                           , h2.full_text       AS trg_code
                           , l.col_techno       AS src_tech
                           , l.col_context_code AS src_forced_context
                           , l.col_lschema_name AS src_lschema
                           , h1.full_text       AS src_code
                      FROM snp_trt t
                               LEFT JOIN odimtd.snp_line_trt l ON t.i_trt = l.i_trt
                               LEFT JOIN odimtd.snp_txt_header h1 ON l.col_i_txt = h1.i_txt AND h1.i_txt_orig = 103
                               LEFT JOIN odimtd.snp_txt_header h2 ON l.def_i_txt = h2.i_txt AND h2.i_txt_orig = 102
                      WHERE t.trt_type = 'U'
                      ORDER BY t.trt_name, l.ord_trt)
    SELECT t.i_trt                                         AS id
         , t.trt_name                                      AS name
         , f.fqname || '.' || t.trt_name
         , f.project_id
         , f.project_name
         , t.i_folder                                      AS folder_id
         , f.fqname -- @todo saçma oldu
         , t.cleanup_on_error::boolean
         , t.is_concurrent::boolean
         , t.is_seeded::boolean
         , (
               SELECT JSON_AGG(ROW_TO_JSON(p))
               FROM (
                        SELECT l.ord_trt          AS exec_order
                             , l.sql_name         AS step_name
                             , l.def_techno       AS trg_tech
                             , l.def_context_code AS trg_foced_context
                             , l.def_lschema_name AS trg_lschema
                             , h2.full_text       AS trg_code
                             , l.col_techno       AS src_tech
                             , l.col_context_code AS src_forced_context
                             , l.col_lschema_name AS src_lschema
                             , h1.full_text       AS src_code
                        FROM snp_trt ti
                                 LEFT JOIN odimtd.snp_line_trt l ON ti.i_trt = l.i_trt
                                 LEFT JOIN odimtd.snp_txt_header h1 ON l.col_i_txt = h1.i_txt AND h1.i_txt_orig = 103
                                 LEFT JOIN odimtd.snp_txt_header h2 ON l.def_i_txt = h2.i_txt AND h2.i_txt_orig = 102
                        WHERE ti.trt_type = 'U'
                          AND t.i_trt = ti.i_trt
                        ORDER BY t.trt_name, l.ord_trt) p) AS steps
         , JSON_BUILD_OBJECT('project_id', f.project_id, 'project_name', f.project_name, 'cleanup_on_error_flag',
                             t.cleanup_on_error::boolean, 'concurrent_flag', is_concurrent::boolean, 'pre_inst_flag',
                             t.is_seeded::boolean)
         , h.full_text
         , t.first_date
         , t.first_user
         , t.last_date
         , t.last_user
         , t.global_id
    FROM snp_trt t
             JOIN odimtd.mtd_folder f ON f.id = t.i_folder
             LEFT JOIN odimtd.snp_txt_header h ON t.i_txt_trt_txt = h.i_txt AND h.i_txt_orig = 107
    WHERE trt_type = 'U';
    /*--mtd_procedure_step------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_procedure_step ( id
                                   , name
                                   , step_name
                                   , exec_order
                                   , trg_tech
                                   , trg_foced_context
                                   , trg_lschema
                                   , trg_code
                                   , src_tech
                                   , src_forced_context
                                   , src_lschema
                                   , src_code
                                   , created_at
                                   , created_by
                                   , updated_at
                                   , updated_by)
    SELECT t.i_trt            AS id
         , t.trt_name         AS name
         , l.sql_name         AS step_name
         , l.ord_trt          AS exec_order
         , l.def_techno       AS trg_tech
         , l.def_context_code AS trg_foced_context
         , l.def_lschema_name AS trg_lschema
         , h2.full_text       AS trg_code
         , l.col_techno       AS src_tech
         , l.col_context_code AS src_forced_context
         , l.col_lschema_name AS src_lschema
         , h1.full_text       AS src_code
         , t.first_date
         , t.first_user
         , t.last_date
         , t.last_user
    FROM snp_trt t
             LEFT JOIN odimtd.snp_line_trt l ON t.i_trt = l.i_trt
             LEFT JOIN odimtd.snp_txt_header h1 ON l.col_i_txt = h1.i_txt AND h1.i_txt_orig = 103
             LEFT JOIN odimtd.snp_txt_header h2 ON l.def_i_txt = h2.i_txt AND h2.i_txt_orig = 102
    WHERE t.trt_type = 'U'
    ORDER BY t.trt_name, l.ord_trt;
    /*--mtd_mapping-------------------------------------------------------------------------------------------------------*/
    INSERT
    INTO odimtd.mtd_mapping( id
                           , name
                           , fqname
                           , folder_id
                           , path
                           , project_id
                           , project_name
                           , reusable_flag
                           , description
                           , created_at
                           , created_by
                           , updated_at
                           , updated_by
                           , global_id)
    SELECT m.i_mapping
         , m.name
         , f.fqname || '.' || m.name
         , m.i_folder
         , f.name_path
         , f.project_id
         , f.project_name
         , m.is_reusable::boolean
         , m.descript
         , m.first_date
         , m.first_user
         , m.last_date
         , m.last_user
         , f.global_id
    FROM odimtd.snp_mapping m
             LEFT JOIN odimtd.mtd_folder f ON m.i_folder = f.id;

    /*--mtd_scen-------------------------------------------------------------------------------------------------------*/

    INSERT
    INTO odimtd.mtd_scen( scen_no, scen_name, scen_version, rel_ecid, rel_eid, scen_folder_id, scen_folder_name
                        , scen_folder_path, props, description, first_date, first_user, last_date, last_user, global_id)
    SELECT s.scen_no
         , s.scen_name
         , s.scen_version
         , CASE
               WHEN s.i_mapping IS NOT NULL THEN 224
               WHEN s.i_trt IS NOT NULL THEN 1507-- proc -- 176:trt
               WHEN s.i_var IS NOT NULL THEN 187
               WHEN s.i_package IS NOT NULL THEN 129
               ELSE NULL END                       AS rel_ecid
         , COALESCE(s.i_mapping, s.i_trt, s.i_var,i_package) AS rel_eid
         , s.i_scen_folder                         AS scen_folder_id
         , f.name                                  AS scen_folder_name
         , f.name_path                             AS scen_folder_path
         , NULL::jsonb                             AS props
         , h.full_text                             AS description
         , s.first_date
         , s.first_user
         , s.last_date
         , s.last_user
         , s.global_id
    FROM odimtd.snp_scen s
             LEFT JOIN odimtd.snp_txt_header h ON s.i_txt_scen = h.i_txt
             LEFT JOIN mtd_scen_folder f ON s.i_scen_folder = f.id;




END ;
$$;

alter procedure odimtd.load_mtd2() owner to odimtd_admin;

create or replace function odimtd.entity_qname_from_id_array(_ref_eup_arr odimtd.euid[]) returns character varying[]
    language sql
as
$$
SELECT array_agg( e.qname ORDER BY r.ordinality )
    FROM mtds_entity e
             JOIN unnest( _ref_eup_arr) WITH ORDINALITY AS r(ecid,eid, ordinality)
                  ON e.eid = r.eid AND e.ecid = r.ecid;
$$;

alter function odimtd.entity_qname_from_id_array(odimtd.euid[]) owner to postgres;

create or replace function odimtd.entity_col_from_id_array(_ref_eup_arr odimtd.euid[], _col character varying) returns character varying[]
    language plpgsql
as
$$
DECLARE
    result varchar[];
BEGIN
    EXECUTE format('SELECT array_agg(e.%I ORDER BY r.ordinality)
        FROM mtds_entity e
        JOIN LATERAL unnest(%L::euid[]) WITH ORDINALITY AS r(ecid, eid, ordinality)
        ON e.eid = r.eid AND e.ecid = r.ecid', _col, _ref_eup_arr) INTO result;
    RETURN result;
END;
$$;

alter function odimtd.entity_col_from_id_array(odimtd.euid[], varchar) owner to postgres;

create or replace function odimtd.list_map_component(_mapp_id integer)
    returns TABLE(tab_ecid integer, tab_eid integer, tab_name character varying, tab_qname character varying, km_path character varying[], source_flag boolean, target_flag boolean, lookup_flag boolean, reusable_flag boolean)
    language plpgsql
as
$$
DECLARE
BEGIN
    RETURN QUERY SELECT r.tab_ecid
                      , r.tab_eid
                      , r.tab_name
                      , r.tab_qname
                      , r.km_path
                      , r.source_flag
                      , r.target_flag
                      , r.lookup_flag
                      , CASE WHEN r.tab_type = 'M' THEN TRUE ELSE FALSE END AS reusable_flag
                 FROM odimtd.mid_comp_tab_rel r
                 WHERE 1 = 1
                   AND mapp_id = _mapp_id;
END;
$$;

alter function odimtd.list_map_component(integer) owner to postgres;

create or replace function odimtd.list_map_col(_mapp_uid odimtd.euid)
    returns TABLE(source_uid odimtd.euid, source_table character varying, source_col character varying, target_uid odimtd.euid, target_table character varying, target_col character varying)
    language plpgsql
as
$$
DECLARE

BEGIN
    RETURN QUERY SELECT d.first_uid       AS source_uid
                      , d.first_cti_qname AS source_table
                      , d.first_qname     AS source_col
                      , d.last_uid        AS target_uid
                      , d.last_cti_qname  AS target_table
                      , d.last_qname      AS target_col
                 FROM odimtd.mtd2_dependency1 d
                 WHERE dep_group_id = 2
                   AND lvl > 0
                   AND d.recsrc_uid_path[1] = _mapp_uid::odimtd.euid
                   AND d.first_recsrc_qname IS NOT NULL;
END;
$$;

alter function odimtd.list_map_col(odimtd.euid) owner to postgres;

create or replace function odimtd.list_tab_usage(_ecid integer, _eid integer)
    returns TABLE(project_name character varying, obj_type character varying, obj_ecid integer, obj_eid integer, name character varying, fqname character varying, path character varying[], km_path character varying[], target_flag boolean, source_flag boolean, lookup_flag boolean)
    language plpgsql
as
$$
DECLARE

BEGIN
    RETURN QUERY
        SELECT m.project_name
                      , 'Mapping'::varchar AS obj_type
                      , 224       AS obj_ecid
                      , m.id      AS obj_eid
                      , m.name
                      , m.fqname
                      , m.path
                      , r.km_path
                      , r.target_flag
                      , r.source_flag
                      , r.lookup_flag
                 FROM mid_comp_tab_rel r
                          JOIN mtd_mapping m ON m.id = r.mapp_id
                 WHERE 1 = 1
                   AND r.tab_ecid = _ecid
                   AND r.tab_eid = _eid;
END;
$$;

alter function odimtd.list_tab_usage(integer, integer) owner to postgres;

create or replace procedure odimtd.load_dep3()
    language plpgsql
as
$$

DECLARE
BEGIN
    SET TIMEZONE = 'Europe/Istanbul';

    TRUNCATE TABLE odimtd.stg_pkg_dep_base;
    /*--stg_pkg_dep_base-------------------------------------------------------------------------------------------------------------------*/

    INSERT
    INTO odimtd.stg_pkg_dep_base (par_ecid, par_eid, step_id, step_name, step_no, step_type, ecid, eid)
    SELECT 129          AS par_ecid
         , s.package_id AS par_eid
         , s.id         AS step_id
         , s.step_name
         , s.step_no
         , s.step_type
         , s.ecid
         , s.eid
    FROM odimtd.mtd_step s
    WHERE s.step_type_code <> 'SE'
    UNION ALL
    SELECT 129          AS par_ecid
         , s.package_id AS par_eid
         , s.id         AS step_id
         , s.step_name
         , s.step_no
         , s.step_type
         , c.rel_ecid
         , c.rel_eid
    FROM odimtd.mtd_step s
             JOIN odimtd.mtd_scen c ON s.scen_name = c.scen_name AND s.scen_version = c.scen_version
    WHERE s.step_type_code = 'SE';

END ;
$$;

alter procedure odimtd.load_dep3() owner to postgres;

create or replace function odimtd.list_tab_to(_ecid integer, _eid integer)
    returns TABLE(lvl integer, uid odimtd.euid, name character varying, recs_uid odimtd.euid, recs_name character varying, uid_pth odimtd.euid[], name_pth character varying[], recs_uid_pth odimtd.euid[], recs_name_pth character varying[], name_tree character varying, sibl_order character varying, is_leaf boolean)
    language plpgsql
as
$$
DECLARE

BEGIN
    RETURN QUERY SELECT r.lvl
                      , r.last_uid
                      , r.last_name
                      , r.last_recs_uid
                      , r.last_recs_name
                      , r.uid_pth
                      , r.name_pth
                      , r.recs_uid_pth
                      , r.recs_name_pth
                      , r.trg_name_tree
                      , r.trg_sibl_order
                      , r.is_trg_leaf
                 FROM mtd2_dependency1 r
                 WHERE 1 = 1
                   AND r.first_uid = ROW (_ecid,_eid)::euid
                 ORDER BY r.trg_sibl_order;
END;
$$;

alter function odimtd.list_tab_to(integer, integer) owner to odimtd_admin;

create or replace function odimtd.list_tab_from(_ecid integer, _eid integer)
    returns TABLE(lvl integer, uid odimtd.euid, name character varying, recs_uid odimtd.euid, recs_name character varying, uid_pth odimtd.euid[], name_pth character varying[], recs_uid_pth odimtd.euid[], recs_name_pth character varying[], name_tree character varying, sibl_order character varying, is_leaf boolean)
    language plpgsql
as
$$
DECLARE

BEGIN
    RETURN QUERY SELECT r.lvl
                      , r.first_uid
                      , r.first_name
                      , r.first_recs_uid
                      , r.first_recs_name
                      , r.uid_pth
                      , r.name_pth
                      , r.recs_uid_pth
                      , r.recs_name_pth
                      , r.src_name_tree
                      , r.src_sibl_order
                      , r.is_src_leaf
                 FROM mtd2_dependency1 r
                 WHERE 1 = 1
                   AND r.last_uid = ROW (_ecid,_eid)::euid
                 ORDER BY r.src_sibl_order;
END;
$$;

alter function odimtd.list_tab_from(integer, integer) owner to odimtd_admin;

create or replace function odimtd.list_map_expression(_mapp_id integer)
    returns TABLE(comp_name character varying, comp_type character varying, attr_name character varying, expression text)
    language plpgsql
as
$$
DECLARE
BEGIN
    RETURN QUERY SELECT mco.name AS comp_name
                      , mco.type_name AS comp_type
                      , matt.name AS attr_name
                      , mex.txt AS expression
                 FROM odimtd.snp_mapping m
                          JOIN odimtd.snp_map_comp mco ON m.i_mapping = mco.i_owner_mapping
                          JOIN odimtd.snp_map_cp mcp ON mcp.i_owner_map_comp = mco.i_map_comp
                          JOIN odimtd.snp_map_attr matt ON matt.i_owner_map_cp = mcp.i_map_cp
                          JOIN odimtd.snp_map_expr mex ON mex.i_owner_map_attr = matt.i_map_attr
                 WHERE 1 = 1
                   AND mex.txt IS NOT NULL
                   AND m.i_mapping = _mapp_id;
END;
$$;

alter function odimtd.list_map_expression(integer) owner to postgres;

