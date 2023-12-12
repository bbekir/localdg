SET search_path = "odimtd";


SELECT foreign_table_name, array_agg( table_name )
                    FROM v_raw_dependency
                    GROUP BY foreign_table_name;


-- kolon listesini güncelleme
WITH tabcols AS (SELECT table_name, string_agg( column_name, ',' ORDER BY ordinal_position ) AS cols
             FROM information_schema.columns
             WHERE table_schema = 'odimtd'
             GROUP BY table_name)
UPDATE meta_conf m
SET fields= (SELECT cols FROM tabcols t WHERE t.table_name = m.trg_table);
-- tabi liste güncellemek yetmez. src_Select i de ..
--snp tablolarındaki kolonlar neyse oracle'dan aynen besleneceklerinden src_select net ..
UPDATE meta_conf SET src_select=fields WHERE trg_table LIKE 'snp_%';

--topluca kolonların veri türünü değiştirme
SELECT 'alter table ' || c.table_name || ' alter column ' || column_name || ' type int using ' || c.column_name ||  '::int;'
FROM information_schema.columns c
WHERE table_schema = 'odimtd'
  AND data_type = 'numeric'
  AND numeric_scale = 0;



--fk bilgilerinden yararlanarak bağımlılar ve bağlılar. fk lar eksik ..
WITH dep AS (SELECT foreign_table_name, array_agg( DISTINCT table_name ORDER BY table_name ) AS deptoit
             FROM v_raw_dependency
             GROUP BY foreign_table_name)
UPDATE meta_conf m
SET todep= (SELECT todep FROM dep d WHERE m.trg_table = d.foreign_table_name)
WHERE trg_table LIKE 'snp%';


WITH dep AS (SELECT table_name, array_agg( DISTINCT foreign_table_name ORDER BY foreign_table_name ) AS dependency
             FROM v_raw_dependency
             GROUP BY table_name)
UPDATE meta_conf m
SET depto= (SELECT depto FROM dep d WHERE m.trg_table = d.table_name)
WHERE trg_table LIKE 'snp%';




DELETE FROM meta_conf
where trg_table not in (
                       SELECT table_name
                       FROM information_schema.tables
                       WHERE table_schema = 'odimtd' AND table_name LIKE 'snp%')


