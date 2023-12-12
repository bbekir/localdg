
ALTER USER odimtd_admin WITH PASSWORD 'EmreEmre2023'

SELECT 'ALTER TABLE '|| schemaname || '.' || tablename ||' OWNER TO odimtd_admin;'
FROM pg_tables WHERE  schemaname IN ('odimtd')
ORDER BY schemaname, tablename;


SELECT 'ALTER SEQUENCE '|| sequence_schema || '.' || sequence_name ||' OWNER TO odimtd_admin;'
FROM information_schema.sequences WHERE  sequence_schema IN ('odimtd')
ORDER BY sequence_schema, sequence_name;



SELECT 'ALTER VIEW '|| table_schema || '.' || table_name ||' OWNER TO odimtd_admin;'
FROM information_schema.views WHERE  table_schema IN ('odimtd')
ORDER BY table_schema, table_name;

SELECT 'ALTER TABLE '|| oid::regclass::text ||' OWNER TO my_new_owner;'
FROM pg_class WHERE relkind = 'm'
ORDER BY oid;