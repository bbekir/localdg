


SELECT a.i_map_attr,a.name, r.qualified_name,r.qualified_name2 FROM snp_map_attr a left JOIN snp_map_ref r on a.i_map_ref=r.i_map_ref
where 1=1

SELECT * FROM snp_map_ref;