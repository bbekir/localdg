

SELECT SUBSTRING(    'OdiStartScen "-SCEN_NAME=SCEN1"''" -SCEN_VERSION=001" "-SYNC_MODE=2" "-VAR1=3" '
               FROM 'SCEN_NAME=([^''\s"]+)')



SELECT (REGEXP_MATCHES(
        'OdiStartScen "-SCEN_NAME=SCEN2'' -SCEN_VERSION=001" "-SYNC_MODE=2" "-var2=2" ',
        E'-SCEN_NAME=([^\s\'"]+)', 'g')::varchar[])[1]



/*muhtemel bug. regexp_match kullanmak left i inner a çeviriyor. Left join klullanabilmek için aşağıdaki gibi kullanmak gerekiyor*/
SELECT s.i_step, s.i_package, s.step_name, s.step_type, h.full_text, m.scen_ver   --- scen_ver varchar[] türünde
FROM snp_step s
         LEFT JOIN snp_txt_header h ON s.i_txt_action = h.i_txt
         LEFT JOIN LATERAL ( SELECT REGEXP_MATCHES(h.full_text, E'-SCEN_VERSION=([^\s\'"]+)', 'g') AS scen_ver ) AS m
                   ON TRUE
WHERE 1 = 1