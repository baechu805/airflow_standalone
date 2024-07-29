#!/bin/bash

DT=$1

user="root"
password="qwer123"
database="history_db"

mysql --local-infile=1  -u"$user" -p"$password" "$database" <<EOF
DELETE FROM  history_db.cmd_usage  WHERE dt = '$DT';

INSERT into cmd_usage 
SELECT 
	CASE WHEN dt like '%-%-%' 
		THEN STR_TO_DATE(dt, '%Y-%m-%d') 
		ELSE STR_TO_DATE('1970-01-01', '%Y-%m-%d') 
	END AS dt,
	command,
	CASE WHEN cnt REGEXP '[0-9]+$' 
		THEN CAST(cnt AS UNSIGNED) 
		ELSE -1
	END AS cnt, 
 	'${DT}'AS tmp_dt
FROM history_db.tmp_cmd_usage 
WHERE dt = '${DT}'
EOF

echo 'DONE'
