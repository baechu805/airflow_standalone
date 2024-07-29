#!/bin/bash

CSV_PATH=$1

user="root"
password="qwer123"
database="history_db"

mysql --local-infile=1  -u"$user" -p"$password" "$database" <<EOF
-- LOAD DATA LOCAL INFILE '/var/lib/mysql-files/csv.csv'
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DEL_DT}';

LOAD DATA LOCAL INFILE '$CSV_PATH'
INTO TABLE history_db.tmp_cmd_usage
CHARACTER SET latin1
FIELDS TERMINATED BY ','
ESCAPED BY '\b'
ENCLOSED BY '^'
LINES TERMINATED BY '\n';
EOF
