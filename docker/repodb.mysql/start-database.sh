#!/bin/bash

echo "Starting MySQL Server"
/usr/sbin/mysqld &
sleep 5

echo -e "\nCreating Database"
echo "CREATE database repo" | mysql --default-character-set=utf8

echo -e "\nCreating Schema"
mysql--default-character-set=utf8 repo < /mysql_schema/setup.mysql

echo -e "\nCreating DB User repouser"
echo "CREATE USER 'repouser' IDENTIFIED BY ''" | mysql --default-character-set=utf8
echo "GRANT ALL PRIVILEGES ON *.* TO 'repouser'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES" | mysql --default-character-set=utf8
echo "Finished creating user."

tail -f /var/log/mysql/*
