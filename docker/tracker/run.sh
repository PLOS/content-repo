#!/bin/sh
sleep 5
echo "db_dsn = DBI:mysql:mogilefs:host=mogdb" >> mogilefsd.conf
mogdbsetup --yes --dbhost=mogdb --dbname=mogilefs --dbuser=root --dbpassword=''
useradd -g nogroup -s /bin/false -d /var/mogdata mogilefs
sudo -u mogilefs mogilefsd -c mogilefsd.conf
echo mogadm host add stored --ip=node --port=7500 --status=alive
mogadm host add stored --ip=node --port=7500 --status=alive
mogadm device add stored 1
mogadm check
mogadm domain add maindomain
mogadm domain list
tail -f /dev/null
