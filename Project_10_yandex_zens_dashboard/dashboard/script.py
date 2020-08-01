#!/usr/bin/python
# -*- coding: utf-8 -*-

import os

db = 'zen'

commands = [
'sudo apt update',
'sudo apt install python3-pip',
'sudo apt install postgresql postgresql-contrib',
'sudo apt-get install python3-psycopg2',
'sudo pip3 install pandas',
'sudo pip3 install numpy',
'sudo pip3 install dash',
'sudo pip3 install dash_core_components',
'sudo pip3 install dash_html_components',
'sudo pip3 install plotly',
'sudo pip3 install sqlalchemy',
'sudo service postgresql start',

f'sudo -u postgres dropdb --if-exists {db}',
f'''sudo -u postgres createdb {db} --encoding='utf-8' ''',
f'''sudo -u postgres psql -d {db} -c "CREATE USER my_user WITH ENCRYPTED PASSWORD 'my_user_password';" ''',
f'''sudo -u postgres psql -d {db} -c 'GRANT ALL PRIVILEGES ON DATABASE {db} TO my_user;' ''',			                     

f'''sudo -u postgres psql -d {db} -c 'create table dash_visits (record_id serial primary key,
									item_topic varchar(128),
									source_topic varchar(128),
									age_segment varchar(128),
									dt timestamp,
									visits int);' ''',
f'''sudo -u postgres psql -d {db} -c 'grant all privileges on table dash_visits to my_user;' ''',
f'''sudo -u postgres psql -d {db} -c 'grant usage, select on sequence dash_visits_record_id_seq to my_user;' ''',

f'''sudo -u postgres psql -d {db} -c 'create table dash_engagement (record_id serial primary key,
		                             dt timestamp,
		                             item_topic varchar(128),
		                             event varchar(128),
		                             age_segment varchar(128),
		                             unique_users bigint);' ''',
f'''sudo -u postgres psql -d {db} -c 'grant all privileges on table dash_engagement to my_user;' ''',
f'''sudo -u postgres psql -d {db} -c 'grant usage, select on sequence dash_engagement_record_id_seq to my_user;' ''',
]

for command in commands:
	print(f'\n-----Begin: {command}\n')
	os.system(command)

print('Создана пустая база данных zen')
print('Работа скрипта закончена')