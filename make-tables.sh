#! /bin/sh 

read -r -p "Are you sure? This will nuke the rno_g_met_live database if it exists (type \"DO IT LIVE!\" to confirm): " response
if [[ "$response" == "DO IT LIVE!" ]] 
then 

  echo "DOING IT LIVE! (goodbye data...)"

psql rno_g_met_live << E_O_SQL

DROP TABLE if exists obs; 


CREATE TABLE obs ( 
msg_id SERIAL PRIMARY KEY, 
obs_time  timestamp not null, 
insert_time  timestamp not null, 
wind_speed real not null, 
wind_dir real not null, 
gust_speed real not null, 
pressure real not null, 
temperature real not null, 
dewpoint real not null
); 

CREATE INDEX obs_time_idx on obs using brin(obs_time); 

E_O_SQL

else
  echo "bwak bwak" 
fi 

