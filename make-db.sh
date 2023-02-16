#! /bin/sh

psql << E_O_SQL

CREATE DATABASE rno_g_met_live; 
CREATE USER met; 
GRANT ALL PRIVILEGES ON DATABASE rno_g_met_live TO met; 

E_O_SQL

