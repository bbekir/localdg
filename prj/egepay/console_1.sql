
-- drop DATABASE DATABASE egepaydb;

-- CREATE DATABASE egepaydb
WITH
OWNER = postgres
TEMPLATE = template0
ENCODING = 'UTF8'
LC_COLLATE = 'tr_TR.UTF-8'
LC_CTYPE = 'tr_TR.UTF-8'
TABLESPACE = pg_default
CONNECTION LIMIT = -1;
);

create table egepayw.vpos_conn
(
	id bigint generated always as identity,
	vpos_id varchar(100),
	bank_bic varchar(10)
		constraint vpos_fk
			references egepayw.ref_bank (bic),
	url varchar(200),
	usr varchar(300),
	pwd varchar(300),
	tdcode varchar(300),
	comment text,
	status smallint default 1,
	insert_date timestamp default CURRENT_TIMESTAMP
);

alter table egepayw.vpos_conn owner to egepay;

