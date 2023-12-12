create table bolt.act_address
(
	id bigint generated always as identity (maxvalue 2147483647)
		constraint act_address_pk
			primary key,
	account_id bigint not null
		constraint act_address_act_account_id_fk
			references bolt.act_account,
	shown_order smallint,
	name varchar(50),
	province varchar(100) not null,
	district varchar(100) not null,
	neighbourhood varchar(100),
	postal_code varchar(30),
	address varchar(500) not null,
	addres_desc varchar(500),
	geo_loc_array double precision[],
	primary_contact_position varchar(100),
	primary_contact_name varchar(100),
	primary_contact_surname varchar(100),
	phone jsonb,
	email jsonb,
	primary_flag smallint,
	comment text,
	info jsonb,
	status smallint default 1 not null,
	v_no smallint default 1 not null,
	insert_date timestamp default CURRENT_TIMESTAMP not null,
	last_update_date timestamp default CURRENT_TIMESTAMP not null,
	geo_loc jsonb,
	contactimage_full_path varchar(500),
	addressimage_full_path varchar(500)
);

alter table bolt.act_address owner to postgres;

create table bolt.act_receiver
(
	id bigint generated always as identity (maxvalue 2147483647)
		constraint act_receiver_pk
			primary key,
	account_id bigint
		constraint act_receiver_act_account_id_fk
			references bolt.act_account,
	gender char,
	salutation varchar(50),
	name varchar(50) not null,
	surname varchar(10) not null,
	unq_id_num varchar(11),
	unq_id_type smallint default 1,
	business_flag smallint,
	phone jsonb,
	email jsonb,
	app_user_id bigint,
	info jsonb,
	status smallint default 1 not null,
	insert_date timestamp default CURRENT_TIMESTAMP not null,
	last_update_date timestamp default CURRENT_TIMESTAMP not null
);

alter table bolt.act_receiver owner to postgres;

alter table bolt.act_address drop constraint act_address_act_account_id_fk;