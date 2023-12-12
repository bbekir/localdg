WITH prod AS (
SELECT 1 AS sales_type, 'sdfsd' AS product_group_cd, 25 AS product_price
UNION ALL SELECT 2, 'ssdfd', 35)
SELECT *
FROM egepayw.lim_product_group_limit l, prod p
WHERE 1 = 1
  AND eff_start_date <= current_timestamp
  AND eff_end_date > current_timestamp
  AND l.sales_type = p.sales_type
  AND l.product_group_code = p.product_group_cd
  AND base_value1 <= p.product_price
  AND base_value2 > p.product_price;


bireysel : sales_tye=2
televizyon : fiyatı 3500  ve üzeri olan tel product_group_id =23

UPDATE mrc_plan
SET eff_end_date = ( SELECT eff_start_date from :table WHERE id= :rec_id)
WHERE 1 = 1
  AND id = (SELECT DISTINCT ON (id) id FROM :table WHERE entity_id = :eid ORDER BY eff_end_date DESC);

create table mrc_param
(
	id bigint generated always as identity
		constraint mrc_param	primary key,
	mercant_id bigint,
	code varchar(20),
	value varchar(300),
	shown_order smallint,
	status smallint default 1,
	insert_date timestamp default CURRENT_TIMESTAMP,
	last_updated_by varchar(30) default CURRENT_USER
);

create table mrc_base_param
(
	id bigint generated always as identity
		constraint mrc_param	primary key,
	code varchar(20),
	def_value varchar(300),
	def_shown_order smallint,
	status smallint default 1,
	insert_date timestamp default CURRENT_TIMESTAMP,
	last_updated_by varchar(30) default CURRENT_USER
);




select table_schema,
       table_name,
       column_name,
       collation_name
from information_schema.columns
where collation_name is not null
order by table_schema,
         table_name,
         ordinal_position;

         SELECT datcollate AS collation
FROM pg_database
WHERE datname = current_database();

alter table egepayw.vpos_conn alter column bank_id type bigint using bank_id::bigint;


GRANT CONNECT ON DATABASE work TO egepay;





drop TABLE egepayw.mrc_order;

create table egepayw.mrc_order
(
	id bigint not null
		constraint pk_mrc_order_id
			primary key,
	order_amount decimal,
	commission_amount decimal,
	discount_amount decimal,
	shipping_cost decimal,
	currency_cd varchar(10),
	customer_ip varchar(30),
	customer_user_agent varchar(100),
	webhook_url varchar(300),
	customer_id bigint,
	customer_trid varchar(20),
	customer_name varchar(100),
	customer_email varchar(100),
	customer_phone varchar(30),
	customer_birthday date,
	billing_address_code varchar(100),
	billing_address_line varchar(300),
	billing_city varchar(100),
	billing_country varchar(150),
	billing_postal_code varchar(20),
	billing_phone varchar(30),
	shipping_address_code varchar(100),
	shipping_address_line varchar(300),
	shipping_city varchar(100),
	shipping_country varchar(150),
	shipping_postal_code varchar(30),
	shipping_phone varchar(30),
	merchant_id bigint
		constraint fk_mrc_order_mrc_merchant
			references egepayw.mrc_merchant
);

alter table egepayw.mrc_order owner to postgres;



create table if not exists egepayw.auth_group
(
	id bigint generated always as identity
		constraint pk_auth_user_group_id
			primary key,
	name varchar(100),
	description varchar(500),
	client_id varchar(100)
		constraint fk_auth_group_auth_client
			references egepayw.auth_client,
	status smallint,
	insert_date timestamp default CURRENT_TIMESTAMP
);

alter table egepayw.auth_group owner to postgres;





 SELECT SET_CONFIG( 'lc_time', 'fr_FR', TRUE );

SHOW client_encoding;

SET CLIENT_ENCODING TO 'WIN1251';


/*
** card_bin ve bağlı tablolar
    * card_bin yüklemeyi kolaylaştırmak için tmp_card_bin
    * ref_card_brand -- visa electron maestro.. kritik değil. referans
    * ref_bank -- bankalar. bic kodu ile ilişkili
    * ref_card_network . kart ağı ( bonus vs)
    * ref_country.
--yaklaşım 1: visa maestor vs den alınabilen gormatta bir bin tablosu bulunur . bunlarda kart bilgisi olmaz. bu veri ile bankalardan alınan bin bilgileri birleştirilir
*/
-- SELECT UPPER(t.bank)
-- FROM tmp_bin t;
-- ,  ref_bank b
-- where t.bank=b.name


call "update " table_name "set status=4 where id =" rec_id ;











