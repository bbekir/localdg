CREATE TABLE oedwh.dim_textdocs AS
    SELECT * FROM pm.textdocs_nestedtab;


DROP TABLE dim_textdocs;

CREATE TABLE oedwh.dim_country AS
    SELECT c.country_id, c.country_name, c.region_id, r.region_name, sysdate AS etl_date
    FROM hr.countries c
             JOIN hr.regions r ON c.region_id = r.region_id

SELECT * FROM hr.regions;

SELECT * FROM hr.locations
;

CREATE TABLE oedwh.fct_supplier_summary AS
    SELECT to_char( o.order_date, 'yyyymm' ) AS sales_period
         , o.sales_rep_id
         , i.supplier_id
         , sum( o.order_total ) AS totalamt
         , sum( o.order_total ) - sum( i.list_price ) AS discnt
         , sysdate AS etl_date
    FROM orders o, order_items a, product_information i
    WHERE o.order_id = a.order_id
      AND a.product_id = i.product_id
    GROUP BY to_char( o.order_date, 'yyyymm' )
           , o.sales_rep_id
           , i.supplier_id
;

CREATE TABLE oedwh.orders AS
    SELECT * FROM orders
;

CREATE TABLE oedwh.customer AS
SELECT c. *
         , 1 AS rec_status
         , trunc( sysdate, 'DD' ) AS create_date
         , cast( 'oeusr' AS varchar2(100) ) AS created_by
         from  v_customers c

drop view v_customers;

CREATE VIEW v_customers AS
    SELECT c.customer_id
         , cust_first_name
         , cust_last_name
         , nls_language
         , nls_territory
         , c.cust_address.city as cust_city
         , c.cust_address.country_id as cust_country
         , c.cust_address.state_province as cust_province
         , c.cust_address.street_address as cust_street_ddress
         , credit_limit
         , cust_email
         , account_mgr_id
         , date_of_birth
         , marital_status
         , gender
         , income_level
    FROM customers c;



SELECT c.cust_address.city FROM customers c;