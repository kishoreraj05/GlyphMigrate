-- Legacy data model: wide, denormalised tables, weak indexing.

CREATE TABLE cust_master (
    cust_id        integer PRIMARY KEY,
    cust_name      varchar(200),
    email          varchar(200),
    phone_home     varchar(30),
    phone_work     varchar(30),
    phone_mobile   varchar(30),
    ship_line1     varchar(200),
    ship_city      varchar(100),
    ship_zip       varchar(20),
    bill_line1     varchar(200),
    bill_city      varchar(100),
    bill_zip       varchar(20),
    status_cd      char(1),          -- A = active, I = inactive, X = closed
    created_on     timestamp,
    last_modified  timestamp          -- no index: incremental pulls will seq-scan
);

CREATE TABLE prod_catalog (
    prod_code  varchar(20) PRIMARY KEY,
    prod_desc  varchar(200),
    last_modified timestamp
);

CREATE TABLE prod_price (
    prod_code  varchar(20) REFERENCES prod_catalog,
    price_type varchar(10),          -- LIST / PROMO
    price      numeric(12,2),
    PRIMARY KEY (prod_code, price_type)
);

CREATE TABLE order_txn (
    ord_id        bigint PRIMARY KEY,
    cust_id       integer,
    prod_code     varchar(20),
    qty           integer,
    amount        numeric(12,2),
    ord_date      date,
    last_modified timestamp
);

INSERT INTO cust_master
SELECT g,
       'Customer ' || g,
       CASE WHEN g % 997 = 0 THEN 'dup@example.com' ELSE 'cust' || g || '@example.com' END,
       CASE WHEN g % 3 = 0 THEN '555-01' || lpad(g::text, 6, '0') END,
       CASE WHEN g % 5 = 0 THEN '555-02' || lpad(g::text, 6, '0') END,
       '555-03' || lpad(g::text, 6, '0'),
       g || ' Main St', (ARRAY['Pune','Austin','Berlin','Leeds'])[1 + g % 4], lpad((g % 99999)::text, 5, '0'),
       CASE WHEN g % 4 = 0 THEN g || ' Billing Ave' END,
       CASE WHEN g % 4 = 0 THEN 'Austin' END,
       CASE WHEN g % 4 = 0 THEN '73301' END,
       (ARRAY['A','A','A','I','X'])[1 + g % 5],
       now() - (g || ' minutes')::interval * 30,
       now() - (g || ' minutes')::interval
FROM generate_series(1, 20000) g;

INSERT INTO prod_catalog
SELECT 'P' || lpad(g::text, 4, '0'), 'Product ' || g, now() - (g || ' hours')::interval
FROM generate_series(1, 200) g;

INSERT INTO prod_price
SELECT prod_code, 'LIST', (10 + random() * 490)::numeric(12,2) FROM prod_catalog;
INSERT INTO prod_price
SELECT prod_code, 'PROMO', (5 + random() * 100)::numeric(12,2) FROM prod_catalog WHERE right(prod_code, 1) IN ('1','5');

INSERT INTO order_txn
SELECT g,
       1 + (g * 7919) % 20000,
       'P' || lpad((1 + g % 200)::text, 4, '0'),
       1 + g % 9,
       ((1 + g % 9) * (10 + g % 300))::numeric(12,2),
       (now() - (g % 1500 || ' days')::interval)::date,
       now() - (g || ' seconds')::interval
FROM generate_series(1, 100000) g;

ANALYZE;
