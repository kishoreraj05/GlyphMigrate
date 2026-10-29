-- Re-modelled target: normalised, typed, constrained.

CREATE TABLE customer (
    id          bigint PRIMARY KEY,
    full_name   text NOT NULL,
    email       text NOT NULL UNIQUE,
    status      text NOT NULL CHECK (status IN ('active', 'inactive', 'closed')),
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE address (
    customer_id bigint NOT NULL REFERENCES customer(id),
    kind        text   NOT NULL CHECK (kind IN ('shipping', 'billing')),
    line1       text   NOT NULL,
    city        text,
    postal_code text,
    PRIMARY KEY (customer_id, kind)
);

CREATE TABLE phone (
    customer_id bigint NOT NULL REFERENCES customer(id),
    kind        text   NOT NULL,
    number      text   NOT NULL,
    PRIMARY KEY (customer_id, kind)
);

CREATE TABLE product (
    sku         text PRIMARY KEY,
    name        text NOT NULL,
    list_price  numeric(12,2) NOT NULL,
    promo_price numeric(12,2)
);

CREATE TABLE sales_order (
    id          bigint PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES customer(id),
    sku         text   NOT NULL REFERENCES product(sku),
    quantity    integer NOT NULL CHECK (quantity > 0),
    total       numeric(12,2) NOT NULL,
    ordered_on  date NOT NULL
);
