"""A ready-made demo project: legacy wide tables split, merged and cleaned into a normalised model.

Project "Legacy CRM modernisation"
  1. Catalog              prod_catalog + prod_price ──merge──▶ product
  2. Customers & orders   cust_master ─▶ customers_clean ─┬▶ customer
                                                         ├▶ address  (split)
                                                         ├▶ phone    (unpivot)
                          order_txn ───────────────────── ┴▶ sales_order (joins customers_clean)
sales_order references product, so pipeline 1 must be loaded before pipeline 2. Validation enforces this.
"""
import os

PROJECT_ID = "demo"

PRODUCT_SQL = """-- merge: catalog row + LIST/PROMO price rows -> one product row
SELECT p.prod_code  AS sku,
       p.prod_desc  AS name,
       l.price      AS list_price,
       pr.price     AS promo_price
FROM stg_products p
LEFT JOIN stg_prices l  ON l.prod_code  = p.prod_code AND l.price_type  = 'LIST'
LEFT JOIN stg_prices pr ON pr.prod_code = p.prod_code AND pr.price_type = 'PROMO'"""

CLEAN_SQL = """-- shared cleaning step: customer, address, phone and sales_order all read xf_customers_clean
SELECT c.*,
       lower(trim(c.email)) AS email_norm,
       row_number() OVER (PARTITION BY lower(trim(c.email)) ORDER BY c.cust_id) AS email_rank,
       CASE c.status_cd WHEN 'A' THEN 'active'
                        WHEN 'I' THEN 'inactive'
                        ELSE 'closed' END AS status
FROM stg_customers c"""

CUSTOMER_SQL = """SELECT cust_id   AS id,
       cust_name AS full_name,
       -- legacy allowed duplicate emails; target has UNIQUE(email)
       CASE WHEN email_rank = 1 THEN email_norm
            ELSE 'dup+' || cust_id || '.' || email_norm END AS email,
       status,
       created_on AS created_at
FROM xf_customers_clean"""

ADDRESS_SQL = """-- split: one wide row -> up to two address rows
SELECT cust_id AS customer_id, 'shipping' AS kind,
       ship_line1 AS line1, ship_city AS city, ship_zip AS postal_code
FROM xf_customers_clean WHERE ship_line1 IS NOT NULL
UNION ALL
SELECT cust_id, 'billing', bill_line1, bill_city, bill_zip
FROM xf_customers_clean WHERE bill_line1 IS NOT NULL"""

PHONE_SQL = """-- unpivot repeated phone columns into child rows
SELECT c.cust_id AS customer_id, v.kind, v.number
FROM xf_customers_clean c
CROSS JOIN LATERAL (VALUES ('home',   c.phone_home),
                           ('work',   c.phone_work),
                           ('mobile', c.phone_mobile)) v(kind, number)
WHERE v.number IS NOT NULL"""

ORDER_SQL = """-- only orders whose customer survived cleaning
SELECT o.ord_id    AS id,
       o.cust_id   AS customer_id,
       o.prod_code AS sku,
       o.qty       AS quantity,
       o.amount    AS total,
       o.ord_date  AS ordered_on
FROM stg_orders o
JOIN xf_customers_clean c ON c.cust_id = o.cust_id"""


def _src(id_, name, table, pk, wm, x, y):
    return {"id": id_, "type": "source", "position": {"x": x, "y": y},
            "data": {"name": name, "connection_id": "demo-legacy", "table": table, "pk": pk, "watermark": wm,
                     "batch_size": 5000, "throttle_ms": 50}}


def _xf(id_, name, q, x, y):
    return {"id": id_, "type": "transform", "position": {"x": x, "y": y}, "data": {"name": name, "sql": q}}


def _tgt(id_, table, x, y):
    return {"id": id_, "type": "target", "position": {"x": x, "y": y},
            "data": {"name": table, "table": f"public.{table}", "mode": "upsert", "batch_size": 5000,
                     "throttle_ms": 20}}


def _graph(nodes, wires):
    return {"nodes": nodes, "edges": [{"id": f"e_{a}_{b}", "source": a, "target": b} for a, b in wires]}


def seed(store):
    store.put("connection", {
        "id": "demo-legacy", "name": "Legacy Postgres (demo)", "kind": "postgres", "role": "source",
        "host": os.environ.get("GLYPH_DEMO_SOURCE_HOST", "localhost"),
        "port": int(os.environ.get("GLYPH_DEMO_SOURCE_PORT", 55432)),
        "database": "legacy", "user": "glyph", "password": "glyph"})
    store.put("connection", {
        "id": "demo-modern", "name": "Modern Postgres (demo)", "kind": "postgres", "role": "target",
        "host": os.environ.get("GLYPH_DEMO_TARGET_HOST", "localhost"),
        "port": int(os.environ.get("GLYPH_DEMO_TARGET_PORT", 55433)),
        "database": "modern", "user": "glyph", "password": "glyph"})
    store.delete("pipeline", "demo")  # single-pipeline demo from before projects existed
    project = store.put("project", {
        "id": PROJECT_ID, "name": "Legacy CRM modernisation",
        "description": "Move the legacy CRM (wide cust_master, order_txn, catalog) onto the new normalised model. "
                       "Run Catalog before Customers & orders: sales_order references product."})

    store.put("pipeline", {
        "id": "demo-catalog", "project_id": PROJECT_ID, "name": "1. Catalog", "target_connection_id": "demo-modern",
        "graph": _graph(
            [_src("s_products", "products", "public.prod_catalog", ["prod_code"], "last_modified", 0, 0),
             _src("s_prices", "prices", "public.prod_price", ["prod_code", "price_type"], None, 0, 160),
             _xf("x_product", "product", PRODUCT_SQL, 330, 70),
             _tgt("t_product", "product", 660, 70)],
            [("s_products", "x_product"), ("s_prices", "x_product"), ("x_product", "t_product")])},
        parent=PROJECT_ID)

    store.put("pipeline", {
        "id": "demo-crm", "project_id": PROJECT_ID, "name": "2. Customers & orders",
        "target_connection_id": "demo-modern",
        "graph": _graph(
            [_src("s_customers", "customers", "public.cust_master", ["cust_id"], "last_modified", 0, 160),
             _src("s_orders", "orders", "public.order_txn", ["ord_id"], "last_modified", 0, 480),
             _xf("x_clean", "customers_clean", CLEAN_SQL, 300, 160),
             _xf("x_customer", "customer", CUSTOMER_SQL, 600, 0),
             _xf("x_address", "address", ADDRESS_SQL, 600, 160),
             _xf("x_phone", "phone", PHONE_SQL, 600, 320),
             _xf("x_order", "sales_order", ORDER_SQL, 600, 480),
             _tgt("t_customer", "customer", 900, 0),
             _tgt("t_address", "address", 900, 160),
             _tgt("t_phone", "phone", 900, 320),
             _tgt("t_order", "sales_order", 900, 480)],
            [("s_customers", "x_clean"), ("x_clean", "x_customer"), ("x_clean", "x_address"),
             ("x_clean", "x_phone"), ("x_clean", "x_order"), ("s_orders", "x_order"),
             ("x_customer", "t_customer"), ("x_address", "t_address"), ("x_phone", "t_phone"),
             ("x_order", "t_order")])},
        parent=PROJECT_ID)
    return project
