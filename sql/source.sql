CREATE SCHEMA IF NOT EXISTS ecommerce;
CREATE SCHEMA IF NOT EXISTS project_meta;

CREATE TABLE IF NOT EXISTS ecommerce.customers (
    customer_id varchar(32) PRIMARY KEY,
    customer_unique_id varchar(32) NOT NULL,
    postal_code varchar(5), city varchar(256), state varchar(2),
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ecommerce.orders (
    order_id varchar(32) PRIMARY KEY,
    customer_id varchar(32) NOT NULL REFERENCES ecommerce.customers,
    status varchar(32) NOT NULL,
    purchased_at timestamp NOT NULL, approved_at timestamp,
    carrier_delivered_at timestamp, customer_delivered_at timestamp,
    estimated_delivery_at timestamp,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    order_item_key varchar(64) PRIMARY KEY,
    order_id varchar(32) NOT NULL REFERENCES ecommerce.orders,
    order_item_id integer NOT NULL,
    product_id varchar(32) NOT NULL, seller_id varchar(32) NOT NULL,
    shipping_limit_at timestamp NOT NULL,
    price numeric(14,2) NOT NULL, freight_value numeric(14,2) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (order_id, order_item_id),
    CHECK (order_item_key = order_id || ':' || order_item_id::text)
);
CREATE TABLE IF NOT EXISTS ecommerce.order_payments (
    payment_key varchar(64) PRIMARY KEY,
    order_id varchar(32) NOT NULL REFERENCES ecommerce.orders,
    payment_sequential integer NOT NULL, payment_type varchar(32) NOT NULL,
    payment_installments integer NOT NULL, payment_value numeric(14,2) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (order_id, payment_sequential),
    CHECK (payment_key = order_id || ':' || payment_sequential::text)
);

-- Business timestamps retain the export's timezone-less values. updated_at is
-- this database's audit time, not an extraction watermark. DMS reads WAL.
CREATE OR REPLACE FUNCTION ecommerce.touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;
DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['customers','orders','order_items','order_payments'] LOOP
        EXECUTE format('ALTER TABLE ecommerce.%I REPLICA IDENTITY FULL', table_name);
        EXECUTE format('CREATE OR REPLACE TRIGGER touch_updated_at BEFORE UPDATE ON ecommerce.%I FOR EACH ROW EXECUTE FUNCTION ecommerce.touch_updated_at()', table_name);
    END LOOP;
END;
$$;
CREATE TABLE IF NOT EXISTS project_meta.seed_runs (
    seed_id text PRIMARY KEY,
    loaded_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    row_counts jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS project_meta.simulation_steps (
    scenario text NOT NULL, phase text NOT NULL,
    completed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (scenario, phase)
);
