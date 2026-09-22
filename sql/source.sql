CREATE SCHEMA IF NOT EXISTS healthcare;
CREATE SCHEMA IF NOT EXISTS project_meta;

CREATE TABLE IF NOT EXISTS healthcare.patients (
    patient_id uuid PRIMARY KEY,
    birth_date date NOT NULL,
    death_date date,
    gender varchar(256) NOT NULL,
    city varchar(256),
    state varchar(256),
    postal_code varchar(256),
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS healthcare.encounters (
    encounter_id uuid PRIMARY KEY,
    patient_id uuid NOT NULL REFERENCES healthcare.patients,
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    encounter_class varchar(256) NOT NULL,
    total_claim_cost numeric(14,2) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

CREATE TABLE IF NOT EXISTS healthcare.claims (
    claim_id uuid PRIMARY KEY,
    patient_id uuid NOT NULL REFERENCES healthcare.patients,
    encounter_id uuid REFERENCES healthcare.encounters,
    status varchar(256) NOT NULL,
    outstanding_primary numeric(14,2),
    outstanding_secondary numeric(14,2),
    outstanding_patient numeric(14,2),
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS healthcare.claim_transactions (
    transaction_id uuid PRIMARY KEY,
    claim_id uuid NOT NULL REFERENCES healthcare.claims,
    patient_id uuid NOT NULL REFERENCES healthcare.patients,
    transaction_type varchar(256) NOT NULL,
    amount numeric(14,2),
    posted_at timestamptz NOT NULL,
    payments numeric(14,2),
    adjustments numeric(14,2),
    transfers numeric(14,2),
    outstanding numeric(14,2),
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- IDs are stable. These timestamps describe this operational database's writes,
-- not when the historical Synthea export originally changed. CDC reads WAL.
CREATE OR REPLACE FUNCTION healthcare.touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['patients','encounters','claims','claim_transactions'] LOOP
        EXECUTE format('ALTER TABLE healthcare.%I REPLICA IDENTITY FULL', table_name);
        EXECUTE format('CREATE OR REPLACE TRIGGER touch_updated_at BEFORE UPDATE ON healthcare.%I FOR EACH ROW EXECUTE FUNCTION healthcare.touch_updated_at()', table_name);
    END LOOP;
END;
$$;

CREATE TABLE IF NOT EXISTS project_meta.seed_runs (
    archive_sha256 text PRIMARY KEY,
    loaded_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    row_counts jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS project_meta.simulation_steps (
    scenario text NOT NULL,
    phase text NOT NULL,
    completed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (scenario, phase)
);
