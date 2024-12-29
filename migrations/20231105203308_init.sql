-- Add migration script here
CREATE TABLE IF NOT EXISTS general_conf (
    id SERIAL PRIMARY KEY,
    default_simulation_forecast_days SMALLINT NOT NULL CHECK(default_simulation_forecast_days >= 0),
    default_scenario_random_range_factor DECIMAL(3,2) NOT NULL,
    default_maximum_historic_days SMALLINT NOT NULL CHECK(default_maximum_historic_days >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS product_props (
    id UUID PRIMARY KEY,
    simulation_forecast_days SMALLINT CHECK(simulation_forecast_days >= 0),
    scenario_random_range_factor DECIMAL(3,2),
    maximum_historic_days SMALLINT CHECK(maximum_historic_days >= 0),
    maximum_quantity INTEGER CHECK(maximum_quantity >= 0) NOT NULL,
    minimum_quantity INTEGER CHECK(minimum_quantity >= 0) DEFAULT 0 NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS product_simulation_summary (
    id SERIAL,
    product_id UUID REFERENCES product_props (id) NOT NULL,
    probability_losses_by_missing DECIMAL(3,3) NOT NULL,
    probability_losses_by_nospace DECIMAL(3,3) NOT NULL,
    probability_losses_by_expirat DECIMAL(3,3) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    first_date_with_losses DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, product_id)
);

CREATE TABLE IF NOT EXISTS product_simulation_summary_by_day (
    product_simulation_summary_id INTEGER NOT NULL,
    date DATE NOT NULL,
    probability_losses_by_missing DECIMAL(3,3) NOT NULL,
    probability_losses_by_nospace DECIMAL(3,3) NOT NULL,
    probability_losses_by_expirat DECIMAL(3,3) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_simulation_summary_id, date)
);

CREATE TABLE IF NOT EXISTS product_batch (
    id SERIAL,
    product_id UUID REFERENCES product_props (id) NOT NULL,
    entry_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deadline_date TIMESTAMPTZ NOT NULL,
    finished_date TIMESTAMPTZ,
    is_finished BOOLEAN NOT NULL GENERATED ALWAYS AS (finished_date IS NOT NULL) STORED,
    quantity INTEGER NOT NULL CHECK (quantity >= 0) DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, product_id)
);
CREATE INDEX IF NOT EXISTS idx_is_finished ON product_batch (is_finished);

CREATE TABLE IF NOT EXISTS product_mov_hist_no_part (
    product_id UUID REFERENCES product_props (id),
    entry_qty INTEGER NOT NULL DEFAULT 0,
    withdrawal_qty INTEGER NOT NULL DEFAULT 0,
    mov_date DATE NOT NULL DEFAULT NOW(),
    week_of_year SMALLINT CHECK(week_of_year >= 1 AND week_of_year <= 53) ,
    day_of_week SMALLINT CHECK(day_of_week >= 0 AND day_of_week <= 6) GENERATED ALWAYS AS (date_part('dow', mov_date)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, mov_date, week_of_year)
);

CREATE TABLE IF NOT EXISTS product_mov_hist (
    product_id UUID REFERENCES product_props (id),
    entry_qty INTEGER NOT NULL DEFAULT 0,
    withdrawal_qty INTEGER NOT NULL DEFAULT 0,
    mov_date DATE NOT NULL DEFAULT NOW(),
    -- A generated column cannot be part of a partition key (https://www.postgresql.org/docs/current/ddl-generated-columns.html)
    week_of_year SMALLINT NOT NULL CHECK(week_of_year >= 1 AND week_of_year <= 53) ,
    day_of_week SMALLINT CHECK(day_of_week >= 0 AND day_of_week <= 6) GENERATED ALWAYS AS (date_part('dow', mov_date)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, mov_date, week_of_year)
) partition by range(week_of_year);
CREATE TABLE product_mov_hist_1 PARTITION OF product_mov_hist FOR VALUES FROM (1) TO (14);
CREATE TABLE product_mov_hist_2 PARTITION OF product_mov_hist FOR VALUES FROM (14) TO (27);
CREATE TABLE product_mov_hist_3 PARTITION OF product_mov_hist FOR VALUES FROM (27) TO (40);
-- Each range's bounds are understood as being inclusive at the lower end and exclusive at the upper end.
-- For example, if one partition's range is from 1 to 10 , and the next one's range is from 10 to 20 , then value 10 belongs to the second partition not the first.
-- https://www.postgresql.org/docs/current/ddl-partitioning.html
CREATE TABLE product_mov_hist_4 PARTITION OF product_mov_hist FOR VALUES FROM (40) TO (54);
