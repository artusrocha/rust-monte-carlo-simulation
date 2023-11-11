-- Add migration script here
CREATE TABLE IF NOT EXISTS general_conf (
    id SERIAL PRIMARY KEY,
    simulation_length INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS item (
    id SERIAL PRIMARY KEY,
    default_time_limit INTEGER NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS item_batch (
    id SERIAL,
    item_id INTEGER REFERENCES item (id),
    entry_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deadline_date TIMESTAMPTZ NOT NULL,
    finished_date TIMESTAMPTZ,
    is_finished BOOLEAN NOT NULL GENERATED ALWAYS AS (finished_date IS NOT NULL) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_is_finished ON item_batch (is_finished);

CREATE TABLE IF NOT EXISTS item_mov_hist_no_part (
    item_id INTEGER REFERENCES item (id),
    entry_qty INTEGER NOT NULL DEFAULT 0,
    withdrawal_qty INTEGER NOT NULL DEFAULT 0,
    mov_date DATE NOT NULL DEFAULT NOW(),
    week_of_year SMALLINT CHECK(week_of_year >= 1 AND week_of_year <= 53) ,
    day_of_week SMALLINT CHECK(day_of_week >= 0 AND day_of_week <= 6) GENERATED ALWAYS AS (date_part('dow', mov_date)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (item_id, mov_date, week_of_year)
);

CREATE TABLE IF NOT EXISTS item_mov_hist (
    item_id INTEGER REFERENCES item (id),
    entry_qty NUMERIC NOT NULL DEFAULT 0,
    withdrawal_qty NUMERIC NOT NULL DEFAULT 0,
    mov_date DATE NOT NULL DEFAULT NOW(),
    week_of_year SMALLINT CHECK(week_of_year >= 1 AND week_of_year <= 53) ,
    day_of_week SMALLINT CHECK(day_of_week >= 0 AND day_of_week <= 6) GENERATED ALWAYS AS (date_part('dow', mov_date)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (item_id, mov_date, week_of_year)
) partition by range(week_of_year);
CREATE TABLE item_mov_hist_1 PARTITION OF item_mov_hist FOR VALUES FROM (1) TO (14);
CREATE TABLE item_mov_hist_2 PARTITION OF item_mov_hist FOR VALUES FROM (14) TO (27);
CREATE TABLE item_mov_hist_3 PARTITION OF item_mov_hist FOR VALUES FROM (27) TO (40);
CREATE TABLE item_mov_hist_4 PARTITION OF item_mov_hist FOR VALUES FROM (40) TO (54);
