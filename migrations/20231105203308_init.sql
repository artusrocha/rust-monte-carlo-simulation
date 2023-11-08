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
    id SERIAL PRIMARY KEY,
    item_id INTEGER REFERENCES item (id),
    entry_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deadline_date TIMESTAMPTZ NOT NULL,
    finished_date TIMESTAMPTZ,
    is_finished BOOLEAN GENERATED ALWAYS AS (finished_date IS NOT NULL) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS item_movement_historic (
    id SERIAL PRIMARY KEY,
    item_id INTEGER REFERENCES item (id),
    entry_quantity INTEGER NOT NULL DEFAULT 0,
    withdrawal_quantity INTEGER NOT NULL DEFAULT 0,
    movement_date DATE NOT NULL DEFAULT NOW(),
    movement_week_of_year INTEGER GENERATED ALWAYS AS (date_part('week', movement_date)) STORED,
    movement_day_of_week INTEGER GENERATED ALWAYS AS (date_part('dow', movement_date)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);



