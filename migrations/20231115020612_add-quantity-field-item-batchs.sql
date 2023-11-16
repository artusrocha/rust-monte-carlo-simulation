-- Add migration script here
ALTER TABLE item_batch
ADD COLUMN quantity NUMERIC NOT NULL CHECK (quantity >= 0) DEFAULT 0;

SELECT * FROM information_schema.columns
WHERE table_schema = 'public'
AND table_name   = 'item_batch';