CREATE TABLE IF NOT EXISTS t1 (
    acc_id INTEGER NOT NULL,
    par_id INTEGER NOT NULL,
    ins_id INTEGER NOT NULL,
    cat SMALLINT,
    qty INTEGER NOT NULL,
    factor NUMERIC NOT NULL,
    ratio NUMERIC NOT NULL GENERATED ALWAYS AS (qty * factor) STORED, 
    field1 NUMERIC,
    field2 NUMERIC,
    field3 NUMERIC,
    field4 NUMERIC,
    field5 NUMERIC,
    field6 NUMERIC,
    field7 NUMERIC,
    field8 NUMERIC,
    field9 NUMERIC,
    field10 NUMERIC,
    PRIMARY KEY (acc_id, par_id, ins_id)
);
CREATE INDEX IF NOT EXISTS idx_cat_t1 ON t1 (cat);

CREATE TABLE IF NOT EXISTS t2 (
    acc_id INTEGER NOT NULL,
    par_id INTEGER NOT NULL,
    ins_id INTEGER NOT NULL,
    cat SMALLINT,
    qty INTEGER NOT NULL,
    factor NUMERIC NOT NULL,
    ratio NUMERIC NOT NULL GENERATED ALWAYS AS (qty * factor) STORED, 
    field1 NUMERIC,
    field2 NUMERIC,
    field3 NUMERIC,
    field4 NUMERIC,
    field5 NUMERIC,
    field6 NUMERIC,
    field7 NUMERIC,
    field8 NUMERIC,
    field9 NUMERIC,
    field10 NUMERIC,
    PRIMARY KEY (acc_id, par_id, ins_id)
);
CREATE INDEX IF NOT EXISTS idx_cat_t2 ON t2 (cat);

-- ------------------------------------------------

CREATE TABLE IF NOT EXISTS t3 (
    acc_id INTEGER NOT NULL,
    par_id INTEGER NOT NULL,
    ins_id INTEGER NOT NULL,
    cat SMALLINT,
    qty INTEGER NOT NULL,
    factor NUMERIC NOT NULL,
    ratio NUMERIC NOT NULL GENERATED ALWAYS AS (qty * factor) STORED, 
    PRIMARY KEY (acc_id, par_id, ins_id)
);
CREATE INDEX IF NOT EXISTS idx_cat_t3 ON t3 (cat);

CREATE TABLE IF NOT EXISTS t4 (
    acc_id INTEGER NOT NULL,
    par_id INTEGER NOT NULL,
    ins_id INTEGER NOT NULL,
    cat SMALLINT,
    qty INTEGER NOT NULL,
    factor NUMERIC NOT NULL,
    ratio NUMERIC NOT NULL GENERATED ALWAYS AS (qty * factor) STORED, 
    PRIMARY KEY (acc_id, par_id, ins_id)
);
CREATE INDEX IF NOT EXISTS idx_cat_t4 ON t4 (cat);

-- ------------------------------------------------
CREATE TABLE IF NOT EXISTS acc (
    id INTEGER NOT NULL PRIMARY KEY,
    par_id SMALLINT NOT NULL,
    dst INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_par_dst_acc ON acc (par_id, dst);

CREATE TABLE IF NOT EXISTS t5 (
    acc_id INTEGER NOT NULL REFERENCES acc(id),
    ins_id INTEGER NOT NULL,
    grp SMALLINT,
    grpv char(1),
    qty INTEGER NOT NULL,
    factor NUMERIC NOT NULL,
    ratio NUMERIC NOT NULL GENERATED ALWAYS AS (qty * factor) STORED, 
    PRIMARY KEY (acc_id, ins_id)
);
CREATE INDEX IF NOT EXISTS idx_grpv_t5 ON t5 (grp, grpv);

select acc.dst, grp, grpv, sum(ratio) as sum 
from t5 
join acc on t5.acc_id = acc.id 
group by acc.dst, grp, grpv;

select grp, grpv, sum(ratio) as sum 
from t5 
group by grp, grpv;

select dst, grp, grpv, sum(ratio) as sum 
from acc 
join t5 on t5.acc_id = acc.id 
group by dst, grp, grpv;

select count(distinct (dst, grp, grpv)) 
from acc 
join t5 on t5.acc_id = acc.id 
group by dst, grp, grpv;


select t3.acc_id, t3.cat, (t3.sum - t4.sum) as r 
from (select acc_id, cat, sum(ratio) as sum from t3 group by cat, acc_id) t3 
join (select acc_id, cat, sum(ratio) as sum from t4 group by cat, acc_id) t4 on t3.cat = t4.cat and t3.acc_id = t4.acc_id 
where t3.sum > t4.sum ;

select t3.acc_id, t3.cat, (t3.sum - t4.sum) as r 
from (select acc_id, cat, sum(ratio) as sum from t3 group by cat, acc_id) t3 
join (select acc_id, cat, sum(ratio) as sum from t4 group by cat, acc_id) t4 on t3.cat = t4.cat and t3.acc_id = t4.acc_id ;

 select dst, grp, sum from (select dst, grp, sum(ratio) as sum from acc join t5 on acc_id=id where par_id=1 group by dst, grp) where sum > 0;


-- work_mem
SET work_mem = '256MB';
-- SELECT * FROM users ORDER BY LOWER(display_name);
RESET work_mem;