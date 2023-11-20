explain
select t1.cat, (t1.sum - t2.sum) as r 
from (select cat, sum(ratio) as sum from t1 group by cat) t1 
join (select cat, sum(ratio) as sum from t2 group by cat) t2 on t1.cat = t2.cat 
where t1.sum > t2.sum ;
-- 5s ~ 7s

select t3.cat, (t3.sum - t4.sum) as r 
from (select cat, sum(ratio) as sum from t3 group by cat) t3 
join (select cat, sum(ratio) as sum from t4 group by cat) t4 on t3.cat = t4.cat 
where t3.sum > t4.sum ;
-- 4s ~ 6s

CREATE MATERIALIZED VIEW mv4 AS 
 SELECT cat, sum(ratio) AS sum FROM t4 GROUP BY cat

select t3.cat, (t3.sum - mv4.sum) as r 
from (select cat, sum(ratio) as sum from t3 group by cat) t3 
join mv4 on t3.cat = mv4.cat 
where t3.sum > mv4.sum ;
-- 1.9s ~ 3s

CREATE MATERIALIZED VIEW mv2 AS 
 SELECT cat, sum(ratio) AS sum FROM t2 GROUP BY cat

select t1.cat, (t1.sum - mv2.sum) as r 
from (select cat, sum(ratio) as sum from t1 group by cat) t1 
join mv2 on t1.cat = mv2.cat 
where t1.sum > mv2.sum ;
-- 1.9s ~ 3s
