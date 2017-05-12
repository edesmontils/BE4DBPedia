-- To see queries and results connect to mysql with mysql -u <user_name> -p database_name --force --comments -vvv
-- This scrip can be executed after loading the database with loadPrecisionRecall_MySQL.sh
-- Execute this script with source queries.sql

-- Queries and their results are spool on queries_result.lst
SYSTEM rm queries.lst
tee queries.lst

-- This table contains all values of precision and recall calculated as a cartesian product of ground truth queries (SPARQL queries of DBpedia) and deduced queries (BGP's extraction of LIFT)
DESC precisionRecall;

-- This table contains the highgest values of the "quality" of LIFT deductions. We consider as quality the average of precision and recall.

-- Returns the average of the quality of the LIFT deductions
SELECT AVG(quality) 
FROM quality;

-- Returns the total average of precision
SELECT AVG(precision_) 
FROM preci;

-- Returns the total average of recall
SELECT AVG(recall) 
FROM recall;

-- Returns the different values of precision and the number of BGPs having that value
SELECT count(precision_) nb_bgps_date_ip, precision_
FROM preci
GROUP BY precision_
ORDER BY 1 DESC;

-- Returns the different values of recall and the number of BGPs having that value
SELECT count(recall) nb_bgps_date_ip, recall
FROM recall
GROUP BY recall
ORDER BY 1 DESC;

-- Returns the average of precision obtained by date 
SELECT date_, AVG(quality)
FROM quality
GROUP BY date_
ORDER BY 2 DESC;

-- Returns the average of precision obtained by date 
SELECT date_, AVG(precision_)
FROM preci 
GROUP BY date_
ORDER BY 2 DESC;

-- Returns the average of recall obtained by date 
SELECT date_, AVG(recall) 
FROM recall
GROUP BY date_
ORDER BY 2 DESC;

-- Returns the average of precision by ip
SELECT ip, AVG(quality) 
FROM quality
GROUP BY ip  
ORDER BY 2 DESC;

-- Returns the average of precision by ip
SELECT ip, AVG(precision_) 
FROM preci
GROUP BY ip  
ORDER BY 2 DESC;

-- Returns the average of recall by ip
SELECT ip, AVG(recall) 
FROM recall
GROUP BY ip 
ORDER BY 2 DESC;

-- Returns the global quality of queries that occurred more than once
SELECT AVG(avg_quality_repet)
FROM (SELECT COUNT(*), AVG(quality) avg_quality_repet
	FROM quality
	GROUP BY bgp_dbp 
	HAVING count(*)>1) y
;

-- Returns the global precision of queries that occurred more than once
SELECT AVG(avg_preci_repet)
FROM (SELECT COUNT(*), AVG(precision_) avg_preci_repet
	FROM preci
	GROUP BY bgp_dbp 
	HAVING count(*)>1) y
;

-- Returns the global recall of queries that occurred more than once
SELECT AVG(avg_recall_repet)
FROM (SELECT COUNT(*), AVG(recall) avg_recall_repet
	FROM recall
	GROUP BY bgp_dbp 
	HAVING count(*)>1) y
;

-- Returns the quality of queries that occurred more than once
SELECT COUNT(*), AVG(quality) avg_quality_repet
FROM quality
GROUP BY bgp_dbp 
HAVING count(*)>1
ORDER BY 1 DESC;

-- Returns the precision of queries that occurred more than once
SELECT COUNT(*), AVG(precision_) avg_preci_repet
FROM preci
GROUP BY bgp_dbp 
HAVING count(*)>1
ORDER BY 1 DESC;

-- Returns the recall of queries that occurred more than once
SELECT COUNT(*), AVG(recall) avg_recall_repet
FROM recall
GROUP BY bgp_dbp 
HAVING count(*)>1
ORDER BY 1 DESC;

-- Next queries return the number of occurrences (if higher than 1) and the quality of the TOP N most repeated BGPs
SELECT AVG(avg_quality_repet) 
FROM (SELECT COUNT(*), AVG(quality) avg_quality_repet 
	FROM quality 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 53) x
;

SELECT AVG(avg_quality_repet) 
FROM (SELECT COUNT(*), AVG(quality) avg_quality_repet 
	FROM quality 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 42) x
;
SELECT AVG(avg_quality_repet) 
FROM (SELECT COUNT(*), AVG(quality) avg_quality_repet 
	FROM quality 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 31) x
;

SELECT AVG(avg_quality_repet) 
FROM (SELECT COUNT(*), AVG(quality) avg_quality_repet 
	FROM quality 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 21) x
;

SELECT AVG(avg_quality_repet) 
FROM (SELECT COUNT(*), AVG(quality) avg_quality_repet 
	FROM quality 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 15) x
;

SELECT AVG(avg_quality_repet) 
FROM (SELECT COUNT(*), AVG(quality) avg_quality_repet 
	FROM quality 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 10) x
;

SELECT AVG(avg_quality_repet) 
FROM (SELECT COUNT(*), AVG(quality) avg_quality_repet 
	FROM quality 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 5) x
;

-- Next queries return the number of occurrences (if higher than 1) and the precision of the TOP N most repeated BGPs
SELECT AVG(avg_preci_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_preci_repet 
	FROM preci 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 53) x
;

SELECT AVG(avg_preci_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_preci_repet 
	FROM preci 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 42) x
;

SELECT AVG(avg_preci_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_preci_repet 
	FROM preci 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 31) x
;

SELECT AVG(avg_preci_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_preci_repet 
	FROM preci 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 21) x
;

SELECT AVG(avg_preci_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_preci_repet 
	FROM preci 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 15) x
;

SELECT AVG(avg_preci_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_preci_repet 
	FROM preci 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 10) x
;

SELECT AVG(avg_preci_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_preci_repet 
	FROM preci 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 5) x
;

-- Next queries return the number of occurrences (if higher than 1) and the recall of the TOP N most repeated BGPs
SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(recall) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 53) x
;

SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(recall) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 42) x
;

SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(recall) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 31) x
;

SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(recall) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 21) x
;

SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(recall) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 10) x
;

SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(recall) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 15) x
;

SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(recall) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 5) x
;


-- nb_occurrences_dbp gives the number of same queries existing in one hour in the ground truth. As LIFT merges these same queries if they appear in the same input log, only one of them was executed and analyzed with lift.

-- Returns by ip the number of occurrence of the same BGP and their quality
SELECT ip,sum(nb_occurrences_dbp), AVG(quality) 
FROM quality
group by ip ORDER BY 2 DESC;

-- Returns by ip the number of occurrence of the same BGP and their precision
SELECT ip,sum(nb_occurrences_dbp), AVG(precision_) 
FROM preci
group by ip ORDER BY 2 DESC;

-- Returns by ip the number of occurrence of the same BGP and their recall
SELECT ip, SUM(nb_occurrences_dbp), AVG(recall) 
FROM recall 
group by ip 
ORDER BY 2 DESC;

notee