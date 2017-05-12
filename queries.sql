-- To see queries and results connect to mysql with mysql -u <user_name> -p database_name --force --comments -vvv
-- Execute this script with source queries.sql

-- Queries and their results are spool on queries_result.lst
tee queries.lst 

-- This table contains all values of precision and recall calculated as a cartesian product of ground truth queries (SPARQL queries of DBpedia) and deduced queries (BGP's extraction of LIFT)
DESC precisionRecall;

-- Returns the number of BGPs 
SELECT COUNT(*) 
FROM (SELECT bgp_dbp 
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp) x
;

-- Returns the average between precision and recall
SELECT AVG(quality) 
FROM (SELECT date_,ip, MAX((preci+recall)/2) quality 
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp) x
;

-- This table contains only the highest values of precision. For time performance reason this table will be preferable used in next queries.
DROP TABLE preci;
CREATE TABLE preci(
date_ varchar(20),
ip varchar(33),
bgp_dbp varchar(2000),
nb_occurrences_dbp int(4),
rank_dbp int(5),
precision_ DECIMAL(6,5))
	SELECT date_,ip,bgp_dbp,nb_occurrences_dbp,rank_dbp, MAX(preci) precision_
	FROM precisionRecall
	GROUP BY date_,ip,bgp_dbp,nb_occurrences_dbp,rank_dbp;

-- This table contains only the highest values of precision. For time performance reason this table will be preferable used in next queries.
DROP TABLE RECALL;
CREATE TABLE recall(
date_ varchar(20),
ip varchar(33),
bgp_dbp varchar(2000),
nb_occurrences_dbp int(4),
rank_dbp int(5),
recall DECIMAL(6,5))
	SELECT date_,ip,bgp_dbp,nb_occurrences_dbp,rank_dbp,MAX(recall) recall
	FROM precisionRecall
	GROUP BY date_,ip,bgp_dbp,nb_occurrences_dbp,rank_dbp;

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
SELECT ip, AVG(precision_) 
FROM preci
GROUP BY ip  
ORDER BY 2 DESC;


-- Returns the average of recall by ip
SELECT ip, AVG(recall) 
FROM recall
GROUP BY ip 
ORDER BY 2 DESC;

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


-- Next queries return the number of occurrences (if higher than 1) of the TOP N most repeated BGPs
SELECT AVG(avg_preci_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_preci_repet 
	FROM preci 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 20) x
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
	LIMIT 15) x
;

SELECT AVG(avg_preci_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_preci_repet 
	FROM preci 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 5) x
;

-- Next queries return the number of occurrences (if higher than 1) of the TOP N most repeated BGPs
SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 20) x
;

SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 15) x
;

SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 10) x
;

SELECT AVG(avg_recall_repet) 
FROM (SELECT COUNT(*), AVG(precision_) avg_recall_repet 
	FROM recall 
	GROUP BY bgp_dbp 
	HAVING count(*)>1 
	ORDER BY 1 DESC
	LIMIT 5) x
;


-- nb_occurrences_dbp gives the number of same queries existing in one hour in the ground truth. As LIFT merges these same queries if they appear in the same input log, only one of them was executed and analyzed with lift.
-- Returns by ip the number of occurrence of the same BGP and their precision
SELECT ip,sum(nb_occurrences_dbp), AVG(precision_) 
FROM (SELECT date_,ip, nb_occurrences_dbp, MAX(preci) precision_
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp,nb_occurrences_dbp) x 
group by ip ORDER BY 2 DESC;

-- nb_occurrences_dbp gives the number of same queries existing in one hour in the ground truth. As LIFT merges these same queries if they appear in the same input log, only one of them was executed and analyzed with lift.
-- Returns by ip the number of occurrence of the same BGP and their recall
SELECT ip, SUM(nb_occurrences_dbp), AVG(recall) 
FROM (SELECT date_,ip, nb_occurrences_dbp, MAX(recall) recall
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp,nb_occurrences_dbp) x 
group by ip 
ORDER BY 2 DESC;

notee