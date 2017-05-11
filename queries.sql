-- To see queries and results connect to mysql with mysql -u <user_name> -p database_name --force --comments -vvv
-- Execute this script with source queries.sql

-- Queries and their results are spool on queries_result.lst
tee queries_result.lst 

-- This table contains precision an recall of ground truth queries (SPARQL queries of DBpedia) and deduced queries (BGP's extraction of LIFT)
DESC precisionRecall;

-- Returns the number of BGPs 
SELECT COUNT(*) 
FROM (SELECT bgp_dbp 
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp) x
;

-- Returns the total average of precision
SELECT AVG(precision_) 
FROM (SELECT date_,ip,MAX(preci) precision_
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp) x
;

-- Returns the total average of recall
SELECT AVG(recall) 
FROM (SELECT date_,ip, MAX(recall) recall 
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp) x
;

-- Returs the average of precision and recall
SELECT AVG(precision_), AVG(recall) 
FROM (SELECT date_,ip, MAX(preci) precision_, MAX(recall) recall 
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp) x
;

-- Returns the average between precision and recall
SELECT AVG(quality) 
FROM (SELECT date_,ip, MAX((preci+recall)/2) quality 
	FROM precisionRecall GROUP BY date_,ip,bgp_dbp) x
;

-- Returns the different values of precision and the number of BGPs having that value
SELECT precision_, count(precision_) nb_bgps_date_ip 
FROM (SELECT date_,ip, MAX(preci) precision_ 
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp 
	ORDER BY precision_) x 
GROUP BY precision_;

-- Returns the different values of recall and the number of BGPs having that value
SELECT recall, count(recall) nb_bgps_date_ip
FROM(SELECT date_,ip,bgp_dbp, MAX(recall) recall 
	FROM precisionRecall GROUP BY date_,ip,bgp_dbp) x 
GROUP BY recall;

-- Returns the average of precision obtained by date 
SELECT date_,AVG(precision_) 
FROM (SELECT date_,ip, MAX(preci) precision_ 
	FROM precisionRecall GROUP BY date_,ip,bgp_dbp) x 
group by date_;

-- Returns the average of recall obtained by date 
SELECT date_,AVG(recall) 
FROM (SELECT date_,ip, MAX(recall) recall
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp) x 
group by date_;

-- Returns the average of precision by ip
SELECT ip, AVG(precision_) 
FROM (SELECT date_,ip, MAX(preci) precision_ 
	FROM precisionRecall 
	GROUP BY date_,ip,bgp_dbp) x 
GROUP BY ip  
ORDER BY 2 DESC;


-- Returns the average of recall by ip
SELECT ip, AVG(recall) 
FROM (SELECT date_,ip, MAX(recall) recall
	FROM precisionRecall
	GROUP BY date_,ip,bgp_dbp) x 
GROUP BY ip 
ORDER BY 2 DESC;


-- Returns the number repetitions (if higher than 1) of BGPs and the average of their precision
SELECT count(*), avg(precision_) 
FROM (SELECT date_,ip,bgp_dbp, MAX(preci) precision_
	FROM precisionRecall
	GROUP BY date_,ip,bgp_dbp) x 
GROUP BY bgp_dbp 
HAVING count(*)>1 
order by 1 DESC;

-- Returns the number repetitions (if higher than 1) of BGPs and the average of their recall
SELECT count(*), avg(recall) 
FROM (SELECT date_,ip,bgp_dbp, MAX(recall) recall
	FROM precisionRecall
	GROUP BY date_,ip,bgp_dbp) x 
GROUP BY bgp_dbp 
HAVING count(*)>1 
order by 1 DESC;

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