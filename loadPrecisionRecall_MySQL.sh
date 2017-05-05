#!/bin/bash
# Loads, into a mysql table, lines of csv files generated by filesCompare.py, with the write_result_csv function of bgpCompare.py

# Create a mysql table in a database named database_name, user is <user_name> and password is <password>
mysql -u <user_name> -p<password> database_name -e "drop table precisionRecall;"
mysql -u <user_name> -p<password> database_name -e "create table precisionRecall(
date_ varchar(20),
ip varchar(33),
bgp_dbp varchar(2000),
bgp_lift varchar(2000),
nb_occurrences_dbp int(4),
nb_occurrences_lift int(4),
rank_dbp int(5),
rank_lift int(5),
preci DECIMAL(6,5),
recall DECIMAL(6,5)
);"

# Precision table should be empty to avoid having duplicates.
# echo "Table traces has been emptied"

# All precisionRecall.csv files are loaded into the precision table.
for file in data/precisionRecall/*/*precisionRecall.csv
do
	echo "Loading file $file ..."
	mysql -u <user_name> -p<password> database_name -e "
      LOAD DATA LOCAL INFILE '$file'
      INTO TABLE precisionRecall 
      FIELDS TERMINATED BY ';' 
      LINES TERMINATED BY '\n'
		IGNORE 1 LINES;
		show WARNINGS"
done

# This query gives the average of precision and recall by date_, user (ip), and by query.
mysql -u <user_name> -p<password> database_name -e "SELECT AVG(precision_), AVG(recall) FROM (SELECT date_,ip,MAX(preci) precision_, MAX(recall) recall FROM precisionRecall GROUP BY date_,ip,bgp_dbp) x;"