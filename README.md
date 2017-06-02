# BGP Extractor for logs of the SPARQL endpoint of DBpedia
#### DBpedia logs from http://usewod.org

## Contacts

- Emmanuel Desmontils (Emmanuel.Desmontils_at_univ-nantes.fr)
- Patricia Serrano-Alvarado (Patricia.Serrano-Alvarado_at_univ-nantes.fr)

## usage

To analyse a day of DBPedia 2015 (e.g. October 31). Consider the log of this day is in the directory './data/logs20151031/' and it is called 'access.log-20151031.log'. 

The first step is to extract BGP from this log :
```
python3.6 bgp-extractor.py -p 64 -d ./data/logs20151031/logs-20151031-extract -f ./data/logs20151031/access.log-20151031.log
```
The result is a set of directories (one for each hour) that contains one file by user. Each file is named 'userIp-be4dbp.xml'

Then, filter BGP that can be excuted on the data provider (e.g. a TPF serveur with a timeout of 20 secondes)

```
python3.6 bgp-test-endpoint.py -e TPF ./data/logs20151031/logs-20151031-extract/*/*-be4dbp.xml -to 20
```

The result is, for each user file, a file (named 'userIp-be4dbp-tested-TPF.xml'), valid with 'http://documents.ls2n.fr/be4dbp/log.dtd' (which uses 'http://documents.ls2n.fr/be4dbp/bgp.dtd'), where each 'entry' (a BGP) is evaluated according to the data provider.

Next, rank BGPs to identify most frequent ones

```
python3.6 bgp-ranking-analysis.py ./data/logs20151031/logs-20151031-extract/*/*-tested-TPF.xml
```

The result is, for each user file, a file (named 'userIp-be4dbp-tested-TPF-ranking.xml') valid with 'http://documents.ls2n.fr/be4dbp/ranking.dtd'.

Next, we suppose that LIFT results (for extracted queries) are in the directory './data/divers/liftDeductions/traces/' (see 'https://github.com/coumbaya/lift' for execution LIFT). This directory contains a set of directories (one by hour). Each one contains a file for each user (same hierarchy as for dbpedia log extraction).

Like for dbpedia extracted BGPs, rank BGP founded by LIFT.

```
python3.6 bgp-ranking-analysis.py ./data/divers/liftDeductions/traces/*/traces_*-be4dbp-tested-TPF-ranking/*-ldqp.xml -t All
```

Then, compute precision and recall to produce a set of CSV files

```
sh bigCompare.sh 
```

Finaly, to be able to calculate agregates (avg, max...), load CSV files in a MySQL database.

```
sh loadPrecisionRecall_MySQL.sh
```

## Command descriptions

### bgp-extractor

```
usage: bgp-extractor.py [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                        [-t REFDATE] [-d BASEDIR] [-r] [--tpfc]
                        [-e {SPARQLEP,TPF,None}] [-ep EP] [-to TIMEOUT]
                        [-p NB_PROCESSES]
                        file

Parallel BGP Extractor for DBPedia log.

positional arguments:
  file                  Set the file to study

optional arguments:
  -h, --help            show this help message and exit
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level (INFO by default)
  -t REFDATE, --datetime REFDATE
                        Set the date-time to study in the log
  -d BASEDIR, --dir BASEDIR
                        Set the directory for results ('./logs' by default)
  -p NB_PROCESSES, --proc NB_PROCESSES
                        Number of processes used to extract (4 by default)
                        over 8 usuable processes
```

### bgp-test-endpoint

```
usage: bgp-test-endpoint.py [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                            [-p NB_PROCESSES] [-e {SPARQL,TPF}] [-ep EP]
                            [-to TIMEOUT]
                            file [file ...]

Etude des requêtes

positional arguments:
  file                  files to analyse

optional arguments:
  -h, --help            show this help message and exit
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level
  -p NB_PROCESSES, --proc NB_PROCESSES
                        Number of processes used (8 by default)
  -e {SPARQL,TPF}, --empty {SPARQL,TPF}
                        Request a SPARQL or a TPF endpoint to verify the query
                        and test it returns at least one triple (TPF by
                        default)
  -ep EP, --endpoint EP
                        The endpoint requested for the '-e' ('--empty') option
                        (for exemple 'http://localhost:5001/dbpedia_3_9' for
                        TPF by default)
  -to TIMEOUT, --timeout TIMEOUT
                        Endpoint Time Out (60 by default). If '-to 0' and the
                        file already tested, the entry is not tested again.
```

### bgp-ranking-analysis

```
usage: bgp-ranking-analysis.py [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                               [-p NB_PROCESSES]
                               [-t {NotEmpty,Valid,WellFormed,All}]
                               file [file ...]

Etude du ranking

positional arguments:
  file                  files to analyse

optional arguments:
  -h, --help            show this help message and exit
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level
  -p NB_PROCESSES, --proc NB_PROCESSES
                        Number of processes used (8 by default)
  -t {NotEmpty,Valid,WellFormed,All}, --type {NotEmpty,Valid,WellFormed,All}
                        How to take into account the validation by a SPARQL or
                        a TPF endpoint (NotEmpty by default)
```

The '-t' argument describes entries the process has to take into account :
- 'All' : all entries,
- 'WellFormed' : only correct SPARQL queries,
- 'Valid' : only queries that are accepted by the endpoint (e.g. TPF client does'nt accept all SPARQL queries)
- 'NotEmpty' : only queries having at least one answer with the endpoint 

## Librairies to install 

- RDFLib : https://github.com/RDFLib/rdflib (doc: https://rdflib.readthedocs.io/)
- SPARQLWarpper : https://github.com/RDFLib/sparqlwrapper (doc: https://rdflib.github.io/sparqlwrapper/)
- lxml : http://lxml.de/

