# BGP Extractor for logs of the SPARQL endpoint of DBpedia
#### DBpedia logs from http://usewod.org

## Contacts

- Emmanuel Desmontils (Emmanuel.Desmontils_at_univ-nantes.fr)
- Patricia Serrano-Alvarado (Patricia.Serrano-Alvarado_at_univ-nantes.fr)

## usage
###  bgp-extractor

```
usage: bgp-extractor.py [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                        [-t REFDATE] [-d BASEDIR] [-r] [--tpfc] [-e] [-ep EP]
                        file

BGP Extractor for DBPedia log.

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
  -r, --ranking         do ranking after extraction
  --tpfc                filter some query the TPF Client does'nt treat
  -e, --empty           Request an endpoint to verify the query and test it
                        returns at least one triple
  -ep EP, --endpoint EP
                        The endpoint requested for the '-e' ('--empty') option
                        ('http://dbpedia.org/sparql' by default)
```

### bgp-extractor-mp

```
usage: bgp-extractor-mp.py [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                           [-t REFDATE] [-d BASEDIR] [-r] [--tpfc] [-e]
                           [-ep EP] [-p NB_PROCESSES]
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
  -r, --ranking         do ranking after extraction
  --tpfc                filter some query the TPF Client does'nt treat
  -e, --empty           Request an endpoint to verify the query and test it
                        returns at least one triple
  -ep EP, --endpoint EP
                        The endpoint requested for the '-e' ('--empty') option
                        ('http://dbpedia.org/sparql' by default)
  -p NB_PROCESSES, --proc NB_PROCESSES
                        Number of processes used to extract (4 by default)
                        over 8 usuable processes
```

### bgp-ranking-analysis

```
usage: bgp-ranking-analysis.py [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                               [-p NB_PROCESSES]
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
```

## Exemple

Creating a BGP analysis for DBPedia 2015 (October 31)


```
python3.2 -OO bgp-extractor-mp.py -p 64 -d ./logs-20151031 access.log-20151031.log
python3.2 -OO bgp-ranking-analysis.py ./logs-20151031/*/*-be4dbp.xml
tar cvfz logs-20151031.tar ./logs-20151031/ ; gzip logs-20151031.tar
```
## Librairies to install 

- RDFLib : https://github.com/RDFLib/rdflib https://rdflib.readthedocs.io/
- SPARQLWarpper : https://github.com/RDFLib/sparqlwrapper https://rdflib.github.io/sparqlwrapper/
- lxml : http://lxml.de/

