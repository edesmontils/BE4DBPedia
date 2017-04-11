# BGP Extractor for DBpedia Logs provided in http://usewod.org


## Contacts

- Emmanuel Desmontils (Emmanuel.Desmontils_at_univ-nantes.fr)
- Patricia Serrano-Alvarado (Patricia.Serrano-Alvarado_at_univ-nantes.fr)

## usage
###  bgp-extractor

```
usage: bgp-extractor.py [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                        [-f FILE] [-t REFDATE] [-d BASEDIR]

BGP Extractor for DBPedia log.

optional arguments:
  -h, --help            show this help message and exit
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level
  -f FILE, --file FILE  Set the file to study
  -t REFDATE, --datetime REFDATE
                        Set the date-time to study in the log
  -d BASEDIR, --dir BASEDIR
                        Set the directory for results

```

### bgp-extractor-mp

```
usage: bgp-extractor-mp.py [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                           [-f FILE] [-t REFDATE] [-d BASEDIR]
                           [-p NB_PROCESSES]

Parallel BGP Extractor for DBPedia log.

optional arguments:
  -h, --help            show this help message and exit
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level
  -f FILE, --file FILE  Set the file to study
  -t REFDATE, --datetime REFDATE
                        Set the date-time to study in the log
  -d BASEDIR, --dir BASEDIR
                        Set the directory for results
  -p NB_PROCESSES, --proc NB_PROCESSES
                        Number of processes used (8 by default)

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
python3.2 -OO bgp-extractor-mp.py -p 64 -d ./logs-20151031 -f access.log-20151031.log
python3.2 -OO bgp-ranking-analysis.py ./logs-20151031/*/*-be4dbp.xml
tar cvfz logs-20151031.tar ./logs-20151031/ ; gzip logs-20151031.tar
```


