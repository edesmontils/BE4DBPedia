# BGP Extractor for DBPedia Log


## Contact

- E. Desmontils
- P. Serrano-Alvarado

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


