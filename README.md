# modern-data-pipeline

## stack

- dbt
- dagster
- python
- sql
- bigquery
- pyspark
- dataproc
- gcs
- docker
- github actions



## ETL stages

- focus on measurements data, which has: point or place of measurement, measurement values, source, relevant context about point or place.
 

### landing

- jobs can append/read only
- freedom of file org
- incremental data must be partitioned by timestamp
- no deletion, versioned
- data is added as is read, i.e., no transformations
- for structured and unstructured data
- some form of structure can be assumed

### processing


- two types: full and incremental
- naming: `(inc|full)_{source)_{name}_{observations}`
- jobs can apennd/read/full-delete only
- stage before proc dataset
- data here is cleaned and structured from landing
- two types: full and incremental
- incremental has a ingestion or landing timestamp
- incremental are partitionend by ingestion timestamp
- non-trivial transformation that extend the data is expected here, e.g., token extraction from a text column
- test available here are; nullity, typing, row uniqueness among the proccesing data.
- focus on measurements data, which has: measurement, point or place of measurement, measurement values,  source, relevant context about point or place.



### bi


## docs

- should include glosary
- in markdown
- diaxtasis format
- aim at ETL developers



## misc
- consumer of the data
- etl developer
- codebase maintainer


## infra
- ingestion bucket
- staging bucket
- staging dataset
- presentation dataset
- 





# 

- 