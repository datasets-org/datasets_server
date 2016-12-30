# Datasets server

Datasets project solves problem with organizing data sets. It also tries to 
ensure experiment consistency and repeatability by data set **immutability**,
 unique identification, usage and change logs.

This project is inspired by: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/45390.pdf

Data set discovery and identification is based on presence of the file `dataset
.yaml`. 

## Complementary projects
- https://github.com/tivvit/datasets
- https://github.com/tivvit/datasets_browser

## Data set properties
- `id` - UUID
- `name` - Human readable name  
- `maintainer` - Email to person responsible for the data set 
- `tags` - Data set tags for simple identification
- `internal` - Denotes if the data set is not publicly available
- `data` - Paths to folders with data (inside the data set path) 
- `url` - Public url for the data set
- `from` - id of the parent data set

Generated:
- `type` - "fs" for the filesystem
- `changelog` - Changes detected in the data set 
- `usages` - Reported usages (from the lib)

Generated from the fs:

Fields starting with `_` are paths in the container (changed based on 
`storage_replace` to final fields - `path` ...)
- `paths`, `_paths` - Path to data set 
- `links`, `_links` - Symlinks pointing to the data set
- `markdowns`, `_markdowns` - Markdown files found in the data set
- `characteristics` - Generated statistics of the data set (size, number of 
files, extensions)

## Config
- `database_path` - Where the LMDB should be stored 
- `iter_file_limit` - When searching `dataset.yaml` folders with more then 
this count won't be scanned
- `datasets` - paths to folders used for scanning  
- `storage_replace` - Replace the container paths with the real ones

## Storage types
Data sets may be added trough the API or with the file system analysis. Other
 sources like HDFS or databases may be added. 

The system is currently used with distributed FS (MooseFS - similar to GFS or
 Ceph) mounted with FUSE. Local FS will also work great.

## Database
Any key-value database is ok. Right now local **LMDB** is used. 

Other database may be used by adding connector with `storage.Storage` interface.
Aerospike will be officially supported soon.

## Todo
- data set monitoring + email notifications

## Development
```sh
docker-compose up dev
```

Feel free to contribute.

## Copyright and License
&copy; 2016 [Vít Listík](http://tivvit.cz)

Released under [MIT license](https://github.com/tivvit/datasets_server/blob/master/LICENSE)