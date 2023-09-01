# p2t
 
parquet to table stdout

## Usage

### For Snowflake

create view ddl from parquet file 

```bash
$ p2t sf -file ./example/test.parquet -t view
```

create table ddl from parquet file 

```bash
$ p2t sf -file ./example/test.parquet -t table
```
