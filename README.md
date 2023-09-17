# p2t
 
parquet to table stdout

## Installation

```bash
$ go install github.com/ytake/p2t@latest
```

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

create terraform snowflake table resource from parquet file 

```bash
$ p2t sf -file ./example/test.parquet -t tf
```
