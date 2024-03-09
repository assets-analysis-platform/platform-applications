# References
- https://ethereum-etl.readthedocs.io/en/latest/
- https://evgemedvedev.medium.com/exporting-and-analyzing-ethereum-blockchain-f5353414a94e


## Export all (comment out not needed parts in ethereum-etl/export_all.sh before run)
```shell
cd ethereum-etl/
```
```shell
sh export_all.sh -s <start_block> -e <end_block> -b <partition_size> -p <rpc_provider> -o <output_directory>
```

## Export blocks, transactions and logs for specified time range (days) using Docker
```
$ cd .\ethereum-etl
$ docker build -t eth-etl:latest .
$ docker run -v .\..\output\data\raw\blockchains\ethereum:/ethereum-etl/output eth-etl:latest export_blocks_transactions_and_logs -s 2018-01-03 -e 2018-01-03 -p https://rpc.ankr.com/eth/SUPER_SECRET_TOKEN
```

## Stream data

### Example 1 -> blockchain data to a Postgres database
```shell
ethereumetl stream -s <start_block> --output postgresql+pg8000://<user>:<password>@<host>:<port>/<database>
```

### Example 2 -> blockchain data to kafka
```shell
ethereumetl stream -s <start_block> --output kafka/<host>:<port>
```