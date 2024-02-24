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

## Stream data

### Example 1 -> blockchain data to a Postgres database
```shell
ethereumetl stream -s <start_block> --output postgresql+pg8000://<user>:<password>@<host>:<port>/<database>
```

### Example 2 -> blockchain data to kafka
```shell
ethereumetl stream -s <start_block> --output kafka/<host>:<port>
```