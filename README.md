# crdb-cdc
cockroachdb change-data-capture

https://www.cockroachlabs.com/docs/v21.1/stream-data-out-of-cockroachdb-using-changefeeds.html#create-a-changefeed-core

https://www.cockroachlabs.com/docs/v21.1/changefeed-for

```sh
docker compose -f cockroachdb.yaml up

docker exec -it crdb ./cockroach sql --insecure -e "SET CLUSTER SETTING kv.rangefeed.enabled = true;"
docker exec -it crdb ./cockroach sql --insecure -e "CREATE DATABASE crdb_test;"

# get an interactive shell
docker exec -it crdb ./cockroach sql --insecure

# migrations up
migrate -path ./db/migrations -database 'cockroachdb://root@localhost:26257/crdb_test?sslmode=disable' up

# tail the CDC
docker exec -it crdb ./cockroach sql --insecure --format=csv -e "EXPERIMENTAL CHANGEFEED FOR crdb_test.events;"

# tail the CDC after a timestamp
docker exec -it crdb ./cockroach sql --insecure --format=csv -e "EXPERIMENTAL CHANGEFEED FOR crdb_test.events WITH resolved='1s', cursor='1621932825533091978.0000000000';"

# run changefeed
go run . --mode changefeed

# load data
go run . --mode load &  go run . --mode load & go run . --mode load
```
