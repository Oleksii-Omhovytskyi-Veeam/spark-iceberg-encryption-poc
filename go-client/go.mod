module github.com/example/iceberg-native-aws-kms-builtin/go-client

go 1.23.2

// Apache Spark Connect client for Go. Pinned to the v35 line, which targets
// Spark 4.0.x Connect servers (gRPC protobuf are forward/backward compatible
// within the 4.x Connect protocol). The SQL-string path (Sql / Show / Collect)
// used here is stable.
require github.com/apache/spark-connect-go/v35 v35.0.0-20250317154112-ffd832059443
