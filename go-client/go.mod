module github.com/example/iceberg-native-aws-kms-builtin/go-client

go 1.23.2

// Apache Spark Connect client for Go. Pinned to the v35 line, which targets
// Spark 4.0.x Connect servers (gRPC protobuf are forward/backward compatible
// within the 4.x Connect protocol). The SQL-string path (Sql / Show / Collect)
// used here is stable.
require github.com/apache/spark-connect-go/v35 v35.0.0-20250317154112-ffd832059443

require (
	cloud.google.com/go/compute/metadata v0.6.0 // indirect
	github.com/apache/arrow-go/v18 v18.2.0 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0 // indirect
	golang.org/x/mod v0.23.0 // indirect
	golang.org/x/net v0.36.0 // indirect
	golang.org/x/oauth2 v0.28.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	golang.org/x/tools v0.30.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/grpc v1.71.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)
