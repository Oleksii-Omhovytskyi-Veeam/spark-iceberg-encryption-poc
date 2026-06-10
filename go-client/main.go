// Command go-client is a THIN external client that drives TWO natively-encrypted
// Iceberg tables -- one per namespace, each wrapped by a DIFFERENT AWS KMS key --
// through a remote Spark Connect server using the official Apache spark-connect-go
// (v35) client.
//
// The point of the demo: this process contains NO Spark, NO Iceberg, NO KMS
// client, and NO AWS credentials. It only opens a gRPC session to the Spark
// Connect server (sc://...:15002) and sends SQL strings. The server holds the
// HiveCatalog, the BUILT-IN org.apache.iceberg.aws.AwsKeyManagementClient
// (activated by encryption.kms-type=aws), and the LocalStack AWS creds. Yet
// the data the server writes on this client's behalf is envelope-encrypted with a
// per-namespace KMS key:
//
//	demo.tenant1_backup.ObjectVersions  encryption.key-id = alias/iceberg/tenant1  (KMS key A)
//	demo.tenant2_backup.ObjectVersions  encryption.key-id = alias/iceberg/tenant2  (KMS key B)
//
// Per namespace it runs: CREATE NAMESPACE, CREATE TABLE (with the namespace's
// key-id + format-version=3), batched multi-row INSERT, then SELECT (printed).
// The SELECTs succeeding proves KMS Decrypt worked for each distinct key.
//
// Endpoint is configurable via SPARK_CONNECT_REMOTE (default the in-compose
// address sc://spark-connect:15002; from the host use sc://localhost:15002).
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/spark-connect-go/v35/spark/sql"
)

// nsSpec describes one namespace + its encrypted table + the KMS key/alias that
// wraps that table's DEKs.
type nsSpec struct {
	namespace  string // e.g. demo.tenant1_backup
	table      string // e.g. demo.tenant1_backup.ObjectVersions
	keyID      string // encryption.key-id == KMS alias, e.g. alias/iceberg/tenant1
	createCols string
	rows       string // VALUES (...) , (...) for the batched INSERT
	wantRows   int
}

var specs = []nsSpec{
	{
		namespace:  "demo.tenant1_backup",
		table:      "demo.tenant1_backup.ObjectVersions",
		keyID:      "alias/iceberg/tenant1",
		createCols: `id STRING, restore_point_id STRING, payload_ref STRING`,
		rows: `('obj-0001', 'rp-2026-06-09-01', 's3://backups/tenant1/obj-0001.dat'),
               ('obj-0002', 'rp-2026-06-09-01', 's3://backups/tenant1/obj-0002.dat'),
               ('obj-0003', 'rp-2026-06-09-02', 's3://backups/tenant1/obj-0003.dat'),
               ('obj-0004', 'rp-2026-06-09-02', 's3://backups/tenant1/obj-0004.dat')`,
		wantRows: 4,
	},
	{
		namespace:  "demo.tenant2_backup",
		table:      "demo.tenant2_backup.ObjectVersions",
		keyID:      "alias/iceberg/tenant2",
		createCols: `id STRING, restore_point_id STRING, payload_ref STRING`,
		rows: `('obj-0001', 'rp-2026-06-10-01', 's3://backups/tenant2/obj-0001.dat'),
               ('obj-0002', 'rp-2026-06-10-01', 's3://backups/tenant2/obj-0002.dat'),
               ('obj-0003', 'rp-2026-06-10-02', 's3://backups/tenant2/obj-0003.dat')`,
		wantRows: 3,
	},
}

func remoteAddr() string {
	if r := os.Getenv("SPARK_CONNECT_REMOTE"); r != "" {
		return r
	}
	return "sc://spark-connect:15002"
}

// exec runs a SQL statement that is not expected to return rows (DDL/DML).
// spark-connect-go returns a DataFrame for every statement; we Collect it so the
// server actually executes the plan, then ignore the (empty) result.
func exec(ctx context.Context, spark sql.SparkSession, label, query string) error {
	log.Printf("[%s] >>> %s", label, query)
	df, err := spark.Sql(ctx, query)
	if err != nil {
		return fmt.Errorf("%s: Sql: %w", label, err)
	}
	if _, err := df.Collect(ctx); err != nil {
		return fmt.Errorf("%s: Collect: %w", label, err)
	}
	log.Printf("[%s] OK", label)
	return nil
}

func runNamespace(ctx context.Context, spark sql.SparkSession, s nsSpec) error {
	log.Printf("=== namespace %s -> KMS key %s ===", s.namespace, s.keyID)

	if err := exec(ctx, spark, "CREATE NS",
		fmt.Sprintf("CREATE NAMESPACE IF NOT EXISTS %s", s.namespace)); err != nil {
		return err
	}

	// encryption.key-id carries the namespace's KMS key/alias; format-version=3 is
	// required for Iceberg native encryption.
	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
            %s
        ) USING iceberg
        TBLPROPERTIES ('encryption.key-id' = '%s', 'format-version' = '3')`,
		s.table, s.createCols, s.keyID)
	if err := exec(ctx, spark, "CREATE TABLE", createSQL); err != nil {
		return err
	}

	// Idempotent demo: start from an empty table so repeated runs are stable.
	if err := exec(ctx, spark, "TRUNCATE",
		fmt.Sprintf("DELETE FROM %s", s.table)); err != nil {
		return err
	}

	// Batched multi-row INSERT.
	if err := exec(ctx, spark, "INSERT",
		fmt.Sprintf("INSERT INTO %s VALUES %s", s.table, s.rows)); err != nil {
		return err
	}

	// SELECT back -- success here proves KMS Decrypt worked for this key.
	selectSQL := fmt.Sprintf("SELECT * FROM %s ORDER BY id", s.table)
	log.Printf("[SELECT] >>> %s", selectSQL)
	df, err := spark.Sql(ctx, selectSQL)
	if err != nil {
		return fmt.Errorf("SELECT: Sql: %w", err)
	}
	if err := df.Show(ctx, 100, false); err != nil {
		return fmt.Errorf("SELECT: Show: %w", err)
	}
	rows, err := df.Collect(ctx)
	if err != nil {
		return fmt.Errorf("SELECT: Collect: %w", err)
	}
	log.Printf("[SELECT] %d row(s) returned from %s:", len(rows), s.table)
	for _, r := range rows {
		log.Printf("  %v", r.Values())
	}
	if len(rows) != s.wantRows {
		return fmt.Errorf("namespace %s: expected %d rows, got %d", s.namespace, s.wantRows, len(rows))
	}
	return nil
}

func run() error {
	ctx := context.Background()
	remote := remoteAddr()
	log.Printf("connecting to Spark Connect server at %s", remote)

	spark, err := sql.NewSessionBuilder().Remote(remote).Build(ctx)
	if err != nil {
		return fmt.Errorf("build session: %w", err)
	}
	defer spark.Stop()
	log.Printf("session established")

	for _, s := range specs {
		if err := runNamespace(ctx, spark, s); err != nil {
			return err
		}
	}

	log.Printf("====================================================================")
	log.Printf("SUMMARY: two tenant namespaces, two different AWS KMS keys (via LocalStack)")
	log.Printf("====================================================================")
	for _, s := range specs {
		log.Printf("  %-20s table=%-36s encryption.key-id=%s",
			s.namespace, s.table, s.keyID)
	}
	log.Printf("Each tenant table wrapped its Iceberg DEK with its OWN KMS key.")
	log.Printf("GO CLIENT DONE: CREATE + batched INSERT + SELECT succeeded for both encrypted tables.")
	return nil
}

func main() {
	log.SetFlags(log.Ltime)
	if err := run(); err != nil {
		log.Fatalf("ERROR: %v", err)
	}
}
