module github.com/gehhilfe/eventflux

go 1.23.1

require (
	github.com/google/uuid v1.6.0
	github.com/hallgren/eventsourcing v0.6.0
	github.com/hallgren/eventsourcing/core v0.4.0
	github.com/lib/pq v1.10.9
	github.com/nats-io/nats-server/v2 v2.10.22
	github.com/nats-io/nats.go v1.37.0
	go.etcd.io/bbolt v1.3.11
)

require github.com/huandu/xstrings v1.4.0 // indirect

require (
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/huandu/go-sqlbuilder v1.33.0
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/minio/highwayhash v1.0.3 // indirect
	github.com/nats-io/jwt/v2 v2.7.2 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/veqryn/slog-context v0.7.0 // indirect
	golang.org/x/crypto v0.29.0 // indirect
	golang.org/x/sys v0.27.0 // indirect
	golang.org/x/time v0.8.0 // indirect
)

replace github.com/hallgren/eventsourcing => github.com/gehhilfe/eventsourcing v0.0.0-20240915163141-526368b670bb

replace github.com/hallgren/eventsourcing/core => github.com/gehhilfe/eventsourcing/core v0.0.0-20240915163141-526368b670bb
