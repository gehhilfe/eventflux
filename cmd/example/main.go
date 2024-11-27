package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/gehhilfe/eventflux"
	"github.com/gehhilfe/eventflux/bus"
	"github.com/gehhilfe/eventflux/cmd/example/api"
	"github.com/gehhilfe/eventflux/cmd/example/model"
	"github.com/gehhilfe/eventflux/cmd/example/projection"
	"github.com/gehhilfe/eventflux/core"
	"github.com/gehhilfe/eventflux/store/memory"
	"github.com/google/uuid"
	"github.com/hallgren/eventsourcing"
	"github.com/nats-io/nats.go"
)

var (
	natsServer = flag.String("server", nats.DefaultURL, "NATS server URL")
	port       = flag.String("port", "8080", "port")
	store      = flag.String("store", "memory", "store")
)

func main() {

	eventsourcing.SetIDFunc(uuid.NewString)

	flag.Parse()

	// Create NATS connection
	nc, err := nats.Connect(*natsServer)
	if err != nil {
		slog.Error("Failed to connect to NATS", slog.Any("error", err))
		os.Exit(1)
	}
	defer nc.Close()
	// js, err := nc.JetStream()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	var sm core.StoreManager
	//if *store == "memory" {
	sm = memory.NewInMemoryStoreManager()
	//} else if *store == "bolt" {
	//	sm, _ = bolt.NewBoltStoreManager("test.db")
	//} else {
	//sm, _ := postgres.NewStoreManager("postgres://postgres:admin@localhost:5432/postgres?sslmode=disable")
	//}

	mb := bus.NewCoreNatsMessageBus(nc, "")

	_, err = eventflux.NewStores(sm, mb)
	if err != nil {
		slog.Error("Failed to create stores", slog.Any("error", err))
		os.Exit(1)
	}

	var eventStore core.SubStore
	for s := range sm.List(core.Metadata{"type": "local"}) {
		eventStore = s
		break
	}
	if eventStore == nil {
		eventStore, err = sm.Create(core.StoreId(uuid.New()), core.Metadata{"type": "local"})
		if err != nil {
			slog.Error("Failed to create event store", slog.Any("error", err))
			os.Exit(1)
		}
	}

	repo := eventsourcing.NewEventRepository(eventStore)
	repo.Register(&model.NotificationAggregate{})

	// Create projections
	reg := eventsourcing.NewRegister()
	reg.Register(&model.NotificationAggregate{})

	notificationOverview := projection.NewNotificationOverview(sm, reg)
	go notificationOverview.Project()

	//notificationView := projection.NewNotificationView(sm.DB(), sm, reg)
	//go notificationView.Project()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /notification", api.CreateNotificationHandler(repo))
	mux.HandleFunc("POST /notification/{id}/read", api.MarkNotificationAsReadHandler(repo))
	mux.HandleFunc("GET /notification/overview", api.ListRecentNotificationsHandler(notificationOverview))
	mux.HandleFunc("GET /notification/sse", api.StreamNotificationsHandler(sm))
	mux.HandleFunc("OPTIONS /notification/sse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		w.WriteHeader(http.StatusOK)
	})

	// Context from signal
	sigIntCh := make(chan os.Signal, 1)
	signal.Notify(sigIntCh, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sigIntCh
		slog.Info("Shutting down...")
		cancel()
	}()

	slog.Info("Starting server", slog.Any("port", *port))
	tcpListener, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		slog.Error("Failed to listen", slog.Any("error", err))
		os.Exit(1)
	}
	defer tcpListener.Close()
	go http.Serve(tcpListener, mux)
	<-ctx.Done()
}
