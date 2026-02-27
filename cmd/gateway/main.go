package main

import (
	"context"
	"net/http" // Enable pprof endpoints for profiling
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/gobwas/ws"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/gateway"
	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/hub"
	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/repository"
	"github.com/shubham-shewale/stock-watchlist/pkg/config"
)

func main() {
	cfg, _ := config.LoadConfig()
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	repo := repository.NewRedisStore(rdb)

	// Dependency Injection: Hub depends on the Repository Interface
	wsHub := hub.NewHub(repo, logger)

	validTickers := make(map[string]bool)
	for _, t := range cfg.Gateway.ValidTickers {
		validTickers[t] = true
	}

	mux := http.NewServeMux()

	// Register pprof handlers
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}

		client := gateway.NewClient(conn, wsHub, logger, validTickers)
		client.Start()
	})

	srv := &http.Server{Addr: cfg.App.Port, Handler: mux}

	go func() {
		logger.Info("Server Started", zap.String("port", cfg.App.Port))
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal("HTTP Error", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	srv.Shutdown(context.Background())
	// wsHub.Shutdown() // Implement if needed
	logger.Info("Shutdown Complete")
}
