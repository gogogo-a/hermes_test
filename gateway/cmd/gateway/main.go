package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"

	"hermes/gateway/internal/config"
	"hermes/gateway/internal/httpapi"
	kafkapkg "hermes/gateway/internal/kafka"
	"hermes/gateway/internal/schema"
)

func main() {
	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	schemaPath := cfg.TaskSchemaPath
	if !filepath.IsAbs(schemaPath) {
		// Try relative to cwd, then relative to repo root (parent of gateway/)
		if _, statErr := os.Stat(schemaPath); statErr != nil {
			alt := filepath.Join("..", schemaPath)
			if _, err2 := os.Stat(alt); err2 == nil {
				schemaPath = alt
			}
		}
	}

	val, err := schema.Load(schemaPath)
	if err != nil {
		log.Fatalf("schema: %v", err)
	}

	prod := kafkapkg.NewProducer(cfg.KafkaBootstrap)
	defer func() { _ = prod.Close() }()

	srv := httpapi.New(cfg, val, prod, log.Default())
	log.Printf("gateway listening on %s (Kafka %s, schema %s)", cfg.GatewayAddr, cfg.KafkaBootstrap, schemaPath)
	if err := http.ListenAndServe(cfg.GatewayAddr, srv.Routes()); err != nil {
		log.Fatalf("http: %v", err)
	}
}
