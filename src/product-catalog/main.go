// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package main

//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go
//go:generate go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
//go:generate protoc --go_out=./ --go-grpc_out=./ --proto_path=../../pb ../../pb/demo.proto

import (
	"context"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	otelhooks "github.com/open-feature/go-sdk-contrib/hooks/open-telemetry/pkg"
	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
	"github.com/open-feature/go-sdk/openfeature"
	pb "github.com/opentelemetry/opentelemetry-demo/src/product-catalog/genproto/oteldemo"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	log                 *logrus.Logger
	catalog             []*pb.Product
	resource            *sdkresource.Resource
	initResourcesOnce   sync.Once
	redisClient         *redis.Client
	redisListRangeLimit int
)

const DEFAULT_RELOAD_INTERVAL = 10

func init() {
	log = logrus.New()

	redisListRangeLimit = 1200
	if limitStr := os.Getenv("REDIS_LIST_RANGE_LIMIT"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil && limit > 0 {
			redisListRangeLimit = limit
		} else {
			log.Warnf("Invalid REDIS_LIST_RANGE_LIMIT value: %s, using default: %d", limitStr, redisListRangeLimit)
		}
	}

	log.Infof("Redis list range limit: %d\n", redisListRangeLimit)

	loadProductCatalog()
}

func initResource() *sdkresource.Resource {
	initResourcesOnce.Do(func() {
		extraResources, _ := sdkresource.New(
			context.Background(),
			sdkresource.WithOS(),
			sdkresource.WithProcess(),
			sdkresource.WithContainer(),
			sdkresource.WithHost(),
		)
		resource, _ = sdkresource.Merge(
			sdkresource.Default(),
			extraResources,
		)
	})
	return resource
}

func initTracerProvider() *sdktrace.TracerProvider {
	ctx := context.Background()

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		log.Fatalf("OTLP Trace gRPC Creation: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(initResource()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp
}

func initMeterProvider() *sdkmetric.MeterProvider {
	ctx := context.Background()

	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		log.Fatalf("new otlp metric grpc exporter failed: %v", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
		sdkmetric.WithResource(initResource()),
	)
	otel.SetMeterProvider(mp)
	return mp
}

func main() {
	tp := initTracerProvider()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatalf("Tracer Provider Shutdown: %v", err)
		}
		log.Println("Shutdown tracer provider")
	}()

	mp := initMeterProvider()
	defer func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			log.Fatalf("Error shutting down meter provider: %v", err)
		}
		log.Println("Shutdown meter provider")
	}()
	openfeature.AddHooks(otelhooks.NewTracesHook())
	err := openfeature.SetProvider(flagd.NewProvider())
	if err != nil {
		log.Fatal(err)
	}

	err = runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second))
	if err != nil {
		log.Fatal(err)
	}

	svc := &productCatalog{}
	var port string
	mustMapEnv(&port, "PRODUCT_CATALOG_PORT")

	var redisAddr string
	mustMapEnv(&redisAddr, "REDIS_ADDRESS")
	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Redis Ping: %v", err)
	}

	log.Infof("Product Catalog gRPC server started on port: %s", port)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("TCP Listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	reflection.Register(srv)

	pb.RegisterProductCatalogServiceServer(srv, svc)
	healthpb.RegisterHealthServer(srv, svc)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	defer cancel()

	go func() {
		if err := srv.Serve(ln); err != nil {
			log.Fatalf("Failed to serve gRPC server, err: %v", err)
		}
	}()

	<-ctx.Done()

	srv.GracefulStop()
	log.Println("Product Catalog gRPC server stopped")
}

type productCatalog struct {
	pb.UnimplementedProductCatalogServiceServer
}

func loadProductCatalog() {
	log.Info("Loading Product Catalog...")
	var err error
	catalog, err = readProductFiles()
	if err != nil {
		log.Fatalf("Error reading product files: %v\n", err)
		os.Exit(1)
	}

	// Default reload interval is 10 seconds
	// interval := DEFAULT_RELOAD_INTERVAL
	// si := os.Getenv("PRODUCT_CATALOG_RELOAD_INTERVAL")
	// if si != "" {
	// 	interval, _ = strconv.Atoi(si)
	// 	if interval <= 0 {
	// 		interval = DEFAULT_RELOAD_INTERVAL
	// 	}
	// }
	// log.Infof("Product Catalog reload interval: %d", interval)

	// ticker := time.NewTicker(time.Duration(interval) * time.Second)

	// go func() {
	// 	for {
	// 		select {
	// 		case <-ticker.C:
	// 			log.Info("Reloading Product Catalog...")
	// 			catalog, err = readProductFiles()
	// 			if err != nil {
	// 				log.Errorf("Error reading product files: %v", err)
	// 				continue
	// 			}
	// 		}
	// 	}
	// }()
}

func readProductFiles() ([]*pb.Product, error) {

	// find all .json files in the products directory
	entries, err := os.ReadDir("./products")
	if err != nil {
		return nil, err
	}

	jsonFiles := make([]fs.FileInfo, 0, len(entries))
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			jsonFiles = append(jsonFiles, info)
		}
	}

	// read the contents of each .json file and unmarshal into a ListProductsResponse
	// then append the products to the catalog
	var products []*pb.Product
	for _, f := range jsonFiles {
		jsonData, err := os.ReadFile("./products/" + f.Name())
		if err != nil {
			return nil, err
		}

		var res pb.ListProductsResponse
		if err := protojson.Unmarshal(jsonData, &res); err != nil {
			return nil, err
		}

		products = append(products, res.Products...)
	}

	log.Infof("Loaded %d products", len(products))

	return products, nil
}

func mustMapEnv(target *string, key string) {
	value, present := os.LookupEnv(key)
	if !present {
		log.Fatalf("Environment Variable Not Set: %q", key)
	}
	*target = value
}

func (p *productCatalog) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (p *productCatalog) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "health check via Watch not implemented")
}

func (p *productCatalog) ListProducts(ctx context.Context, req *pb.Empty) (*pb.ListProductsResponse, error) {
	span := trace.SpanFromContext(ctx)
	log.Info("ListProducts called")

	span.SetAttributes(
		attribute.Int("app.products.count", len(catalog)),
	)

	// Try to get all products from Redis
	// Create a context with 100ms timeout for Redis operations
	redisCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// Use LRANGE to fetch first 600 product keys
	// Get the Redis list range limit from environment variable or use default

	keys, err := redisClient.LRange(redisCtx, "product_keys", 0, int64(redisListRangeLimit)).Result()
	if err != nil {
		span.SetStatus(otelcodes.Error, fmt.Sprintf("Redis error: %v", err))
		span.AddEvent("Falling back to in-memory catalog")
		log.Error("ListProducts failed in LRange:", err)
		return nil, err
	}

	// If no keys found in Redis, use the in-memory catalog
	if len(keys) == 0 {
		span.AddEvent("No products found in Redis, using in-memory catalog")
		return &pb.ListProductsResponse{Products: catalog}, nil
	}

	// Retrieve each product from Redis
	log.Info("ListProducts completed successfully")
	return &pb.ListProductsResponse{Products: catalog}, nil
}

func (p *productCatalog) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.Product, error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.String("app.product.id", req.Id),
	)

	redisCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err := redisClient.LRange(redisCtx, "product_keys", 0, int64(redisListRangeLimit)).Result()
	if err != nil {
		span.SetStatus(otelcodes.Error, fmt.Sprintf("Redis error: %v", err))
		span.AddEvent("Falling back to in-memory catalog")
		log.Error("GetProduct failed in LRange:", err)
		return nil, err
	}

	// GetProduct will fail on a specific product when feature flag is enabled
	if p.checkProductFailure(ctx, req.Id) {
		msg := fmt.Sprintf("Error: Product Catalog Fail Feature Flag Enabled")
		span.SetStatus(otelcodes.Error, msg)
		span.AddEvent(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	var found *pb.Product

	// Try to get the product from Redis first
	val, err := redisClient.Get(ctx, req.Id).Result()
	if err == nil {
		// Product found in Redis, unmarshal it
		product := &pb.Product{}
		if unmarshalErr := protojson.Unmarshal([]byte(val), product); unmarshalErr == nil {
			found = product
			span.AddEvent("Product retrieved from Redis cache")
		} else {
			span.AddEvent(fmt.Sprintf("Failed to unmarshal product from Redis: %v", unmarshalErr))
		}
	} else if err != redis.Nil {
		// Redis error (not a "not found" error)
		span.AddEvent(fmt.Sprintf("Redis error: %v", err))
	}

	if found == nil {
		msg := fmt.Sprintf("Product Not Found: %s", req.Id)
		span.SetStatus(otelcodes.Error, msg)
		span.AddEvent(msg)
		return nil, status.Errorf(codes.NotFound, msg)
	}

	msg := fmt.Sprintf("Product Found - ID: %s, Name: %s", req.Id, found.Name)
	span.AddEvent(msg)
	span.SetAttributes(
		attribute.String("app.product.name", found.Name),
	)
	return found, nil
}

func (p *productCatalog) SearchProducts(ctx context.Context, req *pb.SearchProductsRequest) (*pb.SearchProductsResponse, error) {
	span := trace.SpanFromContext(ctx)

	var result []*pb.Product
	for _, product := range catalog {
		if strings.Contains(strings.ToLower(product.Name), strings.ToLower(req.Query)) ||
			strings.Contains(strings.ToLower(product.Description), strings.ToLower(req.Query)) {
			result = append(result, product)
		}
	}
	span.SetAttributes(
		attribute.Int("app.products_search.count", len(result)),
	)
	return &pb.SearchProductsResponse{Results: result}, nil
}

func (p *productCatalog) checkProductFailure(ctx context.Context, id string) bool {
	if id != "OLJCESPC7Z" {
		return false
	}

	client := openfeature.NewClient("productCatalog")
	failureEnabled, _ := client.BooleanValue(
		ctx, "productCatalogFailure", false, openfeature.EvaluationContext{},
	)
	return failureEnabled
}

func createClient(ctx context.Context, svcAddr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, svcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
}
