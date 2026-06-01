package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/YonglinLi/config-center/pkg/config"
	"github.com/YonglinLi/config-center/pkg/raftnode"
	"github.com/YonglinLi/config-center/pkg/service"
	pb_config "github.com/YonglinLi/config-center/proto/config_service"
	pb_raft "github.com/YonglinLi/config-center/proto/raft_transport"
)

func main() {
	configPath := flag.String("config", "", "path to config file")
	nodeID := flag.String("id", "", "node ID (overrides config)")
	raftAddr := flag.String("raft-addr", "", "raft address (overrides config)")
	grpcAddr := flag.String("grpc-addr", "", "gRPC address (overrides config)")
	dataDir := flag.String("data-dir", "", "data directory (overrides config)")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap cluster")
	flag.Parse()

	var cfg *config.NodeConfig
	var err error

	if *configPath != "" {
		cfg, err = config.LoadConfig(*configPath)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
	} else {
		cfg = config.DefaultConfig()
	}

	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}
	if *raftAddr != "" {
		cfg.RaftAddr = *raftAddr
	}
	if *grpcAddr != "" {
		cfg.GRPCAddr = *grpcAddr
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *bootstrap {
		cfg.Bootstrap = true
	}

	node, err := raftnode.NewRaftNode(cfg)
	if err != nil {
		log.Fatalf("Failed to create raft node: %v", err)
	}

	grpcServer := grpc.NewServer()

	raftTransportSvc := service.NewRaftTransportService(node.Transport)
	pb_raft.RegisterRaftTransportServer(grpcServer, raftTransportSvc)

	configSvc := service.NewConfigServiceServer(node)
	pb_config.RegisterConfigServiceServer(grpcServer, configSvc)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", cfg.GRPCAddr, err)
	}

	fmt.Printf("Config Center node [%s] starting\n", cfg.NodeID)
	fmt.Printf("  Raft addr: %s\n", cfg.RaftAddr)
	fmt.Printf("  gRPC addr: %s\n", cfg.GRPCAddr)
	fmt.Printf("  Data dir:  %s\n", cfg.DataDir)
	fmt.Printf("  Bootstrap: %v\n", cfg.Bootstrap)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\nShutting down...")
	grpcServer.GracefulStop()
	if err := node.Shutdown(); err != nil {
		log.Printf("Error shutting down raft node: %v", err)
	}
	fmt.Println("Shutdown complete.")
}
