package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	pb "github.com/thomazdavis/stratago-dist/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	targetAddr  string
	consistency string
)

func getClient() (pb.KVStoreClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to %s: %w", targetAddr, err)
	}
	return pb.NewKVStoreClient(conn), conn, nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "stratocli",
		Short: "StrataGo Dist CLI - Interact with the distributed database",
	}

	rootCmd.PersistentFlags().StringVarP(&targetAddr, "addr", "a", "localhost:18001", "gRPC address of the node")

	putCmd := &cobra.Command{
		Use:   "put [key] [value]",
		Short: "Insert or update a key-value pair",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := getClient()
			if err != nil {
				return err
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			req := &pb.PutRequest{
				Key:   args[0],
				Value: []byte(args[1]),
			}

			resp, err := client.Put(ctx, req)
			if err != nil {
				return fmt.Errorf("rpc failed: %w", err)
			}

			if resp.Success {
				fmt.Println("OK")
			} else {
				fmt.Printf("FAILED: %s\n", resp.Message)
			}
			return nil
		},
	}

	getCmd := &cobra.Command{
		Use:   "get [key]",
		Short: "Retrieve a value by key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := getClient()
			if err != nil {
				return err
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			var level pb.GetRequest_ConsistencyLevel
			switch strings.ToUpper(consistency) {
			case "STRONG":
				level = pb.GetRequest_STRONG
			case "FAST":
				level = pb.GetRequest_FAST
			case "EVENTUAL":
				level = pb.GetRequest_EVENTUAL
			default:
				return fmt.Errorf("invalid consistency level. Must be STRONG, FAST, or EVENTUAL")
			}

			req := &pb.GetRequest{
				Key:         args[0],
				Consistency: level,
			}

			resp, err := client.Get(ctx, req)
			if err != nil {
				return fmt.Errorf("rpc failed: %w", err)
			}

			if resp.Found {
				fmt.Printf("%s\n", string(resp.Value))
			} else {
				fmt.Println("(nil) - key not found")
			}
			return nil
		},
	}
	getCmd.Flags().StringVarP(&consistency, "consistency", "c", "STRONG", "Consistency level (STRONG, FAST, EVENTUAL)")

	deleteCmd := &cobra.Command{
		Use:   "delete [key]",
		Short: "Remove a key-value pair",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := getClient()
			if err != nil {
				return err
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			req := &pb.DeleteRequest{
				Key: args[0],
			}

			resp, err := client.Delete(ctx, req)
			if err != nil {
				return fmt.Errorf("rpc failed: %w", err)
			}

			if resp.Success {
				fmt.Println("OK")
			} else {
				fmt.Println("FAILED")
			}
			return nil
		},
	}

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster status and current leader",

		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := getClient()
			if err != nil {
				return err
			}

			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

			defer cancel()

			// Use a STRONG get to surface leader info
			resp, err := client.Get(ctx, &pb.GetRequest{
				Key:         "__status__",
				Consistency: pb.GetRequest_EVENTUAL,
			})
			if err != nil {
				return fmt.Errorf("rpc failed: %w", err)
			}

			fmt.Printf("Leader: %s\n", resp.LeaderAddress)

			return nil
		},
	}

	rootCmd.AddCommand(putCmd, getCmd, deleteCmd, statusCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
