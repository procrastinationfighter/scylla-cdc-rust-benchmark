package main

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/scylladb/scylla-cdc-go"
	"github.com/spf13/cobra"
	"math"
	"time"
)

type BenchmarkConsumer struct {
	ch chan int
}

func (bc BenchmarkConsumer) Consume(_ context.Context, change scyllacdc.Change) error {
	for _, rowChange := range change.Delta {
		val, _ := rowChange.GetValue("ck")
		v := val.(int)
		bc.ch <- v
	}
	return nil
}

func (bc BenchmarkConsumer) End() error {
	return nil
}

type BenchmarkConsumerFactory struct {
	ch chan int
}

func (bcf BenchmarkConsumerFactory) CreateChangeConsumer(ctx context.Context, input scyllacdc.CreateChangeConsumerInput) (scyllacdc.ChangeConsumer, error) {
	return BenchmarkConsumer{}, nil
}

func makeBenchmarkConsumerFactory(ch chan int) BenchmarkConsumerFactory {
	return BenchmarkConsumerFactory{ch: ch}
}

func run(cmd *cobra.Command, args []string) {
	checksumChan := make(chan int, 100)

	factory := makeBenchmarkConsumerFactory(checksumChan)

	ctx := context.TODO()

	cluster := gocql.NewCluster(hostname)
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	cfg := scyllacdc.ReaderConfig{
		Session:               session,
		TableNames:            []string{keyspace + "." + table},
		ChangeConsumerFactory: factory,
		Advanced: scyllacdc.AdvancedReaderConfig{
			ConfidenceWindowSize: time.Duration(int64(windowSize * math.Pow10(9))),
		},
	}

	reader, err := scyllacdc.NewReader(ctx, &cfg)
	if err != nil {
		panic(err)
	}

	go func() {
		err := reader.Run(ctx)
		if err != nil {
			panic(err)
		}
	}()
	checksum := 0

	for i := 0; i < rowsCount; i++ {
		x := <-checksumChan
		checksum += x
	}

	fmt.Println("Scylla-cdc-rust has read ", rowsCount, " rows! The checksum is ", checksum, ".")
	reader.Stop()
}

var rootCmd = &cobra.Command{Use: "app", Run: run}
var keyspace string
var table string
var hostname string
var windowSize float64
var safetyInterval float64
var sleepInterval float64
var rowsCount int

func init() {
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for this command")

	rootCmd.PersistentFlags().StringVarP(&keyspace, "keyspace", "k", "", "keyspace")
	rootCmd.PersistentFlags().StringVarP(&table, "table", "t", "", "table")
	rootCmd.PersistentFlags().StringVarP(&hostname, "hostname", "h", "", "hostname")

	rootCmd.PersistentFlags().Float64Var(&windowSize, "window_size", 0, "window size")
	rootCmd.PersistentFlags().Float64Var(&safetyInterval, "safety_interval", 0, "safety interval")
	rootCmd.PersistentFlags().Float64Var(&sleepInterval, "sleep_interval", 0, "sleep interval")

	rootCmd.PersistentFlags().IntVar(&rowsCount, "rows_count", 0, "rows count")
}

func main() {
	rootCmd.Execute()
}
