package main

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/scylladb/scylla-cdc-go"
	"github.com/spf13/cobra"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var wg sync.WaitGroup

type BenchmarkConsumer struct {
	sum *int64
}

func (bc BenchmarkConsumer) Consume(_ context.Context, change scyllacdc.Change) error {
	for _, rowChange := range change.Delta {
		val, _ := rowChange.GetValue("ck")
		v := val.(*int64)
		atomic.AddInt64(bc.sum, *v)
		wg.Done()
	}
	return nil
}

func (bc BenchmarkConsumer) End() error {
	return nil
}

type BenchmarkConsumerFactory struct {
	sum *int64
}

func (bcf BenchmarkConsumerFactory) CreateChangeConsumer(ctx context.Context, input scyllacdc.CreateChangeConsumerInput) (scyllacdc.ChangeConsumer, error) {
	return BenchmarkConsumer{sum: bcf.sum}, nil
}

func makeBenchmarkConsumerFactory(sum *int64, wg *sync.WaitGroup) BenchmarkConsumerFactory {
	return BenchmarkConsumerFactory{sum: sum}
}

func run(cmd *cobra.Command, args []string) {
	sum := int64(0)
	wg = sync.WaitGroup{}
	wg.Add(rowsCount)

	factory := makeBenchmarkConsumerFactory(&sum, &wg)

	ctx := context.TODO()

	cluster := gocql.NewCluster(hostname)
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Timeout = time.Second * 10
	cluster.ConnectTimeout = time.Second * 10
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	time.Sleep(time.Second * 10)

	cfg := scyllacdc.ReaderConfig{
		Session:               session,
		TableNames:            []string{keyspace + "." + table},
		ChangeConsumerFactory: factory,
		Advanced: scyllacdc.AdvancedReaderConfig{
			ConfidenceWindowSize:   time.Duration(int64(windowSize * math.Pow10(9))),
			QueryTimeWindowSize:    time.Duration(int64(windowSize * math.Pow10(9))),
			ChangeAgeLimit:         time.Hour * 24 * 7,
			PostEmptyQueryDelay:    time.Duration(int64(sleepInterval * math.Pow10(9))),
			PostFailedQueryDelay:   time.Duration(int64(sleepInterval * math.Pow10(9))),
			PostNonEmptyQueryDelay: time.Duration(int64(sleepInterval * math.Pow10(9))),
		},
	}

	reader, err := scyllacdc.NewReader(ctx, &cfg)
	if err != nil {
		panic(err)
	}

	go func() {
		wg.Wait()
		fmt.Println("Scylla-cdc-rust has read ", rowsCount, " rows! The checksum is ", sum, ".")
		reader.Stop()
	}()

	err = reader.Run(ctx)
	if err != nil {
		panic(err)
	}
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

	rootCmd.PersistentFlags().Float64Var(&windowSize, "window-size", 0, "window size")
	rootCmd.PersistentFlags().Float64Var(&safetyInterval, "safety-interval", 0, "safety interval")
	rootCmd.PersistentFlags().Float64Var(&sleepInterval, "sleep-interval", 0.001, "sleep interval")

	rootCmd.PersistentFlags().IntVar(&rowsCount, "rows-count", 0, "rows count")
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		panic(err)
	}
}
