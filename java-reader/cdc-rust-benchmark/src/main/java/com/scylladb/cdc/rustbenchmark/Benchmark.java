package com.scylladb.cdc.rustbenchmark;

import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.lib.RawChangeConsumerProvider;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChangeConsumer;

import java.util.concurrent.CompletableFuture;

/**
 * Hello world!
 *
 */
public class Benchmark {
    public static void main(String[] args) {

        // Build a provider of consumers. The CDCConsumer instance
        // can be run in multi-thread setting and a separate
        // RawChangeConsumer is used by each thread.
        RawChangeConsumerProvider changeConsumerProvider = threadId -> {
            // Build a consumer of changes. You should provide
            // a class that implements the RawChangeConsumer
            // interface.
            //
            // Here, we use a lambda for simplicity.
            //
            // The consume() method of RawChangeConsumer returns
            // a CompletableFuture, so your code can perform
            // some I/O responding to the change.
            //
            // The RawChange name alludes to the fact that
            // changes represented by this class correspond
            // 1:1 to rows in *_scylla_cdc_log table.
            return change -> CompletableFuture.completedFuture(null);
        };

        // Build a CDCConsumer, which is single-threaded
        // (workersCount(1)), reads changes
        // from [keyspace].[table] and passes them
        // to consumers created by changeConsumerProvider.
        try (CDCConsumer consumer = CDCConsumer.builder()
                .addContactPoint("172.17.0.2:9042")
                .addTable(new TableName("ks", "t1"))
                .withConsumerProvider(changeConsumerProvider)
                .withWorkersCount(1)
                .build()) {

            // Start a consumer. You can stop it by using .stop() method
            // or it can be automatically stopped when created in a
            // try-with-resources (as shown above).
            consumer.start();

            // The consumer is started in background threads.
            // It is consuming the CDC log and providing read changes
            // to the consumers.

            // Wait for SIGINT:
            CountDownLatch terminationLatch = new CountDownLatch(1);
            Signal.handle(new Signal("INT"), signal -> terminationLatch.countDown());
            terminationLatch.await();
        } catch (InterruptedException ex) {
            System.err.println("Exception occurred while running the Printer: "
                    + ex.getMessage());
        }

        // The CDCConsumer is gracefully stopped after try-with-resources.
    }
}
