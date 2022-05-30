package com.scylladb.cdc.rustbenchmark;

import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.lib.RawChangeConsumerProvider;
import com.scylladb.cdc.model.TableName;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class Benchmark {
    public static void main(String[] args) {
        Namespace parsedArguments = parseArguments(args);
        String source = parsedArguments.getString("source");
        String keyspace = parsedArguments.getString("keyspace"), table = parsedArguments.getString("table");
        int rowCount = Integer.parseInt(parsedArguments.getString("rowcount"));
        long windowSize = Long.parseLong(parsedArguments.getString("window"));

        AtomicLong checksum = new AtomicLong(0);
        CountDownLatch terminationLatch = new CountDownLatch(rowCount);

        RawChangeConsumerProvider changeConsumerProvider = threadId -> {
            return change -> {
                checksum.updateAndGet(val -> val + change.getCell("ck").getLong());
                terminationLatch.countDown();
                return CompletableFuture.completedFuture(null);
            };
        };

        try (CDCConsumer consumer = CDCConsumer.builder()
                .addContactPoint(source)
                .addTable(new TableName(keyspace, table))
                .withConsumerProvider(changeConsumerProvider)
                .withQueryTimeWindowSizeMs(windowSize)
                .build()) {
            consumer.start();

            terminationLatch.await();
            System.out.println("Scylla-cdc-java has read " + rowCount + " rows. The checksum is " + checksum.get() + ".");
        } catch (InterruptedException ex) {
            System.err.println("Exception occurred while running the Benchmark: "
                    + ex.getMessage());
        }
    }

    private static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("./cdc-rust-benchmark").build().defaultHelp(true);
        parser.addArgument("-k", "--keyspace").required(true).help("Keyspace name");
        parser.addArgument("-t", "--table").required(true).help("Table name");
        parser.addArgument("-s", "--source").required(true)
                .setDefault("127.0.0.1").help("Address of a node in source cluster");
        parser.addArgument("-c", "--rowcount").required(true).help("Number of rows to read.");
        parser.addArgument("-w", "--window").required(true).help("Size of the time window in milliseconds.");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
            return null;
        }
    }
}