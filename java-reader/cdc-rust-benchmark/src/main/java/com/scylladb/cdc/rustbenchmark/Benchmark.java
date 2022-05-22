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

public class Benchmark {
    public static void main(String[] args) {
        Namespace parsedArguments = parseArguments(args);
        String source = parsedArguments.getString("source");
        String keyspace = parsedArguments.getString("keyspace"), table = parsedArguments.getString("table");
        int rowCount = Integer.parseInt(parsedArguments.getString("rowcount"));
        CountDownLatch terminationLatch = new CountDownLatch(rowCount);

        RawChangeConsumerProvider changeConsumerProvider = threadId -> {
            return change -> {
                terminationLatch.countDown();
                return CompletableFuture.completedFuture(null);
            };
        };

        try (CDCConsumer consumer = CDCConsumer.builder()
                .addContactPoint(source)
                .addTable(new TableName(keyspace, table))
                .withConsumerProvider(changeConsumerProvider)
                .build()) {
            consumer.start();

            terminationLatch.await();
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

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
            return null;
        }
    }
}