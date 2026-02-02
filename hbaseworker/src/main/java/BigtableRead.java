package com.example;

import org.apache.hadoop.conf.Configuration;
import com.google.common.util.concurrent.RateLimiter;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class BigtableRead {
    private static final String TABLE_NAME = "repro";
    private static final String COLUMN_FAMILY = "size";
    private static final String APP_PROFILE = "default";
    // Configuration
    private static final int THREAD_COUNT = 10;
    private static final double TOTAL_QPS = 20.0;
    private static final AtomicLong totalRequests = new AtomicLong(0);
    public static void main(String[] args) {
        String projectId = System.getProperty("bigtable.project");
        String instanceId = System.getProperty("bigtable.instance");

        final RateLimiter rateLimiter = RateLimiter.create(TOTAL_QPS);
        final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // BigtableConfiguration connects using your local Application Default Credentials (gcloud auth application-default login)
        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId, APP_PROFILE )) {
            for (int i = 0; i < THREAD_COUNT; i++) {
                int threadId = i;
                executor.submit(() -> {
                    runWorker(connection, TABLE_NAME, rateLimiter, threadId);
                });
            }

            // 4. Monitoring Loop (Main thread just reports status)
            while (!executor.isTerminated()) {
                Thread.sleep(5000);
                System.out.printf("[Status] Total requests sent: %d%n", totalRequests.get());
            }

        } catch (IOException | InterruptedException e) {
            System.err.println("Connection error:");
            e.printStackTrace();
        } finally {
            // Clean up executor if main thread exits
            executor.shutdownNow();
        }
    }

    private static void runWorker(Connection connection, String tableName, RateLimiter rateLimiter, int threadId) {
        // Best Practice: Create a Table instance per thread (it's lightweight but not thread-safe)
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {

            while (!Thread.currentThread().isInterrupted()) {
                // Block until a permit is available (throttles this thread)
                rateLimiter.acquire();

                // Generate Data
                String rowKey = "user-" + UUID.randomUUID();
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(
                        Bytes.toBytes(COLUMN_FAMILY),
                        Bytes.toBytes("thread_id"),
                        Bytes.toBytes(String.valueOf(threadId))
                );
                put.addColumn(
                        Bytes.toBytes(COLUMN_FAMILY),
                        Bytes.toBytes("timestamp"),
                        Bytes.toBytes(String.valueOf(System.currentTimeMillis()))
                );

                // Send Request
                table.put(put);

                // Increment shared counter
                totalRequests.incrementAndGet();
            }

        } catch (IOException e) {
            System.err.printf("Thread %d encountered an IO error: %s%n", threadId, e.getMessage());
        } catch (Exception e) {
            System.err.printf("Thread %d encountered an error: %s%n", threadId, e.getMessage());
        }
    }
}
