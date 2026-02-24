/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sushanb;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.threeten.bp.Duration;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class AyncFutureBigtable {
    private static final Logger logger = Logger.getLogger(AyncFutureBigtable.class.getName());

    public static BigtableDataClient createClient() throws IOException {
        BigtableDataSettings.Builder settingsBuilder =
                BigtableDataSettings.newBuilder().setProjectId("autonomous-mote-782").setInstanceId("test-sushanb");

        // Set a 1-minute total timeout for all readRow operations
        Duration timeout = Duration.ofSeconds(60);
        RetrySettings retrySettings = settingsBuilder.stubSettings().readRowSettings().getRetrySettings()
                .toBuilder()
                .setTotalTimeout(timeout)
                .setMaxRpcTimeout(timeout) // Limits the timeout of individual retry attempts to 1m as well
                .build();

        settingsBuilder.stubSettings().readRowSettings().setRetrySettings(retrySettings);

        settingsBuilder
                .stubSettings()
                .setEndpoint("test-bigtable.sandbox.googleapis.com:443").build();

        BigtableDataSettings settings = settingsBuilder.build();
        BigtableDataClient client = BigtableDataClient.create(settings);
        logger.info("BigtableDataClient created successfully.");
        return client;
    }

    public static void performAsyncFuture(BigtableDataClient client, String tableId, String rowKeyString) {
        // 1. Prepare TargetId, ByteString, and Filter
        TableId targetId = TableId.of(tableId);
        ByteString rowKey = ByteString.copyFromUtf8(rowKeyString);

        // Using a pass-through filter for the dummy app, which returns all data in the row
        Filter filter = Filters.FILTERS.pass();

        // 2. Make the async call
        ApiFuture<Row> apiFuture = client.readRowAsync(targetId, rowKey, filter);

        // 3. Create the CompletableFuture bridge
        CompletableFuture<Row> completableFuture = new CompletableFuture<>();

        ApiFutures.addCallback(
                apiFuture,
                new ApiFutureCallback<Row>() {
                    @Override
                    public void onSuccess(Row result) {
                        completableFuture.complete(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        completableFuture.completeExceptionally(t);
                    }
                },
                MoreExecutors.directExecutor()
        );

        // 4. Attach the final CompletableFuture callback
        completableFuture.whenComplete((row, throwable) -> {
            if (throwable != null) {
                // Feature Request: Check if it's a Bigtable/RPC timeout
                if (throwable.getMessage() != null && throwable.getMessage().contains("DEADLINE_EXCEEDED")) {
                    logger.severe("READ_TIMEOUT: Bigtable readRow operation timed out after the configured 60 seconds.");
                } else {
                    logger.severe("API_ERROR: Failed to read row due to another error: " + throwable.getMessage());
                }
            } else if (row != null) {
                logger.info("Successfully read row! Key: " + row.getKey().toStringUtf8());
            } else {
                logger.info("Row not found.");
            }
        });

        logger.info("Async read initiated for row: " + rowKeyString);
    }

    public static void main(String[] args) {
        try (BigtableDataClient client = createClient()) {

            String tableId = "sushanb";
            String rowKey = "test-row-key-1";

            // Execute the async logic
            performAsyncFuture(client, tableId, rowKey);

            // Sleep briefly to prevent the dummy app from exiting before the async callback fires
            try {
                Thread.sleep(Duration.ofMinutes(5).toMillis());
                // Feature Request: Log when the thread sleep finishes naturally
                logger.info("MAIN_TIMEOUT: Main thread completed its 5-minute sleep. The dummy application will now exit.");
            } catch (InterruptedException e) {
                logger.warning("MAIN_INTERRUPTED: Main thread sleep was interrupted prematurely.");
                Thread.currentThread().interrupt(); // Restore interrupted status
            }

        } catch (Exception e) {
            logger.severe("Application encountered a top-level error: " + e.getMessage());
        }
    }
}
