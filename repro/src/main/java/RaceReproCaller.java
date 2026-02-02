import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.BigtableGrpc.BigtableBlockingStub;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.alts.GoogleDefaultChannelCredentials;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Race condition reproduction tool.
 * Creates multiple channels concurrently and shuts them down randomly to trigger
 * race conditions between xDS updates and channel termination.
 */
public class RaceReproCaller {
    private static final Logger LOG = Logger.getLogger(RaceReproCaller.class.getName());

    // Number of concurrent channels to churn
    private static final int NUM_CHANNELS = 10;

    public static void main(String[] args) throws IOException, InterruptedException {
        String projectId = System.getProperty("bigtable.project");
        String instanceId = System.getProperty("bigtable.instance");
        String tableId = System.getProperty("bigtable.table");

        if (projectId == null || instanceId == null || tableId == null) {
            System.err.println("Please provide -Dbigtable.project, -Dbigtable.instance, and -Dbigtable.table");
            return;
        }

        String TABLE_NAME =
                String.format("projects/%s/instances/%s/tables/%s", projectId, instanceId, tableId);

        String host = "bigtable.googleapis.com";
        String target = "google-c2p:///" + host;

        ChannelCredentials channelCredentials = GoogleDefaultChannelCredentials.create();

        // Executor to manage the parallel tests
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CHANNELS);
        CountDownLatch doneLatch = new CountDownLatch(NUM_CHANNELS);

        LOG.info("Starting Race Condition Repro with " + NUM_CHANNELS + " channels...");

        for (int i = 0; i < NUM_CHANNELS; i++) {
            final int channelId = i;
            executor.submit(() -> {
                try {
                    runRaceTest(channelId, target, channelCredentials, TABLE_NAME);
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "Unexpected error in test runner " + channelId, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Wait for all channels to finish their cycle
        doneLatch.await();
        executor.shutdown();
        LOG.info("All race tests completed.");
    }

    private static void runRaceTest(
            int id,
            String target,
            ChannelCredentials creds,
            String tableName) {

        ManagedChannel channel = Grpc.newChannelBuilder(target, creds).build();
        LOG.info(String.format("[Chan-%d] Created.", id));

        ReadRowsRequest request =
                ReadRowsRequest.newBuilder().setTableName(tableName).setRowsLimit(1).build();

        // We start the RPC in a separate thread because we want to call shutdown()
        // on the main thread while the RPC (and internal xDS setup) is in progress.
        Thread rpcThread = new Thread(() -> {
            try {
                BigtableBlockingStub stub =
                        BigtableGrpc.newBlockingStub(channel)
                                .withDeadlineAfter(60, TimeUnit.SECONDS);

                LOG.info(String.format("[Chan-%d] Sending request to trigger xDS...", id));
                Iterator<ReadRowsResponse> it = stub.readRows(request);
                while (it.hasNext()) {
                    it.next(); // Consume to keep stream open
                }
            } catch (StatusRuntimeException e) {
                // Expected: CANCELLED or UNAVAILABLE often happens during aggressive shutdown
                LOG.info(String.format("[Chan-%d] RPC Status: %s (Expected during shutdown)", id, e.getStatus().getCode()));
            } catch (Exception e) {
                // If we see IllegalStateException here, that might be the bug!
                if (e instanceof IllegalStateException && e.getMessage().contains("Channel is being terminated")) {
                    LOG.log(Level.SEVERE, String.format("[Chan-%d] !!! RACE CONDITION REPRODUCED !!!", id), e);
                } else {
                    LOG.log(Level.WARNING, String.format("[Chan-%d] RPC failed (Expected if headers missing/shutdown)", id), e);
                }
            }
        });

        rpcThread.start();

        // THE RACE TRIGGER:
        // Sleep for a random amount of time (0 to 100ms) to attempt to interrupt
        // the channel at different stages of the xDS update / LB creation process.
        try {
            int sleepMs = new Random().nextInt(100);
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        LOG.info(String.format("[Chan-%d] Shutting down NOW...", id));
        try {
            // This is the call that throws the exception in the bug report
            // if called exactly when the LB is creating a subchannel.
            channel.shutdown();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, String.format("[Chan-%d] Exception during shutdown call", id), e);
        }
    }
}