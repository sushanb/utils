import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.BigtableGrpc.BigtableStub;
import com.google.bigtable.v2.FeatureFlags;
import com.google.bigtable.v2.PingAndWarmRequest;
import com.google.bigtable.v2.PingAndWarmResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.alts.GoogleDefaultChannelCredentials;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SingleChannelCaller {
    private static final Logger LOG = Logger.getLogger(SingleChannelCaller.class.getName());

    private static final Metadata.Key<String> REQUEST_PARAMS_KEY =
            Key.of("x-goog-request-params", Metadata.ASCII_STRING_MARSHALLER);
    private static final Key<String> FEATURE_FLAGS_KEY =
            Key.of("bigtable-features", Metadata.ASCII_STRING_MARSHALLER);

    // Matches your ctx.Done() check to cleanly stop the recursive callbacks
    private static volatile boolean isShuttingDown = false;

    public static void main(String[] args) throws IOException, InterruptedException {
        String projectId = System.getenv("PROJECT_ID");
        if (projectId == null || projectId.trim().isEmpty()) {
            projectId = System.getProperty("bigtable.project");
            if (projectId == null || projectId.trim().isEmpty()) {
                LOG.severe("PROJECT_ID environment variable is missing! Exiting...");
                System.exit(1);
            }
        }

        String instanceId = System.getenv("INSTANCE_ID");
        if (instanceId == null || instanceId.trim().isEmpty()) {
            instanceId = System.getProperty("bigtable.instance");
            if (instanceId == null || instanceId.trim().isEmpty()) {
                LOG.severe("INSTANCE_ID environment variable is missing! Exiting...");
                System.exit(1);
            }
        }

        String hostEnv = System.getenv("BIGTABLE_HOST");
        String host =
                (hostEnv != null && !hostEnv.trim().isEmpty()) ? hostEnv : "bigtable.googleapis.com";

        int inFlight = 1;
        String inFlightEnv = System.getenv("SINGLE_CHANNEL_IN_FLIGHT");
        if (inFlightEnv != null && !inFlightEnv.trim().isEmpty()) {
            try {
                inFlight = Integer.parseInt(inFlightEnv);
            } catch (NumberFormatException e) {
                LOG.warning("Invalid SINGLE_CHANNEL_IN_FLIGHT env var, falling back to default 1");
            }
        }

        String appProfileEnv = System.getenv("APP_PROFILE");
        String appProfile =
                (appProfileEnv != null && !appProfileEnv.trim().isEmpty()) ? appProfileEnv : "default";

        String INSTANCE_NAME = String.format("projects/%s/instances/%s", projectId, instanceId);

        ChannelCredentials channelCredentials = GoogleDefaultChannelCredentials.create();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FeatureFlags featureFlags =
                FeatureFlags.newBuilder()
                        .setTrafficDirectorEnabled(true)
                        .setDirectAccessRequested(true)
                        .build();

        featureFlags.writeTo(baos);
        String encodedFeatureFlags = new String(Base64.getUrlEncoder().encode(baos.toByteArray()));
        String requestParams =
                String.format(
                        "name=%s&app_profile_id=%s",
                        URLEncoder.encode(INSTANCE_NAME, StandardCharsets.UTF_8),
                        URLEncoder.encode(appProfile, StandardCharsets.UTF_8));

        ClientInterceptor interceptor =
                new ClientInterceptor() {
                    @Override
                    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

                        return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                            @Override
                            public void start(Listener<RespT> responseListener, Metadata headers) {
                                headers.put(FEATURE_FLAGS_KEY, encodedFeatureFlags);
                                headers.put(REQUEST_PARAMS_KEY, requestParams);
                                super.start(responseListener, headers);
                            }
                        };
                    }
                };

        PingAndWarmRequest request =
                PingAndWarmRequest.newBuilder().setName(INSTANCE_NAME).setAppProfileId(appProfile).build();

        LOG.info("Creating single channel to target: " + host);
        LOG.info("Using App Profile: " + appProfile);
        LOG.info("Using Instance Name: " + INSTANCE_NAME);

        ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
        ManagedChannel channel =
                Grpc.newChannelBuilder(host, channelCredentials)
                        .intercept(interceptor)
                        .maxInboundMessageSize(256 * 1024 * 1024)
                       .executor(virtualExecutor)
                        .build();

        BigtableStub asyncStub = BigtableGrpc.newStub(channel);

        LOG.info("Starting " + inFlight + " async streams to maintain continuous in-flight requests...");

        // Kick off the initial burst of async requests
        for (int i = 0; i < inFlight; i++) {
            fireAsyncRequest(asyncStub, request, i);
        }

        // Latch to keep the main thread alive since we don't have a blocking executor anymore
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        // Shutdown hook to cleanly cancel and wait
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    LOG.info("Shutting down workers and channel...");
                                    isShuttingDown = true; // Signal the callbacks to stop firing
                                    shutdownLatch.countDown(); // Unblock the main thread
                                    try {
                                        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                    LOG.info("Clean shutdown complete.");
                                }));

        // Block the main thread until Ctrl+C
        shutdownLatch.await();
    }

    /**
     * Recursive callback method. Fires a single async request and attaches listeners.
     * When the response arrives (or fails), it calls itself to keep the stream going.
     */
    private static void fireAsyncRequest(BigtableStub asyncStub, PingAndWarmRequest request, int workerID) {
        if (isShuttingDown) {
            return; // Stop the loop if shutting down
        }

        asyncStub.withDeadlineAfter(10, TimeUnit.SECONDS).pingAndWarm(request, new StreamObserver<PingAndWarmResponse>() {
            @Override
            public void onNext(PingAndWarmResponse response) {
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    LOG.log(Level.WARNING, "[Worker " + workerID + "] PingAndWarm() failed with status: " + ((StatusRuntimeException) t).getStatus());
                } else {
                    LOG.log(Level.WARNING, "[Worker " + workerID + "] Unexpected request error: " + t.getMessage());
                }
                // Fire next request immediately, even on failure
                fireAsyncRequest(asyncStub, request, workerID);
            }

            @Override
            public void onCompleted() {
                // Fire next request immediately on success
                fireAsyncRequest(asyncStub, request, workerID);
            }
        });
    }
}