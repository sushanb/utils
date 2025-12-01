import com.google.auth.oauth2.GoogleCredentials;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.BigtableGrpc.BigtableBlockingStub;
import com.google.bigtable.v2.FeatureFlags;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import io.grpc.CallCredentials;
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
import io.grpc.alts.GoogleDefaultChannelCredentials;
import io.grpc.auth.MoreCallCredentials;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;




public class Repro {
    private static final Logger LOG = Logger.getLogger(Repro.class.getName());

    private static final Metadata.Key<String> REQUEST_PARAMS_KEY =
            Key.of("x-goog-request-params", Metadata.ASCII_STRING_MARSHALLER);
    private static final Key<String> FEATURE_FLAGS_KEY =
            Key.of("bigtable-features", Metadata.ASCII_STRING_MARSHALLER);

    public static void main(String[] args) throws IOException {
        String projectId = System.getProperty("bigtable.project");
        String instanceId = System.getProperty("bigtable.instance");
        String tableId = System.getProperty("bigtable.table");

        String TABLE_NAME = String.format("projects/%s/instances/%s/tables/%s",
                projectId, instanceId, tableId);

        String host = "test-bigtable.sandbox.googleapis.com";

        // DirectPath specific
        String target = "google-c2p:///" + host;

        ChannelCredentials channelCredentials = GoogleDefaultChannelCredentials.create();

        // Common to DirectPath & CloudPath
        CallCredentials callCredentials = MoreCallCredentials.from(
                GoogleCredentials.getApplicationDefault());


        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FeatureFlags featureFlags =
                FeatureFlags.newBuilder()
                        .setTrafficDirectorEnabled(true)
                        .setDirectAccessRequested(true)
                        .build();

        featureFlags.writeTo(baos);
        String encodedFeatureFlags = new String(Base64.getUrlEncoder().encode(baos.toByteArray()));
        String requestParams = String.format(
                "table_name=%s&app_profile_id=%s",
                URLEncoder.encode(TABLE_NAME, StandardCharsets.UTF_8),
                URLEncoder.encode("default", StandardCharsets.UTF_8));

        // Create the interceptor once
        ClientInterceptor interceptor = new ClientInterceptor() {
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

        ReadRowsRequest request = ReadRowsRequest.newBuilder()
                .setTableName(TABLE_NAME)
                .setRowsLimit(1)
                .build();

        List<ManagedChannel> channels = new ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                LOG.info("Creating channel " + i);
                ManagedChannel channel = Grpc.newChannelBuilder(target, channelCredentials).build();
                channels.add(channel);

                BigtableBlockingStub stub = BigtableGrpc.newBlockingStub(channel)
                        .withCallCredentials(callCredentials)
                        .withInterceptors(interceptor)
                        .withDeadlineAfter(60, TimeUnit.SECONDS);


                LOG.info("About to send Request on channel " + i);
                try {
                    Iterator<ReadRowsResponse> it = stub.readRows(request);
                    while (it.hasNext()) {
                        LOG.info("[Channel " + i + "] Response: " + it.next());
                    }
                    LOG.info("Done with request on channel " + i);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.info("Error on channel " + i);
                }
            }
        } finally {
            LOG.info("Shutting down channels");
            for (ManagedChannel channel : channels) {
                channel.shutdown();
            }

            LOG.info("All channels shut down.");
        }
    }
}