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
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import io.grpc.alts.AltsContextUtil;
import io.grpc.alts.GoogleDefaultChannelCredentials;
import io.grpc.auth.MoreCallCredentials;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * mvn compile exec:java -Dexec.mainClass=SingleChannelCaller \
 * -Dbigtable.project="" \
 * -Dbigtable.instance="" \
 * -Dbigtable.table=""
 */

public class SingleChannelCaller {
  private static final Logger LOG = Logger.getLogger(SingleChannelCaller.class.getName());

  private static final Metadata.Key<String> REQUEST_PARAMS_KEY =
      Key.of("x-goog-request-params", Metadata.ASCII_STRING_MARSHALLER);
  private static final Key<String> FEATURE_FLAGS_KEY =
      Key.of("bigtable-features", Metadata.ASCII_STRING_MARSHALLER);

  public static void main(String[] args) throws IOException {
    String projectId = System.getProperty("bigtable.project");
    String instanceId = System.getProperty("bigtable.instance");
    String tableId = System.getProperty("bigtable.table");

    String TABLE_NAME =
        String.format("projects/%s/instances/%s/tables/%s", projectId, instanceId, tableId);

    String host = "bigtable.googleapis.com";

    // DirectPath specific
    String target = "google-c2p:///" + host;

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
            "table_name=%s&app_profile_id=%s",
            URLEncoder.encode(TABLE_NAME, StandardCharsets.UTF_8),
            URLEncoder.encode("default", StandardCharsets.UTF_8));

    // Create the interceptor once
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

                final ClientCall<ReqT, RespT> thisCall = this;
                // Wrap the listener to intercept onHeaders
                Listener<RespT> forwardingListener =
                    new SimpleForwardingClientCallListener<>(responseListener) {
                      @Override
                      public void onHeaders(Metadata headers) {
                        boolean altsCheckPassed = false;
                        try {
                          LOG.info("Checking AltsContextUtil on attributes during onHeaders...");
                          // Verify ALTS context is present
                          if (AltsContextUtil.check(thisCall.getAttributes())) {
                            altsCheckPassed = true;
                          }
                        } catch (Exception e) {
                          LOG.warning("AltsContextUtil check failed: " + e.getMessage());
                        }
                        LOG.info("ALTS check: " + altsCheckPassed);
                        super.onHeaders(headers);
                      }
                    };

                super.start(forwardingListener, headers);
              }
            };
          }
        };
    ReadRowsRequest request =
        ReadRowsRequest.newBuilder().setTableName(TABLE_NAME).setRowsLimit(1).build();

    // Create the single channel
    LOG.info("Creating single channel");
    ManagedChannel channel = Grpc.newChannelBuilder(target, channelCredentials).build();

    try {
      BigtableBlockingStub stub =
          BigtableGrpc.newBlockingStub(channel)
              .withInterceptors(interceptor)
              .withDeadlineAfter(60, TimeUnit.SECONDS);

      LOG.info("About to send Request");
      try {
        Iterator<ReadRowsResponse> it = stub.readRows(request);
        while (it.hasNext()) {
          LOG.info("Response: " + it.next());
        }
        LOG.info("Done with request");
      } catch (Exception e) {
        e.printStackTrace();
        LOG.info("Error during request");
      }
    } finally {
      LOG.info("Shutting down channel");
      channel.shutdown();
      LOG.info("Channel shut down.");
    }
  }
}
