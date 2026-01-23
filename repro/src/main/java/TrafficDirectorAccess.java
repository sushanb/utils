import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.util.JsonFormat;
import io.envoyproxy.envoy.config.core.v3.Locality;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.extensions.filters.http.fault.v3.HTTPFault;
import io.envoyproxy.envoy.extensions.filters.http.router.v3.Router;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.envoyproxy.envoy.service.discovery.v3.AggregatedDiscoveryServiceGrpc;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.alts.GoogleDefaultChannelCredentials;
import io.grpc.lookup.v1.RouteLookupClusterSpecifier;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * A standalone caller that sends a specific xDS DiscoveryRequest to the Google Traffic Director
 * control plane (directpath-pa.googleapis.com).
 *
 * <p><b>Prerequisites:</b> This code requires the Envoy Protocol Buffer dependencies (e.g.,
 * 'io.envoyproxy.controlplane:api'). It uses Google Default Credentials, so it should be run in an
 * environment with ALTS (like a GCP VM) or appropriate credentials.
 */
public class TrafficDirectorAccess {
  private static final Logger LOG = Logger.getLogger(TrafficDirectorAccess.class.getName());

  // The target control plane
  private static final String TARGET = "directpath-pa.googleapis.com:443";

  // Configuration Constants from your request
  private static final String NODE_ID = "C2P-1315887034";
  private static final String ZONE = "us-central1-a";
  private static final String USER_AGENT_NAME = "gRPC Java";
  private static final String USER_AGENT_VERSION = "1.78.0-SNAPSHOT";
  private static final String RESOURCE_NAME =
      "xdstp://traffic-director-c2p.xds.googleapis.com/envoy.config.listener.v3.Listener/bigtable.googleapis.com";
  private static final String TYPE_URL = "type.googleapis.com/envoy.config.listener.v3.Listener";

  public static void main(String[] args) throws InterruptedException {
    ChannelCredentials channelCredentials = GoogleDefaultChannelCredentials.create();
    ManagedChannel channel = Grpc.newChannelBuilder(TARGET, channelCredentials).build();

    try {
      LOG.info("Connecting to " + TARGET);

      // 2. Create the Async Stub for Aggregated Discovery Service (ADS)
      AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceStub stub =
          AggregatedDiscoveryServiceGrpc.newStub(channel);

      // Latch to keep the program running until we receive a response or error
      CountDownLatch latch = new CountDownLatch(1);

      // 3. Define the Response Observer
      StreamObserver<DiscoveryResponse> responseObserver =
          new StreamObserver<DiscoveryResponse>() {
            @Override
            public void onNext(DiscoveryResponse value) {
              LOG.info("Received DiscoveryResponse:");
              try {
                // Register types for pretty printing (so nested Any fields don't show as bytes)
                JsonFormat.TypeRegistry registry =
                    JsonFormat.TypeRegistry.newBuilder()
                        .add(Listener.getDescriptor())
                        .add(HttpConnectionManager.getDescriptor())
                        .add(Router.getDescriptor())
                        .add(HTTPFault.getDescriptor())
                        .add(RouteLookupClusterSpecifier.getDescriptor())
                        .build();

                JsonFormat.Printer printer =
                    JsonFormat.printer().usingTypeRegistry(registry).includingDefaultValueFields();

                for (Any any : value.getResourcesList()) {
                  // Check if the resource is specifically a Listener
                  if (any.is(Listener.class)) {
                    Listener listener = any.unpack(Listener.class);
                    System.out.println("--------------------------------------------------");
                    System.out.println("Parsed Listener JSON:");
                    System.out.println(printer.print(listener));
                    System.out.println("--------------------------------------------------");
                  } else {
                    LOG.info("Skipping non-Listener resource: " + any.getTypeUrl());
                  }
                }
              } catch (Exception e) {
                LOG.severe("Error parsing response: " + e.getMessage());
                e.printStackTrace();
              }
              latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
              LOG.severe("RPC Error: " + t.getMessage());
              t.printStackTrace();
              latch.countDown();
            }

            @Override
            public void onCompleted() {
              LOG.info("Stream completed.");
              latch.countDown();
            }
          };

      // 4. Start the Bidirectional Stream
      StreamObserver<DiscoveryRequest> requestObserver =
          stub.streamAggregatedResources(responseObserver);

      // 5. Construct the Node
      Node node =
          Node.newBuilder()
              .setId(NODE_ID)
              .setLocality(Locality.newBuilder().setZone(ZONE).build())
              .setUserAgentName(USER_AGENT_NAME)
              .setUserAgentVersion(USER_AGENT_VERSION)
              .addAllClientFeatures(
                  ImmutableList.of(
                      "envoy.lb.does_not_support_overprovisioning", "xds.config.resource-in-sotw"))
              .build();

      // 6. Construct the DiscoveryRequest
      DiscoveryRequest request =
          DiscoveryRequest.newBuilder()
              .setNode(node)
              .setTypeUrl(TYPE_URL)
              .addResourceNames(RESOURCE_NAME)
              .build();

      LOG.info("Sending DiscoveryRequest for: " + RESOURCE_NAME);

      // 7. Send the request
      requestObserver.onNext(request);

      // 8. Wait for the response
      if (!latch.await(30, TimeUnit.SECONDS)) {
        LOG.warning("Timed out waiting for response.");
      }

      // Complete the stream from client side
      requestObserver.onCompleted();

    } finally {
      channel.shutdownNow();
    }
  }
}
