import com.google.auth.oauth2.GoogleCredentials;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.BigtableGrpc.BigtableBlockingStub;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import io.grpc.CallCredentials;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.alts.AltsChannelCredentials;
import io.grpc.auth.MoreCallCredentials;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * mvn compile exec:java -Dexec.mainClass=SingleChannelCaller \ -Dbigtable.project="" \
 * -Dbigtable.instance="" \ -Dbigtable.table="" \ -Dbigtable.host="" \ -Dbigtable.port="" \
 */
public class SingleIPCaller {
  private static final Logger LOG = Logger.getLogger(SingleIPCaller.class.getName());

  public static void main(String[] args) throws IOException {
    String projectId = System.getProperty("bigtable.project");
    String instanceId = System.getProperty("bigtable.instance");
    String host = System.getProperty("bigtable.host");
    int port = Integer.getInteger("bigtable.port");
    String tableId = System.getProperty("bigtable.table");
    String TABLE_NAME =
        String.format("projects/%s/instances/%s/tables/%s", projectId, instanceId, tableId);

    // DirectPath specific
    ChannelCredentials channelCredentials = AltsChannelCredentials.create();
    ManagedChannel channel =
        Grpc.newChannelBuilderForAddress(host, port, channelCredentials).build();

    // Common to DirectPath & CloudPath
    CallCredentials callCredentials =
        MoreCallCredentials.from(GoogleCredentials.getApplicationDefault());

    ReadRowsRequest request =
        ReadRowsRequest.newBuilder().setTableName(TABLE_NAME).setRowsLimit(1).build();

    // Create the single channel
    LOG.info("Creating single channel");

    try {
      BigtableBlockingStub stub =
          BigtableGrpc.newBlockingStub(channel)
              .withCallCredentials(callCredentials)
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
