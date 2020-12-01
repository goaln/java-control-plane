package io.envoyproxy.controlplane.server;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Duration;
import io.envoyproxy.controlplane.cache.NodeGroup;
import io.envoyproxy.controlplane.cache.TestResources;
import io.envoyproxy.controlplane.cache.v2.SimpleCache;
import io.envoyproxy.controlplane.cache.v2.Snapshot;
import io.envoyproxy.envoy.api.v2.Cluster;
import io.envoyproxy.envoy.api.v2.core.Node;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;

public class TestXdsServer {

  private static final String GROUP = "key";

  /**
   * Example minimal xDS implementation using the java-control-plane lib. This example configures
   * a DiscoveryServer with a v2 cache, but handles v2 or v3 requests from data planes.
   *
   * @param arg command-line args
   */
  public static void main(String[] arg) throws IOException, InterruptedException {
    io.envoyproxy.controlplane.cache.v2.SimpleCache<String> cache = new SimpleCache<>(new NodeGroup<String>() {
      @Override public String hash(Node node) {
        return GROUP;
      }

      @Override public String hash(io.envoyproxy.envoy.config.core.v3.Node node) {
        return GROUP;
      }
    });

    cache.setSnapshot(
        GROUP,
        io.envoyproxy.controlplane.cache.v2.Snapshot.create(
            ImmutableList.of(
                io.envoyproxy.envoy.api.v2.Cluster.newBuilder()
                    .setName("cluster0")
                    .setConnectTimeout(Duration.newBuilder().setSeconds(5))
                    .setType(io.envoyproxy.envoy.api.v2.Cluster.DiscoveryType.STATIC)
                    .build()),
            ImmutableList.of(TestResources.createEndpoint("cluster0", "127.0.0.1", 1234)),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            "1"));

    V2DiscoveryServer discoveryServer = new V2DiscoveryServer(cache);
    V3DiscoveryServer v3DiscoveryServer = new V3DiscoveryServer(cache);

    ServerBuilder builder = NettyServerBuilder.forPort(12345)
        .addService(discoveryServer.getAggregatedDiscoveryServiceImpl())
        .addService(discoveryServer.getClusterDiscoveryServiceImpl())
        .addService(discoveryServer.getEndpointDiscoveryServiceImpl())
        .addService(discoveryServer.getListenerDiscoveryServiceImpl())
        .addService(discoveryServer.getRouteDiscoveryServiceImpl())
        .addService(v3DiscoveryServer.getAggregatedDiscoveryServiceImpl())
        .addService(v3DiscoveryServer.getClusterDiscoveryServiceImpl())
        .addService(v3DiscoveryServer.getEndpointDiscoveryServiceImpl())
        .addService(v3DiscoveryServer.getListenerDiscoveryServiceImpl())
        .addService(v3DiscoveryServer.getRouteDiscoveryServiceImpl());

    Server server = builder.build();

    server.start();

    System.out.println("Server has started on port " + server.getPort());

    Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));

    Thread.sleep(10000);

    cache.setSnapshot(
        GROUP,
        Snapshot.create(
            ImmutableList.of(
                Cluster.newBuilder()
                    .setName("cluster1")
                    .setConnectTimeout(Duration.newBuilder().setSeconds(5))
                    .setType(Cluster.DiscoveryType.STATIC)
                    .build()),
            ImmutableList.of(TestResources.createEndpoint("cluster1", "127.0.0.1", 1235)),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            "1"));

    server.awaitTermination();
  }
}
