package io.envoyproxy.controlplane.server;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.pathao.xds.dao.ClusterDao;
import com.pathao.xds.dao.EndPointDao;
import io.envoyproxy.controlplane.cache.NodeGroup;
import io.envoyproxy.controlplane.cache.v2.SimpleCache;
import io.envoyproxy.envoy.api.v2.Cluster;
import io.envoyproxy.envoy.api.v2.ClusterLoadAssignment;
import io.envoyproxy.envoy.api.v2.core.Node;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConsulXdsServer {

  public static String GROUP = "Global";
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private static SimpleCache<String> cache = new SimpleCache<>(new NodeGroup<String>() {
    @Override public String hash(Node node) {
      return GROUP;
    }

    @Override public String hash(io.envoyproxy.envoy.config.core.v3.Node node) {
      return GROUP;
    }
  });

  private static String consulHost = "ifp3-dev1";
  private static int consulPort = 8500;

  public static void main(String[] args) throws IOException, InterruptedException {
    initConfig();
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

    System.out.println("---Server has started on port " + server.getPort());
    Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));

    scheduler.scheduleAtFixedRate(() -> {
      try {
        System.out.println("Checking config");
        fetchAndUpdateConfig().run();
      } catch (Exception e) {
        System.out.println("Error on Desrialization");
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    }, 10, 10, TimeUnit.SECONDS);

    server.awaitTermination();
  }

  private static void initConfig() {
    String fileName = Optional.ofNullable(System.getProperty("prop")).orElse("xDSServer.properties");
    System.out.println("Property file: " + fileName);
    InputStream input = null;

    try {
      input = new FileInputStream(ClassLoader.getSystemResource(fileName).getFile());
      Properties prop = new Properties();

      prop.load(input);
      consulHost = prop.getProperty("host");
      consulPort = Integer.valueOf(prop.getProperty("port"));
      System.out.println("Consul address " + consulHost + ":" + consulPort);
    } catch (FileNotFoundException e) {
      System.out.println("Error reading properties file");
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static Runnable fetchAndUpdateConfig() {
    return () -> {
      Consul consul = getConsulClient();
      AgentClient agentClient = consul.agentClient();
      String version = "1";
//      ClusterDao clusterDao = new ClusterDao(agentClient);
//      List<Cluster> clusters = clusterDao.getClusters();
      EndPointDao endPointDao = new EndPointDao(agentClient);
      List<ClusterLoadAssignment> clusterLoadAssignments = endPointDao.getEndpoints();
      List<Cluster> clusters = endPointDao.getClusters(clusterLoadAssignments);
      System.out.println("Updating with clusterLoadAssignments=" + clusterLoadAssignments);

      cache.setSnapshot(
          GROUP,
          io.envoyproxy.controlplane.cache.v2.Snapshot.create(
              clusters,
              clusterLoadAssignments,
              ImmutableList.of(endPointDao.getListener()),
              ImmutableList.of(endPointDao.getRouteConfiguration(clusters)),
              ImmutableList.of(),
              version));
      consul.destroy();
    };
  }

  private static Consul getConsulClient() {
    return Consul.builder()
        .withHostAndPort(HostAndPort.fromParts(consulHost, consulPort))
        .build();
  }

}
