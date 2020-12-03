package com.pathao.xds.dao;

import com.google.protobuf.Any;
import com.google.protobuf.Duration;
import com.orbitz.consul.AgentClient;
import com.pathao.xds.dto.ClusterDto;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Address;
import io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.endpoint.v3.Endpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LbEndpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LocalityLbEndpoints;
import io.envoyproxy.envoy.config.listener.v3.Filter;
import io.envoyproxy.envoy.config.listener.v3.FilterChain;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.*;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpFilter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class EndPointDaoV3 {
  private AgentClient agentClient;

  public EndPointDaoV3(AgentClient agentClient) {
    this.agentClient = agentClient;
  }

  public List<ClusterLoadAssignment> getEndpoints() {
    Map<String, List<ClusterDto>> clusterDtoMap = new HashMap<>();
    agentClient.getServices().values().stream().forEach(service -> {
      ClusterDto clusterDto = new ClusterDto(service.getService(), service.getAddress(), service.getPort());
      if(!clusterDtoMap.containsKey(service.getService())){
        clusterDtoMap.put(clusterDto.name, new ArrayList<ClusterDto>());
      }
      clusterDtoMap.get(clusterDto.name).add(clusterDto);
    });
    return clusterDtoMap.keySet().stream().map(key -> {
      List<LocalityLbEndpoints> endpointList = clusterDtoMap.get(key).stream().map(clusterDto -> {
        return LocalityLbEndpoints.newBuilder().addLbEndpoints(LbEndpoint.newBuilder()
            .setEndpoint(Endpoint.newBuilder()
                .setAddress(Address.newBuilder()
                    .setSocketAddress(SocketAddress.newBuilder()
                        .setAddress(clusterDto.host)
                        .setPortValue(clusterDto.port)
                        .setProtocol(SocketAddress.Protocol.TCP))))).build();
      }).collect(toList());
      return ClusterLoadAssignment.newBuilder()
          .setClusterName(key)
          .addAllEndpoints(endpointList)
          .build();
    }).collect(toList());
  }

  public List<Cluster> getClusters(List<ClusterLoadAssignment> clusterLoadAssignments) {
    return clusterLoadAssignments.stream().map(clusterLoadAssignment -> {
      return Cluster.newBuilder()
          .setName(clusterLoadAssignment.getClusterName())
          .setConnectTimeout(Duration.newBuilder().setSeconds(5))
          .setType(Cluster.DiscoveryType.STRICT_DNS)
          .setLoadAssignment(clusterLoadAssignment)
          .build();
    }).collect(toList());
  }

  public Listener getListener(List<Cluster> clusters){
    HttpConnectionManager manager = HttpConnectionManager.newBuilder()
        .setCodecType(HttpConnectionManager.CodecType.AUTO)
        .setStatPrefix("http")
        .setRouteConfig(getRouteConfiguration(clusters))
        .addHttpFilters(HttpFilter.newBuilder()
            .setName("envoy.filters.http.router")).build();
        return Listener.newBuilder()
            .setName("listener_0")
            .setAddress(Address.newBuilder()
                .setSocketAddress(SocketAddress.newBuilder()
                    .setAddress("0.0.0.0")
                    .setPortValue(10000)
                    .setProtocol(SocketAddress.Protocol.TCP)))
            .addFilterChains(FilterChain.newBuilder()
                .addFilters(Filter.newBuilder()
                    .setName("envoy.http_connection_manager")
                    .setTypedConfig(Any.pack(manager))))
            .build();
  }

  public RouteConfiguration getRouteConfiguration(List<Cluster> clusters){
    List<Route> routes = clusters.stream().map(cluster -> {
      return Route.newBuilder()
          .setMatch(RouteMatch.newBuilder()
              .setPrefix("/" + cluster.getName() + "/"))
          .setRoute(RouteAction.newBuilder()
              .setCluster(cluster.getName())).build();
    }).collect(toList());
    return RouteConfiguration.newBuilder()
        .setName("local_route")
        .addVirtualHosts(VirtualHost.newBuilder()
            .setName("all")
            .addDomains("*")
            .addAllRoutes(routes)).build();
  }
}
