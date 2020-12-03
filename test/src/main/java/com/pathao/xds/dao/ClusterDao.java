package com.pathao.xds.dao;

import com.google.protobuf.Duration;
import com.orbitz.consul.AgentClient;
import com.pathao.xds.dto.ClusterDto;
import io.envoyproxy.envoy.api.v2.Cluster;
import io.envoyproxy.envoy.api.v2.core.Address;
import io.envoyproxy.envoy.api.v2.core.SocketAddress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class ClusterDao {
    private AgentClient agentClient;

    public ClusterDao(AgentClient agentClient) {
        this.agentClient = agentClient;
    }

    public List<Cluster> getClusters() {
      Map<String, List<ClusterDto>> clusterDtoMap = new HashMap<>();
      agentClient.getServices().values().stream().forEach(service -> {
        ClusterDto clusterDto = new ClusterDto(service.getService(), service.getAddress(), service.getPort());
        if(!clusterDtoMap.containsKey(service.getService())){
          clusterDtoMap.put(clusterDto.name, new ArrayList<ClusterDto>());
        }
        clusterDtoMap.get(clusterDto.name).add(clusterDto);
      });
      return clusterDtoMap.keySet().stream().map(key -> {
        List<Address> addresses = clusterDtoMap.get(key).stream().map(clusterDto -> {
          return Address.newBuilder().setSocketAddress(
              SocketAddress.newBuilder()
                  .setAddress(clusterDto.host)
                  .setPortValue(clusterDto.port)).build();
        }).collect(toList());
        return Cluster.newBuilder()
            .setName(key)
            .setConnectTimeout(Duration.newBuilder().setSeconds(5))
            .setType(Cluster.DiscoveryType.STRICT_DNS).addHosts(addresses.get(0)).build();
      }).collect(toList());
    }
}
