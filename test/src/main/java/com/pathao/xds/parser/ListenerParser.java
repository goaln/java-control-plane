package com.pathao.xds.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.BoolValue;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.envoyproxy.envoy.api.v2.Listener;
import io.envoyproxy.envoy.api.v2.RouteConfiguration;
import io.envoyproxy.envoy.api.v2.core.Address;
import io.envoyproxy.envoy.api.v2.core.SocketAddress;
import io.envoyproxy.envoy.api.v2.listener.Filter;
import io.envoyproxy.envoy.api.v2.listener.FilterChain;
import io.envoyproxy.envoy.api.v2.route.Route;
import io.envoyproxy.envoy.api.v2.route.RouteAction;
import io.envoyproxy.envoy.api.v2.route.RouteMatch;
import io.envoyproxy.envoy.api.v2.route.VirtualHost;
import io.envoyproxy.envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManager;
import io.envoyproxy.envoy.config.filter.network.http_connection_manager.v2.HttpFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ListenerParser {
    public static List<Listener> buildListeners(JsonNode jsonNode) {
        List<Listener> listeners = new ArrayList<>();
        jsonNode.forEach(jListener -> {
            listeners.add(buildListener(jListener));
        });
        return listeners;
    }

    public static Listener buildListener(JsonNode jListener) {
        return Listener.newBuilder()
                .setName(jListener.get("name").asText())
                .setAddress(buildAddress(jListener.get("address")))
                .addAllFilterChains(buildFilerChains(jListener.get("filter_chains")))
                .build();
    }

    private static Address buildAddress(JsonNode jListener) {
        return Address.newBuilder()
                .setSocketAddress(SocketAddress.newBuilder()
                        .setAddress(jListener.get("socket_address").get("address").asText())
                        .setPortValue(jListener.get("socket_address").get("port_value").asInt())
                        .build())
                .build();
    }

    private static List<FilterChain> buildFilerChains(JsonNode jFilterChains) {
        List<FilterChain> filterChains = new ArrayList<>();
        jFilterChains.forEach(jFilterChain -> {
            filterChains.add(
            FilterChain.newBuilder()
                    .addAllFilters(buildFilters(jFilterChain.get("filters")))
                    .build()
            );
        });
        return filterChains;
    }

    private static List<Filter> buildFilters(JsonNode jFilters) {
        List<Filter> filters = new ArrayList<>();
        jFilters.forEach(jFilter -> {
            filters.add(Filter.newBuilder()
                    .setName(jFilter.get("name").asText())
                    .setConfig(buildFilterConfig(jFilter.get("config")))
                    .build()
            );
        });
        return filters;
    }

    private static Struct buildFilterConfig(JsonNode jFilterConfig) {
        HttpConnectionManager filterConfig = HttpConnectionManager.newBuilder()
                .setStatPrefix(jFilterConfig.get("stat_prefix").asText())
                .setCodecType(HttpConnectionManager
                        .CodecType.valueOf(jFilterConfig.get("codec_type").asText()))
                .setGenerateRequestId(BoolValue.of(jFilterConfig.get("generate_request_id").asBoolean()))
                .setTracing(buildTracing(jFilterConfig.get("tracing")))
                .setRouteConfig(buildRouteConfig(jFilterConfig.get("route_config")))
                .addAllHttpFilters(buildHttpFilters(jFilterConfig.get("http_filters")))
                .build();
        return messageAsStruct(filterConfig);
    }

    private static List<HttpFilter> buildHttpFilters(JsonNode jHttpFilters) {
        List<HttpFilter> filters = new ArrayList<>();
        jHttpFilters.forEach(jHttpFilter -> {
            filters.add(HttpFilter.newBuilder()
                    .setName(jHttpFilter.get("name").asText())
                    .build()
            );
        });
        return filters;
    }

    private static HttpConnectionManager.Tracing buildTracing(JsonNode jTracing) {
        if (jTracing == null) {
            return HttpConnectionManager.Tracing.getDefaultInstance();
        }
        return HttpConnectionManager.Tracing
                .newBuilder().setOperationName(HttpConnectionManager.Tracing
                        .OperationName.valueOf(jTracing.get("operation_name").asText().toUpperCase()))
                .build();
    }

    private static RouteConfiguration buildRouteConfig(JsonNode jRouteConfig) {
        return RouteConfiguration.newBuilder()
                .setName(jRouteConfig.get("name").asText())
                .addAllVirtualHosts(buildVirtualHosts(jRouteConfig.get("virtual_hosts")))
                .build();
    }

    public static List<VirtualHost> buildVirtualHosts(JsonNode jVirtualHosts) {
        List<VirtualHost> virtualHosts = new ArrayList<>();
        jVirtualHosts.forEach(jvh -> {
            virtualHosts.add(VirtualHost.newBuilder()
                    .setName(jvh.get("name").asText())
                    .addAllDomains(buildDomains(jvh.get("domains")))
                    .addAllRoutes(buildRoutes(jvh.get("routes")))
                    .build()
            );
        });
        return virtualHosts;
    }

    public static List<String> buildDomains(JsonNode jDomains) {
        List<String> res = new ArrayList<>();
        jDomains.forEach(x -> res.add(x.asText()));
        return res;
    }

    private static List<Route> buildRoutes(JsonNode jRoutes) {
        List<Route> routes = new ArrayList<>();
        jRoutes.forEach(jRoute -> {
            routes.add(Route.newBuilder()
                    .setMatch(buildMatch(jRoute.get("match")))
                    .setRoute(buildRoute(jRoute.get("route")))
                    .build()
            );
        });
        return routes;
    }

    private static RouteAction buildRoute(JsonNode jRoute) {
        Optional<String> prefix_rewrite = Optional.ofNullable(jRoute.get("prefix_rewrite")).map(JsonNode::asText);
        RouteAction.Builder route = RouteAction.newBuilder()
                .setCluster(jRoute.get("cluster").asText());

        prefix_rewrite.ifPresent(route::setPrefixRewrite);
        return route.build();
    }

    private static RouteMatch buildMatch(JsonNode jMatch) {
        return RouteMatch.newBuilder().setPrefix(jMatch.get("prefix").asText()).build();
    }

    public static Struct messageAsStruct(MessageOrBuilder message) {
        try {
            String json = JsonFormat.printer()
                    .preservingProtoFieldNames()
                    .print(message);

            Struct.Builder structBuilder = Struct.newBuilder();

            JsonFormat.parser().merge(json, structBuilder);

            return structBuilder.build();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to convert protobuf message to struct", e);
        }
    }
}
