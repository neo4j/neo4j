/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.routing.result;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.dbms.routing.result.Role.READ;
import static org.neo4j.dbms.routing.result.Role.ROUTE;
import static org.neo4j.dbms.routing.result.Role.WRITE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.utf8Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.routing.RoutingResult;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * The result format of GetServersV1 and GetServersV2 procedures.
 */
public final class RoutingResultFormat {
    private static final String ROLE_KEY = "role";
    private static final String ADDRESSES_KEY = "addresses";
    private static final TextValue READ_NAME = utf8Value(READ.name());
    private static final TextValue WRTE_NAME = utf8Value(WRITE.name());
    private static final TextValue ROUTE_NAME = utf8Value(ROUTE.name());

    private RoutingResultFormat() {}

    public static AnyValue[] build(RoutingResult result) {
        ListValue routers = asValues(result.routeEndpoints());
        ListValue readers = asValues(result.readEndpoints());
        ListValue writers = asValues(result.writeEndpoints());

        ListValueBuilder servers = ListValueBuilder.newListBuilder();

        if (writers.actualSize() > 0) {
            MapValueBuilder builder = new MapValueBuilder();

            builder.add(ROLE_KEY, WRTE_NAME);
            builder.add(ADDRESSES_KEY, writers);

            servers.add(builder.build());
        }

        if (readers.actualSize() > 0) {
            MapValueBuilder builder = new MapValueBuilder();

            builder.add(ROLE_KEY, READ_NAME);
            builder.add(ADDRESSES_KEY, readers);

            servers.add(builder.build());
        }

        if (routers.actualSize() > 0) {
            MapValueBuilder builder = new MapValueBuilder();

            builder.add(ROLE_KEY, ROUTE_NAME);
            builder.add(ADDRESSES_KEY, routers);

            servers.add(builder.build());
        }

        LongValue timeToLiveSeconds = longValue(MILLISECONDS.toSeconds(result.ttlMillis()));
        return new AnyValue[] {timeToLiveSeconds, servers.build()};
    }

    public static RoutingResult parse(AnyValue[] record) {
        LongValue timeToLiveSeconds = (LongValue) record[0];
        ListValue endpointData = (ListValue) record[1];

        Map<Role, List<SocketAddress>> endpoints = parseRows(endpointData);

        return new RoutingResult(
                endpoints.get(ROUTE), endpoints.get(WRITE), endpoints.get(READ), timeToLiveSeconds.longValue() * 1000);
    }

    public static RoutingResult parse(MapValue record) {
        return parse(new AnyValue[] {
            record.get(ParameterNames.TTL.parameterName()), record.get(ParameterNames.SERVERS.parameterName())
        });
    }

    public static List<SocketAddress> parseEndpoints(ListValue addresses) {
        List<SocketAddress> result = new ArrayList<>(addresses.intSize());
        for (AnyValue address : addresses) {
            result.add(parseAddress(((TextValue) address).stringValue()));
        }
        return result;
    }

    private static Map<Role, List<SocketAddress>> parseRows(ListValue rows) {
        Map<Role, List<SocketAddress>> endpoints = new HashMap<>();
        for (AnyValue single : rows) {
            MapValue row = (MapValue) single;
            Role role = Role.valueOf(((TextValue) row.get("role")).stringValue());
            List<SocketAddress> addresses = parseEndpoints((ListValue) row.get("addresses"));
            endpoints.put(role, addresses);
        }

        Arrays.stream(Role.values()).forEach(r -> endpoints.putIfAbsent(r, Collections.emptyList()));

        return endpoints;
    }

    private static SocketAddress parseAddress(String address) {
        String[] split = address.split(":");
        return new SocketAddress(split[0], Integer.parseInt(split[1]));
    }

    private static ListValue asValues(List<SocketAddress> addresses) {
        return addresses.stream()
                .map(SocketAddress::toString)
                .map(Values::utf8Value)
                .collect(ListValueBuilder.collector());
    }

    // This feels a little fragile perhaps bolt needs to be decouple from the procedure output but that would need a
    // driver change so perhaps this is fine?
    public static MapValue buildMap(RoutingResult result) {
        AnyValue[] builtResult = build(result);

        MapValueBuilder builder = new MapValueBuilder();
        builder.add(ParameterNames.TTL.parameterName(), intValue((int) ((LongValue) builtResult[0]).value()));
        builder.add(ParameterNames.SERVERS.parameterName(), builtResult[1]);

        return builder.build();
    }
}
