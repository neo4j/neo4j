/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.testing.messages;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v41.message.request.HelloMessage;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.bolt.protocol.v44.message.request.BeginMessage;
import org.neo4j.bolt.protocol.v44.message.request.RouteMessage;
import org.neo4j.bolt.protocol.v44.message.request.RunMessage;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public class BoltV44Messages {
    private static final String USER_AGENT = "BoltV4eWire/0.0";

    private static final RequestMessage HELLO = new HelloMessage(
            map("user_agent", USER_AGENT),
            new RoutingContext(true, stringMap("policy", "fast", "region", "europe")),
            map("user_agent", USER_AGENT));

    public static RequestMessage hello() {
        return HELLO;
    }

    public static RequestMessage hello(Map<String, Object> meta) {
        return hello(meta, null);
    }

    public static RequestMessage hello(Map<String, Object> meta, RoutingContext routingContext) {
        if (!meta.containsKey("user_agent")) {
            meta.put("user_agent", USER_AGENT);
        }

        if (routingContext == null) {
            routingContext = new RoutingContext(false, Collections.emptyMap());
        }

        return new HelloMessage(meta, routingContext, meta);
    }

    public static RequestMessage run() {
        return BoltV43Messages.run();
    }

    public static RequestMessage run(String statement) {
        return BoltV44Messages.run(statement, MapValue.EMPTY);
    }

    public static RequestMessage run(String statement, MapValue params) {
        return BoltV44Messages.run(statement, params, MapValue.EMPTY);
    }

    public static RequestMessage run(String statement, MapValue params, MapValue meta) {
        return new RunMessage(statement, params, meta);
    }

    public static RequestMessage pull() {
        return BoltV43Messages.pull();
    }

    public static RequestMessage discard() throws BoltIOException {
        return BoltV43Messages.discard();
    }

    public static RequestMessage begin() {
        return new BeginMessage();
    }

    public static RequestMessage begin(String databaseName) {
        return begin(null, null, null, null, databaseName, null);
    }

    public static RequestMessage begin(String databaseName, String impersonatedUser) {
        return begin(null, null, null, null, databaseName, impersonatedUser);
    }

    public static RequestMessage begin(
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser) {
        if (bookmarks == null) {
            bookmarks = emptyList();
        }
        if (txMetadata == null) {
            txMetadata = emptyMap();
        }

        if (databaseName == null) {
            databaseName = "";
        }

        return new BeginMessage(
                MapValue.EMPTY, bookmarks, txTimeout, accessMode, txMetadata, databaseName, impersonatedUser);
    }

    public static RequestMessage rollback() {
        return BoltV43Messages.rollback();
    }

    public static RequestMessage commit() {
        return BoltV43Messages.commit();
    }

    public static RequestMessage reset() {
        return BoltV43Messages.reset();
    }

    public static RequestMessage goodbye() {
        return BoltV43Messages.goodbye();
    }

    public static RouteMessage route(String impersonatedUser) {
        return new RouteMessage(new MapValueBuilder().build(), List.of(), null, impersonatedUser);
    }
}
