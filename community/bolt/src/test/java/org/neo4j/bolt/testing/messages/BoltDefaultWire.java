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

import io.netty.buffer.ByteBuf;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.values.virtual.MapValue;

/**
 * Provides aliases for the currently selected default wire protocol.
 * <p>
 * The references within this type should be updated along with {@link TransportConnection#DEFAULT_PROTOCOL_VERSION} in order to transmit the correct message
 * variations.
 */
public final class BoltDefaultWire {
    private static final String USER_AGENT = "BoltDefaultWire/0.0";

    private BoltDefaultWire() {}

    public static ByteBuf begin() {
        return BoltV44Wire.begin();
    }

    public static ByteBuf begin(String db) {
        return BoltV44Wire.begin(db);
    }

    public static ByteBuf begin(Collection<String> bookmarks) {
        return BoltV44Wire.begin(bookmarks);
    }

    public static ByteBuf begin(String db, Collection<String> bookmarks) {
        return begin(db, null, bookmarks);
    }

    public static ByteBuf begin(String db, String impersonatedUser) {
        return BoltV44Wire.begin(db, impersonatedUser);
    }

    public static ByteBuf begin(String db, String impersonatedUser, Collection<String> bookmarks) {
        return BoltV44Wire.begin(db, impersonatedUser, bookmarks);
    }

    public static ByteBuf discard() {
        return BoltV44Wire.discard();
    }

    public static ByteBuf discard(long n) {
        return BoltV44Wire.discard(n);
    }

    public static ByteBuf pull() {
        return BoltV44Wire.pull();
    }

    public static ByteBuf pull(long n) {
        return BoltV44Wire.pull(n);
    }

    public static ByteBuf pull(long n, long qid) {
        return BoltV44Wire.pull(n, qid);
    }

    public static ByteBuf hello() {
        return hello(Collections.emptyMap(), null);
    }

    public static ByteBuf hello(Map<String, Object> meta) {
        return hello(meta, null);
    }

    public static ByteBuf hello(Map<String, Object> meta, RoutingContext context) {
        meta = new HashMap<>(meta);
        meta.putIfAbsent("user_agent", USER_AGENT);
        return BoltV44Wire.hello(meta, context);
    }

    public static ByteBuf hello(String username, String password) {
        return hello(Map.of(
                "scheme", "basic",
                "principal", username,
                "credentials", password));
    }

    public static ByteBuf hello(String username, String password, String realm) {
        return hello(Map.of(
                "scheme", "basic",
                "realm", realm,
                "principal", username,
                "credentials", password));
    }

    public static ByteBuf run() {
        return BoltV44Wire.run();
    }

    public static ByteBuf run(String statement) {
        return BoltV44Wire.run(statement);
    }

    public static ByteBuf run(String statement, MapValue params) {
        return BoltV44Wire.run(statement, params);
    }

    public static ByteBuf run(String statement, MapValue params, MapValue meta) {
        return BoltV44Wire.run(statement, params, meta);
    }

    public static ByteBuf rollback() {
        return BoltV44Wire.rollback();
    }

    public static ByteBuf commit() {
        return BoltV44Wire.commit();
    }

    public static ByteBuf reset() {
        return BoltV44Wire.reset();
    }

    public static ByteBuf goodbye() {
        return BoltV44Wire.goodbye();
    }

    public static ByteBuf route() {
        return BoltV44Wire.route();
    }

    public static ByteBuf route(String impersonatedUser) {
        return BoltV44Wire.route(impersonatedUser);
    }

    public static ByteBuf route(RoutingContext context, Collection<String> bookmarks, String db) {
        return BoltV44Wire.route(context, bookmarks, db);
    }

    public static ByteBuf route(
            RoutingContext context, Collection<String> bookmarks, String db, String impersonatedUser) {
        return BoltV44Wire.route(context, bookmarks, db, impersonatedUser);
    }
}
