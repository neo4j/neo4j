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
import org.neo4j.values.virtual.MapValue;

public final class BoltV42Wire {

    private static final String USER_AGENT = "BoltV42Wire/0.0";

    private BoltV42Wire() {}

    public static ByteBuf begin() {
        return BoltV41Wire.begin();
    }

    public static ByteBuf begin(Collection<String> bookmarks) {
        return BoltV41Wire.begin(bookmarks);
    }

    public static ByteBuf discard() {
        return BoltV41Wire.discard();
    }

    public static ByteBuf discard(long n) {
        return BoltV41Wire.discard(n);
    }

    public static ByteBuf pull() {
        return BoltV41Wire.pull();
    }

    public static ByteBuf pull(long n) {
        return BoltV41Wire.pull(n);
    }

    public static ByteBuf pull(long n, long qid) {
        return BoltV41Wire.pull(n, qid);
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
        if (context != null) {
            meta.put("routing", context.getParameters());
        }

        return BoltV41Wire.hello(meta);
    }

    public static ByteBuf run() {
        return BoltV41Wire.run();
    }

    public static ByteBuf run(String statement) {
        return BoltV41Wire.run(statement);
    }

    public static ByteBuf run(String statement, MapValue params) {
        return BoltV41Wire.run(statement, params);
    }

    public static ByteBuf run(String statement, MapValue params, MapValue meta) {
        return BoltV41Wire.run(statement, params, meta);
    }

    public static ByteBuf rollback() {
        return BoltV41Wire.rollback();
    }

    public static ByteBuf commit() {
        return BoltV41Wire.commit();
    }

    public static ByteBuf reset() {
        return BoltV41Wire.reset();
    }

    public static ByteBuf goodbye() {
        return BoltV41Wire.goodbye();
    }
}
