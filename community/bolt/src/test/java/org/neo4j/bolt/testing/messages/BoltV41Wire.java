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
import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.values.virtual.MapValue;

public final class BoltV41Wire {

    private static final String USER_AGENT = "BoltV41Wire/0.0";

    private BoltV41Wire() {}

    public static ByteBuf begin() {
        return BoltV40Wire.begin();
    }

    public static ByteBuf begin(String db) {
        return BoltV40Wire.begin(db);
    }

    public static ByteBuf begin(Collection<String> bookmarks) {
        return BoltV40Wire.begin(bookmarks);
    }

    public static ByteBuf begin(String db, Collection<String> bookmarks) {
        return BoltV40Wire.begin(db, bookmarks);
    }

    public static ByteBuf discard() {
        return BoltV40Wire.discard();
    }

    public static ByteBuf discard(long n) {
        return BoltV40Wire.discard(n);
    }

    public static ByteBuf pull() {
        return BoltV40Wire.pull();
    }

    public static ByteBuf pull(long n) {
        return BoltV40Wire.pull(n);
    }

    public static ByteBuf pull(long n, long qid) {
        return BoltV40Wire.pull(n, qid);
    }

    public static ByteBuf hello() {
        return hello(new HashMap<>(), null);
    }

    public static ByteBuf hello(Map<String, Object> meta) {
        return hello(meta, null);
    }

    public static ByteBuf hello(Map<String, Object> meta, RoutingContext context) {
        meta.putIfAbsent("user_agent", USER_AGENT);
        if (context != null) {
            meta.put("routing", context.getParameters());
        }

        return BoltV40Wire.hello(meta);
    }

    public static ByteBuf run() {
        return BoltV40Wire.run();
    }

    public static ByteBuf run(String statement) {
        return BoltV40Wire.run(statement);
    }

    public static ByteBuf run(String statement, MapValue params) {
        return BoltV40Wire.run(statement, params);
    }

    public static ByteBuf run(String statement, MapValue params, MapValue meta) {
        return BoltV40Wire.run(statement, params, meta);
    }

    public static ByteBuf rollback() {
        return BoltV40Wire.rollback();
    }

    public static ByteBuf commit() {
        return BoltV40Wire.commit();
    }

    public static ByteBuf reset() {
        return BoltV40Wire.reset();
    }

    public static ByteBuf goodbye() {
        return BoltV40Wire.goodbye();
    }
}
