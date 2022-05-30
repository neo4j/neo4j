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
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.virtual.MapValue;

public final class BoltV43Wire {
    private static final String USER_AGENT = "BoltV43Wire/0.0";

    public static final short MESSAGE_TAG_ROUTE = (short) 0x66;

    private BoltV43Wire() {}

    public static ByteBuf begin() {
        return BoltV42Wire.begin();
    }

    public static ByteBuf begin(String db) {
        return BoltV41Wire.begin(db);
    }

    public static ByteBuf begin(Collection<String> bookmarks) {
        return BoltV42Wire.begin(bookmarks);
    }

    public static ByteBuf begin(String db, Collection<String> bookmarks) {
        return BoltV41Wire.begin(db);
    }

    public static ByteBuf discard() {
        return discard(-1);
    }

    public static ByteBuf discard(long n) {
        return BoltV42Wire.discard(n);
    }

    public static ByteBuf pull() {
        return BoltV42Wire.pull();
    }

    public static ByteBuf pull(long n) {
        return BoltV42Wire.pull(n);
    }

    public static ByteBuf pull(long n, long qid) {
        return BoltV42Wire.pull(n, qid);
    }

    public static ByteBuf hello() {
        return hello(new HashMap<>(), null);
    }

    public static ByteBuf hello(Map<String, Object> meta, RoutingContext context) {
        meta.putIfAbsent("user_agent", USER_AGENT);
        if (context != null) {
            meta.put("routing", context.getParameters());
        }

        return BoltV42Wire.hello(meta);
    }

    public static ByteBuf run() {
        return BoltV42Wire.run();
    }

    public static ByteBuf run(String statement) {
        return BoltV42Wire.run(statement);
    }

    public static ByteBuf run(String statement, MapValue params) {
        return BoltV42Wire.run(statement, params);
    }

    public static ByteBuf run(String statement, MapValue params, MapValue meta) {
        return BoltV42Wire.run(statement, params, meta);
    }

    public static ByteBuf rollback() {
        return BoltV42Wire.rollback();
    }

    public static ByteBuf commit() {
        return BoltV42Wire.commit();
    }

    public static ByteBuf reset() {
        return BoltV42Wire.reset();
    }

    public static ByteBuf goodbye() {
        return BoltV42Wire.goodbye();
    }

    public static ByteBuf route() {
        return route(null, null, null);
    }

    public static ByteBuf route(RoutingContext context, Collection<String> bookmarks, String db) {
        var packstreamBuf = PackstreamBuf.allocUnpooled();
        Map<String, String> routingParams;
        if (context != null) {
            routingParams = context.getParameters();
        } else {
            routingParams = Collections.emptyMap();
        }

        Collection<String> bookmarkStrings = bookmarks;
        if (bookmarkStrings == null) {
            bookmarkStrings = Collections.emptyList();
        }

        packstreamBuf
                .writeStructHeader(new StructHeader(3, MESSAGE_TAG_ROUTE))
                .writeMap(routingParams, PackstreamBuf::writeString)
                .writeList(bookmarkStrings, PackstreamBuf::writeString);

        String dbName = db;
        if (dbName == null) {
            packstreamBuf.writeNull();
        } else {
            packstreamBuf.writeString(dbName);
        }

        return packstreamBuf.getTarget();
    }
}
