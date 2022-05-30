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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.virtual.MapValue;

public final class BoltV44Wire {
    private static final String USER_AGENT = "BoltV44Wire/0.0";

    public static final short MESSAGE_TAG_BEGIN = (short) 0x11;
    public static final short MESSAGE_TAG_ROUTE = (short) 0x66;

    private BoltV44Wire() {}

    public static ByteBuf begin() {
        return begin(null, null, null);
    }

    public static ByteBuf begin(String db) {
        return begin(db, null, null);
    }

    public static ByteBuf begin(Collection<String> bookmarks) {
        return BoltV40Wire.begin(bookmarks);
    }

    public static ByteBuf begin(String db, String impersonatedUser) {
        return begin(db, impersonatedUser, null);
    }

    public static ByteBuf begin(String db, String impersonatedUser, Collection<String> bookmarks) {
        var meta = new HashMap<String, Object>();
        if (db != null) {
            meta.put("db", db);
        }
        if (impersonatedUser != null) {
            meta.put("imp_user", impersonatedUser);
        }
        if (bookmarks != null) {
            meta.put("bookmarks", new ArrayList<>(bookmarks));
        }

        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMap(meta)
                .getTarget();
    }

    public static ByteBuf discard() {
        return discard(-1);
    }

    public static ByteBuf discard(long n) {
        return BoltV40Wire.discard(n);
    }

    public static ByteBuf pull() {
        return pull(-1);
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

    public static ByteBuf route() {
        return route(null, null, null, null);
    }

    public static ByteBuf route(String impersonatedUser) {
        return route(null, null, null, impersonatedUser);
    }

    public static ByteBuf route(RoutingContext context, Collection<String> bookmarks, String db) {
        return route(context, bookmarks, db, null);
    }

    public static ByteBuf route(
            RoutingContext context, Collection<String> bookmarks, String db, String impersonatedUser) {
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

        var meta = new HashMap<String, Object>();
        if (db != null) {
            meta.put("db", db);
        }
        if (impersonatedUser != null) {
            meta.put("imp_user", impersonatedUser);
        }

        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, MESSAGE_TAG_ROUTE))
                .writeMap(routingParams, PackstreamBuf::writeString)
                .writeList(bookmarkStrings, PackstreamBuf::writeString)
                .writeMap(meta)
                .getTarget();
    }
}
