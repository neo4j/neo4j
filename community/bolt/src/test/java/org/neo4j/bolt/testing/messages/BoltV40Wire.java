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
import java.util.HashMap;
import java.util.Map;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValues;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.virtual.MapValue;

public final class BoltV40Wire {

    public static final short MESSAGE_TAG_BEGIN = (short) 0x11;
    public static final short MESSAGE_TAG_DISCARD = (short) 0x2F;
    public static final short MESSAGE_TAG_PULL = (short) 0x3F;
    public static final short MESSAGE_TAG_HELLO = (short) 0x01;
    public static final short MESSAGE_TAG_RUN = (short) 0x10;
    public static final short MESSAGE_TAG_ROLLBACK = (short) 0x13;
    public static final short MESSAGE_TAG_COMMIT = (short) 0x12;
    public static final short MESSAGE_TAG_RESET = (short) 0x0F;
    public static final short MESSAGE_TAG_GOODBYE = (short) 0x02;

    private static final String USER_AGENT = "BoltV40Wire/0.0";

    private BoltV40Wire() {}

    public static ByteBuf begin() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMapHeader(0)
                .getTarget();
    }

    public static ByteBuf begin(String db) {
        return begin(db, null);
    }

    public static ByteBuf begin(Collection<String> bookmarks) {
        return begin(null, bookmarks);
    }

    public static ByteBuf begin(String db, Collection<String> bookmarks) {
        var meta = new HashMap<String, Object>();
        if (db != null) {
            meta.put("db", db);
        }
        if (bookmarks != null) {
            meta.put("bookmarks", new ArrayList<>(bookmarks));
        }

        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMap(meta)
                .getTarget();
    }

    public static ByteBuf discard(long n) {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_DISCARD))
                .writeMapHeader(1)
                .writeString("n")
                .writeInt(n)
                .getTarget();
    }

    public static ByteBuf discard() {
        return discard(-1);
    }

    public static ByteBuf pull(long n) {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_PULL))
                .writeMapHeader(1)
                .writeString("n")
                .writeInt(n)
                .getTarget();
    }

    public static ByteBuf pull(long n, long qid) {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_PULL))
                .writeMapHeader(2)
                .writeString("n")
                .writeInt(n)
                .writeString("qid")
                .writeInt(qid)
                .getTarget();
    }

    public static ByteBuf pull() {
        return pull(-1);
    }

    public static ByteBuf hello(Map<String, Object> meta) {
        meta.putIfAbsent("user_agent", USER_AGENT);

        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_HELLO))
                .writeMap(meta)
                .getTarget();
    }

    public static ByteBuf hello() {
        return hello(new HashMap<>());
    }

    public static ByteBuf run() {
        return run("RETURN 1");
    }

    public static ByteBuf run(String statement) {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, MESSAGE_TAG_RUN))
                .writeString(statement)
                .writeMapHeader(0)
                .writeMapHeader(0)
                .getTarget();
    }

    public static ByteBuf run(String statement, MapValue params) {
        var buf = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, MESSAGE_TAG_RUN))
                .writeString(statement);
        PackstreamValues.writeValue(buf, params);
        buf.writeMapHeader(0);

        return buf.getTarget();
    }

    public static ByteBuf run(String statement, MapValue params, MapValue meta) {
        var buf = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, MESSAGE_TAG_RUN))
                .writeString(statement);

        PackstreamValues.writeValue(buf, params);
        PackstreamValues.writeValue(buf, meta);

        return buf.getTarget();
    }

    public static ByteBuf rollback() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(0, MESSAGE_TAG_ROLLBACK))
                .getTarget();
    }

    public static ByteBuf commit() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(0, MESSAGE_TAG_COMMIT))
                .getTarget();
    }

    public static ByteBuf reset() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(0, MESSAGE_TAG_RESET))
                .getTarget();
    }

    public static ByteBuf goodbye() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(0, MESSAGE_TAG_GOODBYE))
                .getTarget();
    }
}
