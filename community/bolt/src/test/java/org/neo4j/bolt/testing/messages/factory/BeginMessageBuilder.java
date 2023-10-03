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
package org.neo4j.bolt.testing.messages.factory;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;
import org.neo4j.bolt.protocol.v50.BoltProtocolV50;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public final class BeginMessageBuilder implements NotificationsMessageBuilder<BeginMessageBuilder> {
    private static final short MESSAGE_TAG_BEGIN = (short) 0x11;
    private final ProtocolVersion version;
    private final HashMap<String, Object> meta;

    public BeginMessageBuilder(ProtocolVersion version) {
        this.meta = new HashMap<>();
        this.version = version;
    }

    public BeginMessageBuilder withDatabase(String value) {
        meta.put("db", value);
        return this;
    }

    public BeginMessageBuilder withImpersonatedUser(String value) {
        if (version.compareTo(BoltProtocolV44.VERSION) >= 0) {
            meta.put("imp_user", value);
        }
        return this;
    }

    public BeginMessageBuilder withBookmarks(Collection<String> value) {
        meta.put("bookmarks", new ArrayList<>(value));
        return this;
    }

    public BeginMessageBuilder withTransactionType(String transactionType) {
        if (version.compareTo(BoltProtocolV50.VERSION) >= 0) {
            meta.put("tx_type", transactionType);
        }
        return this;
    }

    @Override
    public Map<String, Object> getMeta() {
        return meta;
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return version;
    }

    @Override
    public ByteBuf build() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMap(meta)
                .getTarget();
    }
}
