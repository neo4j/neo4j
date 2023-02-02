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
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.v50.BoltProtocolV50;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public class BoltV50Wire extends AbstractBoltWire {

    public BoltV50Wire(ProtocolVersion version) {
        super(version, Feature.UTC_DATETIME);
    }

    public BoltV50Wire() {
        super(BoltProtocolV50.VERSION, Feature.UTC_DATETIME);
    }

    @Override
    public boolean supportsLogonMessage() {
        return false;
    }

    @Override
    protected String getUserAgent() {
        return "BoltWire/5.0";
    }

    @Override
    public ByteBuf begin(String db, String impersonatedUser, Collection<String> bookmarks, String transactionType) {
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
        if (transactionType != null) {
            meta.put("tx_type", transactionType);
        }

        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMap(meta)
                .getTarget();
    }

    @Override
    public ByteBuf logon(Map<String, Object> authToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf logoff() {
        throw new UnsupportedOperationException();
    }
}
