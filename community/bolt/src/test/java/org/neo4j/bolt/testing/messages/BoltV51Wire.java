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
package org.neo4j.bolt.testing.messages;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.v51.BoltProtocolV51;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public class BoltV51Wire extends BoltV50Wire {

    protected BoltV51Wire(ProtocolVersion version) {
        super(version);
    }

    public BoltV51Wire() {
        super(BoltProtocolV51.VERSION);
    }

    @Override
    public boolean supportsLogonMessage() {
        return true;
    }

    @Override
    public String getUserAgent() {
        return "BoltWire/5.1";
    }

    @Override
    public ByteBuf logon(Map<String, Object> authToken) {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_LOGON))
                .writeMap(authToken)
                .getTarget();
    }

    @Override
    public ByteBuf logoff() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(0, MESSAGE_TAG_LOGOFF))
                .getTarget();
    }
}
