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
import java.util.Collections;
import java.util.Map;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public class TelemetryMessageBuilder implements WireMessageBuilder<TelemetryMessageBuilder> {
    private static final short SIGNATURE = 0x54;

    private final ProtocolVersion version;
    private byte apiType;

    public TelemetryMessageBuilder(ProtocolVersion version) {
        this.version = version;
    }

    @Override
    public Map<String, Object> getMeta() {
        return Collections.emptyMap();
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return this.version;
    }

    public TelemetryMessageBuilder withManagedTransactionFunctions() {
        apiType = (byte) 0;
        return this;
    }

    public TelemetryMessageBuilder withUnmanagedTransactions() {
        apiType = (byte) 1;
        return this;
    }

    public TelemetryMessageBuilder withImplicitTransactions() {
        apiType = (byte) 2;
        return this;
    }

    public TelemetryMessageBuilder withExecute() {
        apiType = (byte) 3;
        return this;
    }

    public TelemetryMessageBuilder withANonValidAPIType() {
        apiType = (byte) 4;
        return this;
    }

    @Override
    public ByteBuf build() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, SIGNATURE))
                .writeTinyInt(apiType)
                .getTarget();
    }
}
