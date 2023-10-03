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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.virtual.MapValue;

public final class RunMessageBuilder implements NotificationsMessageBuilder<RunMessageBuilder> {
    public static final short MESSAGE_TAG_RUN = (short) 0x10;
    private final String query;
    private final ProtocolVersion protocolVersion;
    private final WriterPipeline pipeline;
    private final HashMap<String, Object> meta;
    private MapValue params;

    public RunMessageBuilder(ProtocolVersion protocolVersion, String query, WriterPipeline pipeline) {
        this.protocolVersion = protocolVersion;
        this.pipeline = pipeline;
        this.meta = new HashMap<>();
        this.query = query;
        this.params = null;
    }

    public RunMessageBuilder withParameters(MapValue params) {
        this.params = params;
        return this;
    }

    public RunMessageBuilder withDatabase(String value) {
        meta.put("db", value);
        return this;
    }

    public RunMessageBuilder withImpersonatedUser(String value) {
        meta.put("imp_user", value);
        return this;
    }

    @Override
    public Map<String, Object> getMeta() {
        return meta;
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    @Override
    public ByteBuf build() {
        var paramsValue = Objects.requireNonNullElse(params, MapValue.EMPTY);

        var buf = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, MESSAGE_TAG_RUN))
                .writeString(query);

        var ctx = pipeline.forBuffer(buf);
        ctx.writeValue(paramsValue);
        ctx.writeValue(ValueUtils.asMapValue(meta));

        return buf.getTarget();
    }
}
