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
package org.neo4j.packstream.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructRegistry;

public class PackstreamStructEncoder<O> extends MessageToByteEncoder<O> {
    private final StructRegistry<O> registry;

    public PackstreamStructEncoder(Class<? extends O> type, StructRegistry<O> registry) {
        super(type);
        this.registry = registry;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, O msg, ByteBuf out) throws Exception {
        PackstreamBuf.wrap(out).writeStruct(this.registry, msg);
    }
}
