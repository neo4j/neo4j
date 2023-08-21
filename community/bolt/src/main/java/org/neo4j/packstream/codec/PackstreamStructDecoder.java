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
package org.neo4j.packstream.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.packstream.error.reader.UnexpectedStructException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructRegistry;

public class PackstreamStructDecoder<CTX> extends MessageToMessageDecoder<PackstreamBuf> {
    private final CTX ctx;
    private final StructRegistry<CTX, ?> registry;
    private final InternalLog log;

    public PackstreamStructDecoder(CTX ctx, StructRegistry<CTX, ?> registry, InternalLogProvider logging) {
        this.ctx = ctx;
        this.registry = registry;
        this.log = logging.getLog(PackstreamStructDecoder.class);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, PackstreamBuf msg, List<Object> out) throws Exception {
        try {
            var struct = msg.readStruct(this.ctx, this.registry);
            out.add(struct);
        } catch (UnexpectedStructException ex) {
            // TODO: Return FAILURE instead to make Bolt connections more fault tolerant and driver dev friendly?
            this.log.debug("Terminating connection due to invalid message", ex);
            ctx.close();
        }
    }
}
