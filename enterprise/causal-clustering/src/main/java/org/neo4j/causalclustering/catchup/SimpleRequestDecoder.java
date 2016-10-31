/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.function.Factory;

/**
 * This class extends {@link MessageToMessageDecoder} because if it extended
 * {@link io.netty.handler.codec.ByteToMessageDecoder} instead the decode method would fail as no
 * bytes are consumed from the ByteBuf but an object is added in the out list.
 */
class SimpleRequestDecoder extends MessageToMessageDecoder<ByteBuf>
{
    private Factory<? extends Message> factory;

    SimpleRequestDecoder( Factory<? extends Message> factory )
    {
        this.factory = factory;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf msg, List<Object> out ) throws Exception
    {
        out.add( factory.newInstance() );
    }
}
