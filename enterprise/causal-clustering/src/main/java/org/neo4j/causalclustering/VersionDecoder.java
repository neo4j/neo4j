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
package org.neo4j.causalclustering;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class VersionDecoder extends ByteToMessageDecoder
{
    private final Log log;
    private final byte currentVersion;

    protected VersionDecoder( LogProvider logProvider, byte currentVersion )
    {
        this.currentVersion = currentVersion;
        this.log = logProvider.getLog( getClass() );
    }

    public VersionDecoder( LogProvider logProvider )
    {
        this( logProvider, Message.CURRENT_VERSION );
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        byte version = in.readByte();

        if ( version != currentVersion )
        {
            log.error( "Unsupported version %d, current version is %d", version, currentVersion );
        }
        else
        {
            ByteBuf retained = in.slice( in.readerIndex(), in.readableBytes() ).retain();
            in.readerIndex( in.writerIndex() );
            out.add( retained );
        }
    }
}
