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
package org.neo4j.coreedge.catchup;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class RequestDecoderDispatcher<E extends Enum<E>> extends ChannelInboundHandlerAdapter
{
    private final Map<E, ChannelInboundHandler> decoders = new HashMap<>();
    private final Protocol<E> protocol;
    private final Log log;

    RequestDecoderDispatcher( Protocol<E> protocol, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        ChannelInboundHandler delegate = protocol.select( decoders );
        if ( delegate == null )
        {
            log.warn( "Unregistered handler for protocol %s", protocol );
            return;
        }
        delegate.channelRead( ctx, msg );
    }

    public void register( E type, ChannelInboundHandler decoder )
    {
        assert !decoders.containsKey( type ) : "registering twice a decoder for the same type (" + type + ")?";
        decoders.put( type, decoder );
    }
}
