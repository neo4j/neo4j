/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.ndp.transport.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * Implements version one of the Neo4j protocol when transported over a socket. This means this class will handle a
 * simple message framing protocol and forward messages to the messaging protocol implementation, version 1.
 *
 * Versions of the framing protocol are lock-step with the messaging protocol versioning.
 */
public class SocketProtocolV1 implements SocketProtocol
{
    public static final int VERSION = 1;

    @Override
    public void handle( ChannelHandlerContext ctx, ByteBuf data )
    {

    }

    @Override
    public int version()
    {
        return VERSION;
    }
}
