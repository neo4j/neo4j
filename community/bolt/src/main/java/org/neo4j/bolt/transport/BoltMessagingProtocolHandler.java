/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;

/**
 * Implementations define a versioned implementation of the Bolt Protocol. Incoming messages from clients are
 * forwarded to the {@link #handle(io.netty.channel.ChannelHandlerContext, io.netty.buffer.ByteBuf)} method.
 */
public interface BoltMessagingProtocolHandler
{
    /** Handle an incoming message, and reply if desired via the {@code ctx} argument */
    void handle( ChannelHandlerContext ctx, ByteBuf data ) throws IOException;

    /** Used for version negotiation */
    int version();

    /** Close this instance of the protocol, disposing of any held resources */
    void close();
}
