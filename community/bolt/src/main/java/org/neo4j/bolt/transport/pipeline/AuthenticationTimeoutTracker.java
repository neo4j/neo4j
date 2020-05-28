/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.transport.pipeline;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;

/**
 * Close the channel directly if we failed to finish authentication within certain timeout specified by
 * {@link BoltConnectorInternalSettings#unsupported_bolt_unauth_connection_timeout}
 */
public class AuthenticationTimeoutTracker extends ReadTimeoutHandler
{
    public AuthenticationTimeoutTracker( Duration timeout )
    {
        super( timeout.toMillis(), TimeUnit.MILLISECONDS );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        // Override the parent's method to ensure the count down timer is never reset.
        ctx.fireChannelRead( msg );
    }
}
