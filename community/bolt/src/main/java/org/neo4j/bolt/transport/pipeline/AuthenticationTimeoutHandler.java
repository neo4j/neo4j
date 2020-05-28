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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;

import java.time.Duration;

import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;

import static java.lang.String.format;

/**
 * Close the channel directly if we failed to finish authentication within certain timeout specified by
 * {@link BoltConnectorInternalSettings#unsupported_bolt_unauth_connection_timeout}
 */
public class AuthenticationTimeoutHandler extends ChannelInboundHandlerAdapter
{
    private final Duration timeout;

    public AuthenticationTimeoutHandler( Duration timeout )
    {
        this.timeout = timeout;
    }

    @Override
    public void userEventTriggered( ChannelHandlerContext ctx, Object evt ) throws Exception
    {
        if ( evt instanceof IdleStateEvent )
        {
            ctx.close(); // We failed to finish auth within timeout.
            throw new BoltConnectionFatality( format(
                    "A connection '%s' is terminated because the client failed to finish authenticate within %s ms.",
                    ctx.channel(), timeout.toMillis() ),
                    null );

        }
    }
}
