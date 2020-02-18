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

import io.netty.channel.ChannelPipeline;

import java.time.Duration;

/**
 * Protect the channel from unauthenticated users by limiting the resources that they can access to.
 */
public class UnauthenticatedChannelProtector implements ChannelProtector
{
    private final Duration channelTimeout;
    private final ChannelPipeline pipeline;
    private final long maxMessageSize;

    public UnauthenticatedChannelProtector( ChannelPipeline pipeline, Duration channelTimeout, long maxMessageSize )
    {
        this.channelTimeout = channelTimeout;
        this.pipeline = pipeline;
        this.maxMessageSize = maxMessageSize;
    }

    public void afterChannelCreated()
    {
        // Adds auth timeout handlers.
        // The timer is counting down after installation.
        pipeline.addLast( new AuthenticationTimeoutTracker( channelTimeout ) );
        pipeline.addLast( new AuthenticationTimeoutHandler( channelTimeout ) );
    }

    public void beforeBoltProtocolInstalled()
    {
        // Adds limits on how many bytes are allowed.
        pipeline.addLast( new BytesAccumulator( maxMessageSize ) );
    }

    public void disable()
    {
        // Removes auth timeout handlers.
        pipeline.remove( AuthenticationTimeoutTracker.class );
        pipeline.remove( AuthenticationTimeoutHandler.class );

        // Remove byte limits
        pipeline.remove( BytesAccumulator.class );
    }
}
