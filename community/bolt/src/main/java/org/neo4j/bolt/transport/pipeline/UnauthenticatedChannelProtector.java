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
package org.neo4j.bolt.transport.pipeline;

import io.netty.channel.Channel;

import java.time.Duration;

import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;

/**
 * Protect the channel from unauthenticated users by limiting the resources that they can access to.
 */
public class UnauthenticatedChannelProtector implements ChannelProtector
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( UnauthenticatedChannelProtector.class );

    private final Channel channel;
    private final long maxMessageSize;
    private final MemoryTracker memoryTracker;

    private AuthenticationTimeoutHandler timeoutHandler;

    public UnauthenticatedChannelProtector( Channel ch, Duration channelTimeout, long maxMessageSize, MemoryTracker memoryTracker )
    {
        this.channel = ch;
        this.maxMessageSize = maxMessageSize;
        this.memoryTracker = memoryTracker;

        memoryTracker.allocateHeap( AuthenticationTimeoutHandler.SHALLOW_SIZE );
        this.timeoutHandler = new AuthenticationTimeoutHandler( channelTimeout );
    }

    public void afterChannelCreated()
    {
        // Adds auth timeout handlers.
        // The timer is counting down after installation.
        this.channel.pipeline().addLast( timeoutHandler );
    }

    public void beforeBoltProtocolInstalled()
    {
        memoryTracker.allocateHeap( BytesAccumulator.SHALLOW_SIZE );

        // Adds limits on how many bytes are allowed.
        this.channel.pipeline().addLast( new BytesAccumulator( maxMessageSize ) );
    }

    @Override
    public void afterRequestReceived()
    {
        // There is a race here between the bolt thread removing the handler
        // and netty receiving and request. Here we create a local var to
        // prevent a NPE.
        var localTimeoutHandler = timeoutHandler;
        if ( localTimeoutHandler != null )
        {
            localTimeoutHandler.setRequestReceived( true );
        }
    }

    public void disable()
    {
        // Netty ensures that channel pipelines are cleared when a channel becomes inactive, causing further interactions with the pipeline to fail.
        // Cleanup of the remaining resources managed by this implementation has already been performed by the time of this call (via the channel inactivity
        // listeners registered during channel initialization).
        if ( !this.channel.isActive() )
        {
            return;
        }

        // Removes auth timeout handlers.
        this.channel.pipeline().remove( AuthenticationTimeoutHandler.class );
        timeoutHandler = null;

        // Remove byte limits
        this.channel.pipeline().remove( BytesAccumulator.class );

        memoryTracker.releaseHeap( AuthenticationTimeoutHandler.SHALLOW_SIZE + BytesAccumulator.SHALLOW_SIZE );
    }
}
