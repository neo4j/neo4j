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

import io.netty.channel.ChannelPipeline;

import java.time.Duration;

import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;

/**
 * Protect the channel from unauthenticated users by limiting the resources that they can access to.
 */
public class UnauthenticatedChannelProtector implements ChannelProtector
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( UnauthenticatedChannelProtector.class );

    private final Duration channelTimeout;
    private final ChannelPipeline pipeline;
    private final long maxMessageSize;
    private final MemoryTracker memoryTracker;

    public UnauthenticatedChannelProtector( ChannelPipeline pipeline, Duration channelTimeout, long maxMessageSize, MemoryTracker memoryTracker )
    {
        this.channelTimeout = channelTimeout;
        this.pipeline = pipeline;
        this.maxMessageSize = maxMessageSize;
        this.memoryTracker = memoryTracker;
    }

    public void afterChannelCreated()
    {
        memoryTracker.allocateHeap( AuthenticationTimeoutTracker.SHALLOW_SIZE + AuthenticationTimeoutHandler.SHALLOW_SIZE );

        // Adds auth timeout handlers.
        // The timer is counting down after installation.
        pipeline.addLast( new AuthenticationTimeoutTracker( channelTimeout ) );
        pipeline.addLast( new AuthenticationTimeoutHandler( channelTimeout ) );
    }

    public void beforeBoltProtocolInstalled()
    {
        memoryTracker.allocateHeap( BytesAccumulator.SHALLOW_SIZE );

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

        memoryTracker.releaseHeap(
                AuthenticationTimeoutTracker.SHALLOW_SIZE + AuthenticationTimeoutHandler.SHALLOW_SIZE +
                BytesAccumulator.SHALLOW_SIZE );
    }
}
