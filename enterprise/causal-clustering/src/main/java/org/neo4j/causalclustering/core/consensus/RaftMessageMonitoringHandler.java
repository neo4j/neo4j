/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.kernel.monitoring.Monitors;

public class RaftMessageMonitoringHandler implements LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>>
{
    private final LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> raftMessageHandler;
    private final Clock clock;
    private final RaftMessageProcessingMonitor raftMessageDelayMonitor;

    public RaftMessageMonitoringHandler( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> raftMessageHandler,
            Clock clock, Monitors monitors )
    {
        this.raftMessageHandler = raftMessageHandler;
        this.clock = clock;
        this.raftMessageDelayMonitor = monitors.newMonitor( RaftMessageProcessingMonitor.class );
    }

    public static ComposableMessageHandler composable( Clock clock, Monitors monitors )
    {
        return delegate -> new RaftMessageMonitoringHandler( delegate, clock, monitors );
    }

    @Override
    public synchronized void handle( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> incomingMessage )
    {
        Instant start = clock.instant();

        logDelay( incomingMessage, start );

        timeHandle( incomingMessage, start );
    }

    private void timeHandle( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> incomingMessage, Instant start )
    {
        try
        {
            raftMessageHandler.handle( incomingMessage );
        }
        finally
        {
            Duration duration = Duration.between( start, clock.instant() );
            raftMessageDelayMonitor.updateTimer( incomingMessage.type(), duration );
        }
    }

    private void logDelay( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> incomingMessage, Instant start )
    {
        Duration delay = Duration.between( incomingMessage.receivedAt(), start );

        raftMessageDelayMonitor.setDelay( delay );
    }

    @Override
    public void start( ClusterId clusterId ) throws Throwable
    {
        raftMessageHandler.start( clusterId );
    }

    @Override
    public void stop() throws Throwable
    {
        raftMessageHandler.stop();
    }
}
