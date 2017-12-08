/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus;

import java.util.function.LongSupplier;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;

public class LeaderAvailabilityHandler implements LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage>
{
    private final LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> delegateHandler;
    private final LeaderAvailabilityTimers leaderAvailabilityTimers;
    private final LongSupplier term;

    public LeaderAvailabilityHandler( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> delegateHandler,
            LeaderAvailabilityTimers leaderAvailabilityTimers, LongSupplier term )
    {
        this.delegateHandler = delegateHandler;
        this.leaderAvailabilityTimers = leaderAvailabilityTimers;
        this.term = term;
    }

    public static ComposableMessageHandler composable( LeaderAvailabilityTimers leaderAvailabilityTimers, LongSupplier term )
    {
        return ( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> delegate ) ->
                new LeaderAvailabilityHandler( delegate, leaderAvailabilityTimers, term );
    }

    @Override
    public synchronized void start( ClusterId clusterId ) throws Throwable
    {
        delegateHandler.start( clusterId );
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        delegateHandler.stop();
    }

    @Override
    public void handle( RaftMessages.ReceivedInstantClusterIdAwareMessage message )
    {
        handleTimeouts( message );
        delegateHandler.handle( message );
    }

    private void handleTimeouts( RaftMessages.ReceivedInstantClusterIdAwareMessage message )
    {
        if ( shouldRenewElectionTimeout( message.message() ) )
        {
            leaderAvailabilityTimers.renewElection();
        }
    }

    // TODO replace with visitor pattern
    private boolean shouldRenewElectionTimeout( RaftMessages.RaftMessage message )
    {
        switch ( message.type() )
        {
        case HEARTBEAT:
            RaftMessages.Heartbeat heartbeat = (RaftMessages.Heartbeat) message;
            return heartbeat.leaderTerm() >= term.getAsLong();
        case APPEND_ENTRIES_REQUEST:
            RaftMessages.AppendEntries.Request request = (RaftMessages.AppendEntries.Request) message;
            return request.leaderTerm() >= term.getAsLong();
        default:
            return false;
        }
    }
}
