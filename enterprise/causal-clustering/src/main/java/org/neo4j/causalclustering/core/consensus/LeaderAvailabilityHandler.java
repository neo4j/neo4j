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
    private final ShouldRenewElectionTimeout shouldRenewElectionTimeout;

    public LeaderAvailabilityHandler( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> delegateHandler,
            LeaderAvailabilityTimers leaderAvailabilityTimers, LongSupplier term )
    {
        this.delegateHandler = delegateHandler;
        this.leaderAvailabilityTimers = leaderAvailabilityTimers;
        this.shouldRenewElectionTimeout = new ShouldRenewElectionTimeout( term );
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
        if ( message.dispatch( shouldRenewElectionTimeout ) )
        {
            leaderAvailabilityTimers.renewElection();
        }
    }

    private static class ShouldRenewElectionTimeout implements RaftMessages.Handler<Boolean, RuntimeException>
    {
        private final LongSupplier term;

        private ShouldRenewElectionTimeout( LongSupplier term )
        {
            this.term = term;
        }

        @Override
        public Boolean handle( RaftMessages.AppendEntries.Request request )
        {
            return request.leaderTerm() >= term.getAsLong();
        }

        @Override
        public Boolean handle( RaftMessages.Heartbeat heartbeat )
        {
            return heartbeat.leaderTerm() >= term.getAsLong();
        }

        @Override
        public Boolean handle( RaftMessages.Vote.Request request )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.Vote.Response response )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.PreVote.Request request )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.PreVote.Response response )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.AppendEntries.Response response )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.Timeout.Election election )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.Timeout.Heartbeat heartbeat )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.NewEntry.Request request )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.NewEntry.BatchRequest batchRequest )
        {
            return false;
        }

        @Override
        public Boolean handle( RaftMessages.PruneRequest pruneRequest )
        {
            return false;
        }
    }
}
