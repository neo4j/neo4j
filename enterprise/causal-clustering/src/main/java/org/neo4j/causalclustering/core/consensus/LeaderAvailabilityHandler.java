/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus;

import java.util.function.LongSupplier;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;

public class LeaderAvailabilityHandler implements LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>>
{
    private final LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> delegateHandler;
    private final LeaderAvailabilityTimers leaderAvailabilityTimers;
    private final ShouldRenewElectionTimeout shouldRenewElectionTimeout;

    public LeaderAvailabilityHandler( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> delegateHandler,
            LeaderAvailabilityTimers leaderAvailabilityTimers, LongSupplier term )
    {
        this.delegateHandler = delegateHandler;
        this.leaderAvailabilityTimers = leaderAvailabilityTimers;
        this.shouldRenewElectionTimeout = new ShouldRenewElectionTimeout( term );
    }

    public static ComposableMessageHandler composable( LeaderAvailabilityTimers leaderAvailabilityTimers, LongSupplier term )
    {
        return delegate -> new LeaderAvailabilityHandler( delegate, leaderAvailabilityTimers, term );
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
    public void handle( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message )
    {
        handleTimeouts( message );
        delegateHandler.handle( message );
    }

    private void handleTimeouts( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message )
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
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.Vote.Response response )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.PreVote.Request request )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.PreVote.Response response )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.AppendEntries.Response response )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.Timeout.Election election )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.Timeout.Heartbeat heartbeat )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.NewEntry.Request request )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.NewEntry.BatchRequest batchRequest )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.PruneRequest pruneRequest )
        {
            return Boolean.FALSE;
        }
    }
}
