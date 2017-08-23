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
package org.neo4j.causalclustering.core.replication;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.logging.NullLog;
import org.neo4j.time.Clocks;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class RaftReplicatorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final int DEFAULT_TIMEOUT_MS = 15_000;

    private LeaderLocator leaderLocator = mock( LeaderLocator.class );
    private MemberId myself = new MemberId( UUID.randomUUID() );
    private MemberId leader = new MemberId( UUID.randomUUID() );
    private GlobalSession session = new GlobalSession( UUID.randomUUID(), myself );
    private LocalSessionPool sessionPool = new LocalSessionPool( session );
    private TimeoutStrategy timeoutStrategy = new ConstantTimeTimeoutStrategy( 0, MILLISECONDS );
    private AvailabilityGuard availabilityGuard = new AvailabilityGuard( Clocks.systemClock(), NullLog.getInstance() );
    private long replicationLimit = 1000;

    @Test
    public void shouldSendReplicatedContentToLeader() throws Exception
    {
        // given
        when( leaderLocator.getLeader() ).thenReturn( leader );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator =
                new RaftReplicator( leaderLocator, myself, outbound, sessionPool,
                        capturedProgress, timeoutStrategy, availabilityGuard, NullLogProvider.getInstance(), replicationLimit );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        Thread replicatingThread = replicatingThread( replicator, content, false );

        // when
        replicatingThread.start();
        // then
        assertEventually( "making progress", () -> capturedProgress.last, not( equalTo( null ) ), DEFAULT_TIMEOUT_MS, MILLISECONDS );

        // when
        capturedProgress.last.setReplicated();

        // then
        replicatingThread.join( DEFAULT_TIMEOUT_MS );
        assertEquals( leader, outbound.lastTo );
    }

    @Test
    public void shouldResendAfterTimeout() throws Exception
    {
        // given
        when( leaderLocator.getLeader() ).thenReturn( leader );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound outbound = new CapturingOutbound();

        RaftReplicator replicator = new RaftReplicator( leaderLocator, myself, outbound,
                sessionPool, capturedProgress, timeoutStrategy, availabilityGuard, NullLogProvider.getInstance(), replicationLimit );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        Thread replicatingThread = replicatingThread( replicator, content, false );

        // when
        replicatingThread.start();
        // then
        assertEventually( "send count", () -> outbound.count, greaterThan( 2 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );

        // cleanup
        capturedProgress.last.setReplicated();
        replicatingThread.join( DEFAULT_TIMEOUT_MS );
    }

    @Test
    public void shouldReleaseSessionWhenFinished() throws Exception
    {
        // given
        when( leaderLocator.getLeader() ).thenReturn( leader );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound outbound = new CapturingOutbound();

        RaftReplicator replicator = new RaftReplicator( leaderLocator, myself, outbound,
                sessionPool, capturedProgress, timeoutStrategy, availabilityGuard, NullLogProvider.getInstance(), replicationLimit );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        Thread replicatingThread = replicatingThread( replicator, content, true );

        // when
        replicatingThread.start();

        // then
        assertEventually( "making progress", () -> capturedProgress.last, not( equalTo( null ) ),
                DEFAULT_TIMEOUT_MS, MILLISECONDS );
        assertEquals( 1, sessionPool.openSessionCount() );

        // when
        capturedProgress.last.setReplicated();
        capturedProgress.last.futureResult().complete( 5 );
        replicatingThread.join( DEFAULT_TIMEOUT_MS );

        // then
        assertEquals( 0, sessionPool.openSessionCount() );
    }

    @Test
    public void stopReplicationOnShutdown() throws NoLeaderFoundException, InterruptedException
    {
        when( leaderLocator.getLeader() ).thenReturn( leader );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound outbound = new CapturingOutbound();

        RaftReplicator replicator =
                new RaftReplicator( leaderLocator, myself, outbound, sessionPool, capturedProgress, timeoutStrategy,
                        availabilityGuard, NullLogProvider.getInstance(), replicationLimit );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content, true );

        // when
        replicatingThread.start();

        availabilityGuard.shutdown();
        replicatingThread.join();
        assertThat( replicatingThread.getReplicationException(), Matchers.instanceOf( DatabaseShutdownException.class ) );
    }

    private ReplicatingThread replicatingThread( RaftReplicator replicator, ReplicatedInteger content, boolean trackResult )
    {
        return new ReplicatingThread( replicator, content, trackResult );
    }

    private class ReplicatingThread extends Thread
    {

        private final RaftReplicator replicator;
        private final ReplicatedInteger content;
        private final boolean trackResult;
        private volatile Exception replicationException;

        ReplicatingThread( RaftReplicator replicator, ReplicatedInteger content, boolean trackResult )
        {
            this.replicator = replicator;
            this.content = content;
            this.trackResult = trackResult;
        }

        @Override
        public void run()
        {
            try
            {
                Future<Object> futureResult = replicator.replicate( content, trackResult );
                if ( trackResult )
                {
                    try
                    {
                        futureResult.get();
                    }
                    catch ( ExecutionException e )
                    {
                        replicationException = e;
                        throw new IllegalStateException();
                    }
                }
            }
            catch ( Exception e )
            {
                replicationException = e;
            }
        }

        public Exception getReplicationException()
        {
            return replicationException;
        }
    }

    private class CapturingProgressTracker implements ProgressTracker
    {
        private Progress last;

        @Override
        public Progress start( DistributedOperation operation )
        {
            last = new Progress();
            return last;
        }

        @Override
        public void trackReplication( DistributedOperation operation )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void trackResult( DistributedOperation operation, Result result )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abort( DistributedOperation operation )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void triggerReplicationEvent()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int inProgressCount()
        {
            throw new UnsupportedOperationException();
        }
    }

    private class CapturingOutbound<MESSAGE extends Message> implements Outbound<MemberId, MESSAGE>
    {
        private MemberId lastTo;
        private int count;

        @Override
        public void send( MemberId to, MESSAGE message, boolean block )
        {
            this.lastTo = to;
            this.count++;
        }

    }
}
