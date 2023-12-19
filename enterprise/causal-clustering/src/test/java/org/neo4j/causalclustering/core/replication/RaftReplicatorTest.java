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
import org.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class RaftReplicatorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final int DEFAULT_TIMEOUT_MS = 15_000;
    private static final long REPLICATION_LIMIT = 1000;

    private LeaderLocator leaderLocator = mock( LeaderLocator.class );
    private MemberId myself = new MemberId( UUID.randomUUID() );
    private MemberId leader = new MemberId( UUID.randomUUID() );
    private MemberId anotherLeader = new MemberId( UUID.randomUUID() );
    private GlobalSession session = new GlobalSession( UUID.randomUUID(), myself );
    private LocalSessionPool sessionPool = new LocalSessionPool( session );
    private TimeoutStrategy noWaitTimeoutStrategy = new ConstantTimeTimeoutStrategy( 0, MILLISECONDS );
    private AvailabilityGuard availabilityGuard = new AvailabilityGuard( Clocks.systemClock(), NullLog.getInstance() );

    @Test
    public void shouldSendReplicatedContentToLeader() throws Exception
    {
        // given
        Monitors monitors = new Monitors();
        ReplicationMonitor replicationMonitor = mock( ReplicationMonitor.class );
        monitors.addMonitorListener( replicationMonitor );
        when( leaderLocator.getLeader() ).thenReturn( leader );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, monitors );

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

        verify( replicationMonitor, times( 1 ) ).startReplication();
        verify( replicationMonitor, atLeast( 1 ) ).replicationAttempt();
        verify( replicationMonitor, times( 1 ) ).successfulReplication();
        verify( replicationMonitor, never() ).failedReplication( any() );
    }

    @Test
    public void shouldResendAfterTimeout() throws Exception
    {
        // given
        Monitors monitors = new Monitors();
        ReplicationMonitor replicationMonitor = mock( ReplicationMonitor.class );
        monitors.addMonitorListener( replicationMonitor );
        when( leaderLocator.getLeader() ).thenReturn( leader );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, monitors );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        Thread replicatingThread = replicatingThread( replicator, content, false );

        // when
        replicatingThread.start();
        // then
        assertEventually( "send count", () -> outbound.count, greaterThan( 2 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );

        // cleanup
        capturedProgress.last.setReplicated();
        replicatingThread.join( DEFAULT_TIMEOUT_MS );

        verify( replicationMonitor, times( 1 ) ).startReplication();
        verify( replicationMonitor, atLeast( 2 ) ).replicationAttempt();
        verify( replicationMonitor, times( 1 ) ).successfulReplication();
        verify( replicationMonitor, never() ).failedReplication( any() );
    }

    @Test
    public void shouldReleaseSessionWhenFinished() throws Exception
    {
        // given
        when( leaderLocator.getLeader() ).thenReturn( leader );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
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
        // given
        Monitors monitors = new Monitors();
        ReplicationMonitor replicationMonitor = mock( ReplicationMonitor.class );
        monitors.addMonitorListener( replicationMonitor );
        when( leaderLocator.getLeader() ).thenReturn( leader );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, monitors );
        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content, true );

        // when
        replicatingThread.start();

        availabilityGuard.shutdown();
        replicatingThread.join();
        assertThat( replicatingThread.getReplicationException().getCause(), Matchers.instanceOf( AvailabilityGuard.UnavailableException.class ) );

        verify( replicationMonitor, times( 1 ) ).startReplication();
        verify( replicationMonitor, atLeast( 1 ) ).replicationAttempt();
        verify( replicationMonitor, never() ).successfulReplication();
        verify( replicationMonitor, times( 1 ) ).failedReplication( any() );
    }

    @Test
    public void stopReplicationWhenUnavailable() throws NoLeaderFoundException, InterruptedException
    {
        when( leaderLocator.getLeader() ).thenReturn( leader );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content, true );

        // when
        replicatingThread.start();

        availabilityGuard.require( () -> "Database not unavailable" );
        replicatingThread.join();
        assertThat( replicatingThread.getReplicationException().getCause(), Matchers.instanceOf( AvailabilityGuard.UnavailableException.class ) );
    }

    @Test
    public void shouldFailIfNoLeaderIsAvailable() throws NoLeaderFoundException
    {
        // given
        when( leaderLocator.getLeader() ).thenThrow( NoLeaderFoundException.class );

        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );

        // when
        try
        {
            ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
            replicator.replicate( content, true );
            fail( "should have thrown" );
        }
        catch ( ReplicationFailureException ignored )
        {
            // expected
        }
    }

    @Test
    public void shouldFailIfLeaderSwitches() throws NoLeaderFoundException
    {
        // given
        when( leaderLocator.getLeader() ).thenReturn( leader ).thenReturn( anotherLeader );

        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );

        // when
        try
        {
            ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
            replicator.replicate( content, true );
            fail( "should have thrown" );
        }
        catch ( ReplicationFailureException e )
        {
            assertEquals( "Replication aborted since a leader switch was detected", e.getMessage() );
        }
    }

    private RaftReplicator getReplicator( CapturingOutbound<RaftMessages.RaftMessage> outbound, CapturingProgressTracker capturedProgress, Monitors monitors )
    {
        return new RaftReplicator( leaderLocator, myself, outbound, sessionPool, capturedProgress, noWaitTimeoutStrategy, noWaitTimeoutStrategy,
                10, availabilityGuard, NullLogProvider.getInstance(), REPLICATION_LIMIT, monitors );
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

        Exception getReplicationException()
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

    private static class CapturingOutbound<MESSAGE extends Message> implements Outbound<MemberId, MESSAGE>
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
