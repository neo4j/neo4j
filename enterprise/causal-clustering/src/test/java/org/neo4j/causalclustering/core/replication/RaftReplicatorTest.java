/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.core.replication;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

class RaftReplicatorTest
{
    private static final int DEFAULT_TIMEOUT_MS = 15_000;

    private LeaderLocator leaderLocator = mock( LeaderLocator.class );
    private MemberId myself = new MemberId( UUID.randomUUID() );
    private LeaderInfo leaderInfo = new LeaderInfo( new MemberId( UUID.randomUUID() ), 1 );
    private GlobalSession session = new GlobalSession( UUID.randomUUID(), myself );
    private LocalSessionPool sessionPool = new LocalSessionPool( session );
    private TimeoutStrategy noWaitTimeoutStrategy = new ConstantTimeTimeoutStrategy( 0, MILLISECONDS );
    private DatabaseAvailabilityGuard databaseAvailabilityGuard;
    private DatabaseHealth databaseHealth;
    private LocalDatabase localDatabase;

    @BeforeEach
    void setUp() throws IOException
    {
        databaseAvailabilityGuard = new DatabaseAvailabilityGuard( DEFAULT_DATABASE_NAME, Clocks.systemClock(), NullLog.getInstance() );
        databaseHealth = new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), NullLog.getInstance() );
        localDatabase = StubLocalDatabase.create( () -> databaseHealth, databaseAvailabilityGuard );
        localDatabase.start();
    }

    @Test
    void shouldSendReplicatedContentToLeader() throws Exception
    {
        // given
        Monitors monitors = new Monitors();
        ReplicationMonitor replicationMonitor = mock( ReplicationMonitor.class );
        monitors.addMonitorListener( replicationMonitor );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, monitors );
        replicator.onLeaderSwitch( leaderInfo );

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
        assertEquals( leaderInfo.memberId(), outbound.lastTo );

        verify( replicationMonitor, times( 1 ) ).startReplication();
        verify( replicationMonitor, atLeast( 1 ) ).replicationAttempt();
        verify( replicationMonitor, times( 1 ) ).successfulReplication();
        verify( replicationMonitor, never() ).failedReplication( any() );
    }

    @Test
    void shouldResendAfterTimeout() throws Exception
    {
        // given
        Monitors monitors = new Monitors();
        ReplicationMonitor replicationMonitor = mock( ReplicationMonitor.class );
        monitors.addMonitorListener( replicationMonitor );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, monitors );
        replicator.onLeaderSwitch( leaderInfo );

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
    void shouldReleaseSessionWhenFinished() throws Exception
    {
        // given
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        replicator.onLeaderSwitch( leaderInfo );
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
    void stopReplicationOnShutdown() throws InterruptedException
    {
        // given
        Monitors monitors = new Monitors();
        ReplicationMonitor replicationMonitor = mock( ReplicationMonitor.class );
        monitors.addMonitorListener( replicationMonitor );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, monitors );
        replicator.onLeaderSwitch( leaderInfo );
        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content, true );

        // when
        replicatingThread.start();

        databaseAvailabilityGuard.shutdown();
        replicatingThread.join();
        assertThat( replicatingThread.getReplicationException().getCause(), Matchers.instanceOf( UnavailableException.class ) );

        verify( replicationMonitor, times( 1 ) ).startReplication();
        verify( replicationMonitor, atLeast( 1 ) ).replicationAttempt();
        verify( replicationMonitor, never() ).successfulReplication();
        verify( replicationMonitor, times( 1 ) ).failedReplication( any() );
    }

    @Test
    void stopReplicationWhenUnavailable() throws InterruptedException
    {
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        replicator.onLeaderSwitch( leaderInfo );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content, true );

        // when
        replicatingThread.start();

        databaseAvailabilityGuard.require( () -> "Database not unavailable" );
        replicatingThread.join();
        assertThat( replicatingThread.getReplicationException().getCause(), Matchers.instanceOf( UnavailableException.class ) );
    }

    @Test
    void stopReplicationWhenUnHealthy() throws InterruptedException
    {
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        replicator.onLeaderSwitch( leaderInfo );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content, true );

        // when
        replicatingThread.start();

        databaseHealth.panic( new IllegalStateException( "PANIC" ) );
        replicatingThread.join();
        Assertions.assertNotNull( replicatingThread.getReplicationException() );
    }

    @Test
    void shouldFailIfNoLeaderIsAvailable()
    {
        // given
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );

        // when
        assertThrows( ReplicationFailureException.class, () -> replicator.replicate( content, true ) );
    }

    @Test
    void shouldListenToLeaderUpdates() throws ReplicationFailureException
    {
        OneProgressTracker oneProgressTracker = new OneProgressTracker();
        oneProgressTracker.last.setReplicated();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();
        RaftReplicator replicator = getReplicator( outbound, oneProgressTracker, new Monitors() );
        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );

        LeaderInfo lastLeader = leaderInfo;

        // set initial leader, sens to that leader
        replicator.onLeaderSwitch( lastLeader );
        replicator.replicate( content, false );
        assertEquals( outbound.lastTo, lastLeader.memberId() );

        // update with valid new leader, sends to new leader
        lastLeader = new LeaderInfo( new MemberId( UUID.randomUUID() ), 1 );
        replicator.onLeaderSwitch( lastLeader );
        replicator.replicate( content, false );
        assertEquals( outbound.lastTo, lastLeader.memberId() );
    }

    @Test
    void shouldSuccessfullySendIfLeaderIsLostAndFound() throws InterruptedException
    {
        OneProgressTracker capturedProgress = new OneProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        replicator.onLeaderSwitch( leaderInfo );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content, false );

        // when
        replicatingThread.start();

        // then
        assertEventually( "send count", () -> outbound.count, greaterThan( 1 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        replicator.onLeaderSwitch( new LeaderInfo( null, 1 ) );
        capturedProgress.last.setReplicated();
        replicator.onLeaderSwitch( leaderInfo );

        replicatingThread.join( DEFAULT_TIMEOUT_MS );
    }

    private RaftReplicator getReplicator( CapturingOutbound<RaftMessages.RaftMessage> outbound, ProgressTracker progressTracker, Monitors monitors )
    {
        return new RaftReplicator( leaderLocator, myself, outbound, sessionPool, progressTracker, noWaitTimeoutStrategy, 10, databaseAvailabilityGuard,
                NullLogProvider.getInstance(), localDatabase, monitors );
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

    private class OneProgressTracker extends ProgressTrackerAdaptor
    {
        OneProgressTracker()
        {
            last = new Progress();
        }

        @Override
        public Progress start( DistributedOperation operation )
        {
            return last;
        }
    }

    private class CapturingProgressTracker extends ProgressTrackerAdaptor
    {
        @Override
        public Progress start( DistributedOperation operation )
        {
            last = new Progress();
            return last;
        }
    }

    private abstract class ProgressTrackerAdaptor implements ProgressTracker
    {
        protected Progress last;

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
            // do nothing
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

    private static class StubLocalDatabase extends LocalDatabase
    {
        static LocalDatabase create( Supplier<DatabaseHealth> databaseHealthSupplier, AvailabilityGuard availabilityGuard ) throws IOException
        {
            StoreFiles storeFiles = mock( StoreFiles.class );
            when( storeFiles.readStoreId( any() ) ).thenReturn( new StoreId( 1, 2, 3, 4 ) );

            DataSourceManager dataSourceManager = mock( DataSourceManager.class );
            return new StubLocalDatabase( storeFiles, dataSourceManager, databaseHealthSupplier, availabilityGuard );
        }

        StubLocalDatabase( StoreFiles storeFiles, DataSourceManager dataSourceManager, Supplier<DatabaseHealth> databaseHealthSupplier,
                AvailabilityGuard availabilityGuard )
        {
            super( null, storeFiles, null, dataSourceManager, databaseHealthSupplier, availabilityGuard, NullLogProvider.getInstance() );
        }
    }
}
