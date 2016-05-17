/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.state;

import org.junit.Test;

import java.util.Optional;
import java.util.UUID;

import org.neo4j.coreedge.raft.NewLeaderBarrier;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.log.monitoring.RaftLogCommitIndexMonitor;
import org.neo4j.coreedge.raft.replication.DistributedOperation;
import org.neo4j.coreedge.raft.replication.ProgressTrackerImpl;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.raft.replication.tx.CoreReplicatedContent;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransaction;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.edge.CoreServerSelectionStrategy;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class CoreStateTest
{
    private final InMemoryRaftLog raftLog = spy( new InMemoryRaftLog() );

    private final InMemoryStateStorage<Long> lastFlushedStorage = new InMemoryStateStorage<>( -1L );
    private final InMemoryStateStorage<Long> lastApplyingStorage = new InMemoryStateStorage<>( -1L );
    private final InMemoryStateStorage<GlobalSessionTrackerState<CoreMember>> sessionStorage =
            new InMemoryStateStorage<>( new GlobalSessionTrackerState<>() );

    private final DatabaseHealth dbHealth = new DatabaseHealth( mock( DatabasePanicEventGenerator.class ),
            NullLogProvider.getInstance().getLog( getClass() ) );

    private final GlobalSession<CoreMember> globalSession = new GlobalSession<>( UUID.randomUUID(), null );
    private final int flushEvery = 10;

    private final CoreStateApplier applier = new CoreStateApplier( NullLogProvider.getInstance() );
    private InFlightMap<Long,RaftLogEntry> inFlightMap = spy( new InFlightMap<>() );
    private final Monitors monitors = new Monitors();
    private final CoreState coreState = new CoreState( raftLog, flushEvery, () -> dbHealth, NullLogProvider.getInstance(),
            new ProgressTrackerImpl( globalSession ), lastFlushedStorage, lastApplyingStorage, sessionStorage,
            mock( CoreServerSelectionStrategy.class ), applier, mock( CoreStateDownloader.class ), inFlightMap,
            monitors );

    private ReplicatedTransaction nullTx = new ReplicatedTransaction( null );

    private final CoreStateMachines txStateMachine = txStateMachinesMock();

    private CoreStateMachines txStateMachinesMock()
    {
        CoreStateMachines stateMachines = mock( CoreStateMachines.class );
        when( stateMachines.dispatch( any( ReplicatedTransaction.class ), anyLong() ) ).thenReturn( Optional.empty() );
        return stateMachines;
    }

    private final CoreStateMachines failingTxStateMachine = failingTxStateMachinesMock();

    private CoreStateMachines failingTxStateMachinesMock()
    {
        CoreStateMachines stateMachines = mock( CoreStateMachines.class );
        when( stateMachines.dispatch( any( ReplicatedTransaction.class ), anyLong() ) ).thenThrow(
                new IllegalStateException( "This is a failing tx state machine and it's supposed to fail" ) );
        return stateMachines;
    }

    private int sequenceNumber = 0;

    private synchronized ReplicatedContent operation( CoreReplicatedContent tx )
    {
        return new DistributedOperation( tx, globalSession, new LocalOperationId( 0, sequenceNumber++ ) );
    }

    @Test
    public void shouldApplyCommittedCommand() throws Exception
    {
        // given
        RaftLogCommitIndexMonitor listener = mock( RaftLogCommitIndexMonitor.class );
        monitors.addMonitorListener( listener );
        coreState.setStateMachine( txStateMachine, -1 );
        coreState.start();

        // when
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        coreState.notifyCommitted( 2 );
        applier.sync( false );

        // then
        verify( txStateMachine ).dispatch( nullTx, 0 );
        verify( txStateMachine ).dispatch( nullTx, 1 );
        verify( txStateMachine ).dispatch( nullTx, 2 );
        verify( listener).commitIndex( 2 );
    }

    @Test
    public void shouldNotApplyUncommittedCommands() throws Exception
    {
        // given
        coreState.setStateMachine( txStateMachine, -1 );
        coreState.start();

        // when
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        coreState.notifyCommitted( -1 );
        applier.sync( false );

        // then
        verify( txStateMachine, times( 0 ) ).dispatch( any( ReplicatedTransaction.class ), anyInt() );
    }

    @Test
    public void entriesThatAreNotStateMachineCommandsShouldStillIncreaseCommandIndex() throws Exception
    {
        // given
        coreState.setStateMachine( txStateMachine, -1 );
        coreState.start();

        // when
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        coreState.notifyCommitted( 1 );
        applier.sync( false );

        // then
        verify( txStateMachine ).dispatch( nullTx, 1L );
    }

    // TODO: Test recovery, see CoreState#start().

    @Test
    public void shouldPeriodicallyFlushState() throws Exception
    {
        // given
        coreState.setStateMachine( txStateMachine, -1 );
        coreState.start();

        int TIMES = 5;
        for ( int i = 0; i < flushEvery * TIMES; i++ )
        {
            raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        }

        // when
        coreState.notifyCommitted( flushEvery * TIMES );
        applier.sync( false );

        // then
        verify( txStateMachine, times( TIMES ) ).flush();
        assertEquals( flushEvery * (TIMES - 1), (long) lastFlushedStorage.getInitialState() );
    }

    @Test
    public void shouldPanicIfUnableToApply() throws Exception
    {
        // given
        coreState.setStateMachine( failingTxStateMachine, -1 );
        coreState.start();

        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        assertEquals( true, dbHealth.isHealthy() );
        coreState.notifyCommitted( 0 );
        applier.sync( false );

        // then
        assertEquals( false, dbHealth.isHealthy() );
    }

    @Test
    public void shouldApplyToLogFromCache() throws Throwable
    {
        //given n things to apply in the cache, check that they are actually applied.

        // given
        coreState.setStateMachine( txStateMachine, -1 );
        coreState.start();

        inFlightMap.register( 0L, new RaftLogEntry( 1, operation( nullTx ) ) );

        //when
        coreState.notifyCommitted( 0 );
        applier.sync( false );

        //then the cache should have had it's get method called.
        verify( inFlightMap, times( 1 ) ).retrieve( 0L );
        verifyZeroInteractions( raftLog );
    }

    @Test
    public void cacheEntryShouldBePurgedWhenApplied() throws Throwable
    {
        //given a cache in submitApplyJob, the contents of the cache should only contain unapplied "things"

        // given
        coreState.setStateMachine( txStateMachine, -1 );
        coreState.start();

        inFlightMap.register( 0L, new RaftLogEntry( 0, operation( nullTx ) ) );
        inFlightMap.register( 1L, new RaftLogEntry( 0, operation( nullTx ) ) );
        inFlightMap.register( 2L, new RaftLogEntry( 0, operation( nullTx ) ) );
        //when
        coreState.notifyCommitted( 0 );

        applier.sync( false );

        //then the cache should have had its get method called.
        assertNull( inFlightMap.retrieve( 0L ) );
        assertNotNull( inFlightMap.retrieve( 1L ) );
        assertNotNull( inFlightMap.retrieve( 2L ) );
    }

    @Test
    public void shouldFallbackToLogCursorOnCacheMiss() throws Throwable
    {
        // if the cache does not contain all things to be applied, make sure we fall back to the log
        // should only happen in recovery, otherwise this is probably a bug.

        coreState.setStateMachine( txStateMachine, -1 );
        coreState.start();

        //given cache with missing entry
        ReplicatedContent operation0 = operation( nullTx );
        ReplicatedContent operation1 = operation( nullTx );
        ReplicatedContent operation2 = operation( nullTx );

        inFlightMap.register( 0L, new RaftLogEntry( 0, operation0 ) );
        inFlightMap.register( 2L, new RaftLogEntry( 2, operation2 ) );

        raftLog.append( new RaftLogEntry( 0, operation0 ) );
        raftLog.append( new RaftLogEntry( 1, operation1 ) );
        raftLog.append( new RaftLogEntry( 2, operation2 ) );

        //when
        coreState.notifyCommitted( 2 );

        applier.sync( false );

        //then the cache should have had its get method called.
        verify( inFlightMap, times( 0 ) ).retrieve( 2L );
        verify( inFlightMap, times( 3 ) ).unregister( anyLong() ); //everything is cleaned up

        verify( txStateMachine, times( 1 ) ).dispatch( nullTx, 0L );
        verify( txStateMachine, times( 1 ) ).dispatch( nullTx, 1L );
        verify( txStateMachine, times( 1 ) ).dispatch( nullTx, 2L );

        verify( raftLog, times( 1 ) ).getEntryCursor( 1 );
    }

    @Test
    public void shouldFailWhenCacheAndLogMiss() throws Exception
    {
        //When an entry is not in the log, we must fail.

        coreState.setStateMachine( txStateMachine, -1 );
        coreState.start();

        inFlightMap.register( 0L, new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 1, operation( nullTx ) ) );

        //when
        coreState.notifyCommitted( 2 );
        applier.sync( false );

        //then
        assertFalse( dbHealth.isHealthy() );
    }
}
