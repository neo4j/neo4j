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

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
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
import org.neo4j.logging.NullLogProvider;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoreStateTest
{
    private final InMemoryRaftLog raftLog = new InMemoryRaftLog();

    private final InMemoryStateStorage<Long> lastFlushedStorage = new InMemoryStateStorage<>( -1L );
    private final InMemoryStateStorage<Long> lastApplyingStorage = new InMemoryStateStorage<>( -1L );
    private final InMemoryStateStorage<GlobalSessionTrackerState<CoreMember>> sessionStorage = new InMemoryStateStorage<>( new GlobalSessionTrackerState<>() );

    private final DatabaseHealth dbHealth = new DatabaseHealth( mock( DatabasePanicEventGenerator.class ),
            NullLogProvider.getInstance().getLog( getClass() ) );

    private final GlobalSession<CoreMember> globalSession = new GlobalSession<>( UUID.randomUUID(), null );
    private final int flushEvery = 10;
    private final CoreState coreState = new CoreState( raftLog, flushEvery, () -> dbHealth, NullLogProvider.getInstance(),
            new ProgressTrackerImpl( globalSession ), lastFlushedStorage, lastApplyingStorage, sessionStorage,
            mock( CoreServerSelectionStrategy.class ), mock( CoreStateDownloader.class ) );

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
        when( stateMachines.dispatch( any( ReplicatedTransaction.class ), anyLong() ) ).thenThrow( new IllegalStateException() );
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
        coreState.setStateMachine( txStateMachine, -1 );
        coreState.start();

        // when
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        coreState.notifyCommitted( 2 );
        coreState.syncExecutor( false, false );

        // then
        verify( txStateMachine ).dispatch( nullTx, 0 );
        verify( txStateMachine ).dispatch( nullTx, 1 );
        verify( txStateMachine ).dispatch( nullTx, 2 );
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
        coreState.syncExecutor( false, false );

        // then
        verify( txStateMachine, times( 0 ) ).dispatch( any( ReplicatedTransaction.class ), anyInt() );
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
        coreState.notifyCommitted( flushEvery*TIMES );
        coreState.syncExecutor( false, false );

        // then
        verify( txStateMachine, times( TIMES ) ).flush();
        assertEquals( flushEvery*(TIMES-1), (long)lastFlushedStorage.getInitialState() );
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
        coreState.syncExecutor( false, false );

        // then
        assertEquals( false, dbHealth.isHealthy() );
    }
}
