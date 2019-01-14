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
package org.neo4j.causalclustering.core.state;

import org.junit.Test;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.UUID;
import java.util.function.Consumer;

import org.neo4j.causalclustering.SessionTracker;
import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogCommitIndexMonitor;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ProgressTrackerImpl;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CommandApplicationProcessTest
{
    private final InMemoryRaftLog raftLog = spy( new InMemoryRaftLog() );

    private final SessionTracker sessionTracker = new SessionTracker(
            new InMemoryStateStorage<>( new GlobalSessionTrackerState() ) );

    private final DatabaseHealth dbHealth = new DatabaseHealth( mock( DatabasePanicEventGenerator.class ),
            NullLogProvider.getInstance().getLog( getClass() ) );

    private final GlobalSession globalSession = new GlobalSession( UUID.randomUUID(), null );
    private final int flushEvery = 10;
    private final int batchSize = 16;

    private InFlightCache inFlightCache = spy( new ConsecutiveInFlightCache() );
    private final Monitors monitors = new Monitors();
    private CoreState coreState = mock( CoreState.class );
    private final CommandApplicationProcess applicationProcess = new CommandApplicationProcess(
            raftLog, batchSize, flushEvery, () -> dbHealth,
            NullLogProvider.getInstance(), new ProgressTrackerImpl( globalSession ),
            sessionTracker, coreState, inFlightCache, monitors );

    private ReplicatedTransaction nullTx = new ReplicatedTransaction( new byte[0] );

    private final CommandDispatcher commandDispatcher = mock( CommandDispatcher.class );

    {
        when( coreState.commandDispatcher() ).thenReturn( commandDispatcher );
        when( coreState.getLastAppliedIndex() ).thenReturn( -1L );
        when( coreState.getLastFlushed() ).thenReturn( -1L );
    }

    private ReplicatedTransaction tx( byte dataValue )
    {
        byte[] dataArray = new byte[30];
        Arrays.fill( dataArray, dataValue );
        return new ReplicatedTransaction( dataArray );
    }

    private int sequenceNumber;

    private synchronized ReplicatedContent operation( CoreReplicatedContent tx )
    {
        return new DistributedOperation( tx, globalSession, new LocalOperationId( 0, sequenceNumber++ ) );
    }

    @Test
    public void shouldApplyCommittedCommand() throws Throwable
    {
        // given
        RaftLogCommitIndexMonitor listener = mock( RaftLogCommitIndexMonitor.class );
        monitors.addMonitorListener( listener );

        InOrder inOrder = inOrder( coreState, commandDispatcher );

        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( 2 );
        applicationProcess.start();

        // then
        inOrder.verify( coreState ).commandDispatcher();
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 0L ), anyCallback() );
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 1L ), anyCallback() );
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 2L ), anyCallback() );
        inOrder.verify( commandDispatcher ).close();

        verify( listener ).commitIndex( 2 );
    }

    @Test
    public void shouldNotApplyUncommittedCommands() throws Throwable
    {
        // given
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( -1 );
        applicationProcess.start();

        // then
        verifyZeroInteractions( commandDispatcher );
    }

    @Test
    public void entriesThatAreNotStateMachineCommandsShouldStillIncreaseCommandIndex() throws Throwable
    {
        // given
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( 1 );
        applicationProcess.start();

        // then
        InOrder inOrder = inOrder( coreState, commandDispatcher );
        inOrder.verify( coreState ).commandDispatcher();
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 1L ), anyCallback() );
        inOrder.verify( commandDispatcher ).close();
    }

    @Test
    public void duplicatesShouldBeIgnoredButStillIncreaseCommandIndex() throws Exception
    {
        // given
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( nullTx, globalSession, new LocalOperationId( 0, 0 ) ) ) );
        // duplicate
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( nullTx, globalSession, new LocalOperationId( 0, 0 ) ) ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( nullTx, globalSession, new LocalOperationId( 0, 1 ) ) ) );

        // when
        applicationProcess.notifyCommitted( 3 );
        applicationProcess.start();

        // then
        InOrder inOrder = inOrder( coreState, commandDispatcher );
        inOrder.verify( coreState ).commandDispatcher();
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 1L ), anyCallback() );
        // duplicate not dispatched
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 3L ), anyCallback() );
        inOrder.verify( commandDispatcher ).close();
        verifyNoMoreInteractions( commandDispatcher );
    }

    @Test
    public void outOfOrderDuplicatesShouldBeIgnoredButStillIncreaseCommandIndex() throws Exception
    {
        // given
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 100 ), globalSession, new LocalOperationId( 0, 0 ) ) ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 101 ), globalSession, new LocalOperationId( 0, 1 ) ) ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 102 ), globalSession, new LocalOperationId( 0, 2 ) ) ) );
        // duplicate of tx 101
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 101 ), globalSession, new LocalOperationId( 0, 1 ) ) ) );
        // duplicate of tx 100
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 100 ), globalSession, new LocalOperationId( 0, 0 ) ) ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 103 ), globalSession, new LocalOperationId( 0, 3 ) ) ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 104 ), globalSession, new LocalOperationId( 0, 4 ) ) ) );

        // when
        applicationProcess.notifyCommitted( 6 );
        applicationProcess.start();

        // then
        InOrder inOrder = inOrder( coreState, commandDispatcher );
        inOrder.verify( coreState ).commandDispatcher();
        inOrder.verify( commandDispatcher ).dispatch( eq( tx( (byte) 100 ) ), eq( 0L ), anyCallback() );
        inOrder.verify( commandDispatcher ).dispatch( eq( tx( (byte) 101 ) ), eq( 1L ), anyCallback() );
        inOrder.verify( commandDispatcher ).dispatch( eq( tx( (byte) 102 ) ), eq( 2L ), anyCallback() );
        // duplicate of tx 101 not dispatched, at index 3
        // duplicate of tx 100 not dispatched, at index 4
        inOrder.verify( commandDispatcher ).dispatch( eq( tx( (byte) 103 ) ), eq( 5L ), anyCallback() );
        inOrder.verify( commandDispatcher ).dispatch( eq( tx( (byte) 104 ) ), eq( 6L ), anyCallback() );
        inOrder.verify( commandDispatcher ).close();
        verifyNoMoreInteractions( commandDispatcher );
    }

    // TODO: Test recovery, see CoreState#start().

    @Test
    public void shouldPeriodicallyFlushState() throws Throwable
    {
        // given
        int interactions = flushEvery * 5;
        for ( int i = 0; i < interactions; i++ )
        {
            raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        }

        // when
        applicationProcess.notifyCommitted( raftLog.appendIndex() );
        applicationProcess.start();

        // then
        verify( coreState ).flush( batchSize - 1 );
        verify( coreState ).flush( 2 * batchSize - 1 );
        verify( coreState ).flush( 3 * batchSize - 1 );
    }

    @Test
    public void shouldPanicIfUnableToApply() throws Throwable
    {
        // given
        doThrow( RuntimeException.class ).when( commandDispatcher )
                .dispatch( any( ReplicatedTransaction.class ), anyLong(), anyCallback() );
        applicationProcess.start();

        // when
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        applicationProcess.notifyCommitted( 0 );

        assertEventually( "failed apply", dbHealth::isHealthy, is( false ), 5, SECONDS );
    }

    @Test
    public void shouldApplyToLogFromCache() throws Throwable
    {
        // given
        inFlightCache.put( 0L, new RaftLogEntry( 1, operation( nullTx ) ) );

        //when
        applicationProcess.notifyCommitted( 0 );
        applicationProcess.start();

        //then the cache should have had it's get method called.
        verify( inFlightCache, times( 1 ) ).get( 0L );
        verifyZeroInteractions( raftLog );
    }

    @Test
    public void cacheEntryShouldBePurgedAfterBeingApplied() throws Throwable
    {
        // given
        inFlightCache.put( 0L, new RaftLogEntry( 0, operation( nullTx ) ) );
        inFlightCache.put( 1L, new RaftLogEntry( 0, operation( nullTx ) ) );
        inFlightCache.put( 2L, new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( 0 );
        applicationProcess.start();

        // then the cache should have had its get method called.
        assertNull( inFlightCache.get( 0L ) );
        assertNotNull( inFlightCache.get( 1L ) );
        assertNotNull( inFlightCache.get( 2L ) );
    }

    @Test
    public void shouldFailWhenCacheAndLogMiss() throws Throwable
    {
        // given
        inFlightCache.put( 0L, new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 1, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( 2 );
        try
        {
            applicationProcess.start();
            fail();
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }

    @Test
    public void shouldIncreaseLastAppliedForStateMachineCommands() throws Exception
    {
        // given
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( 2 );
        applicationProcess.start();

        // then
        assertEquals( 2, applicationProcess.lastApplied() );
    }

    @Test
    public void shouldIncreaseLastAppliedForOtherCommands() throws Exception
    {
        // given
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );

        // when
        applicationProcess.notifyCommitted( 2 );
        applicationProcess.start();

        // then
        assertEquals( 2, applicationProcess.lastApplied() );
    }

    private Consumer<Result> anyCallback()
    {
        @SuppressWarnings( "unchecked" )
        Consumer<Result> anyCallback = any( Consumer.class );
        return anyCallback;
    }
}
