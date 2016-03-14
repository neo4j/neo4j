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
package org.neo4j.coreedge.raft.replication.tx;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.raft.state.InMemoryStateStorage;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.core.RecoverTransactionLogState;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenRequest;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicatedTransactionStateMachineTest
{
    CoreMember coreMember = new CoreMember( new AdvertisedSocketAddress( "core:1" ),
            new AdvertisedSocketAddress( "raft:1" ) );
    GlobalSession globalSession = new GlobalSession<>( UUID.randomUUID(), coreMember );

    @Test
    public void shouldCommitTransaction() throws Exception
    {
        // given
        LocalOperationId localOperationId = new LocalOperationId( 0, 0 );
        int lockSessionId = 23;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                physicalTx( lockSessionId ), globalSession, localOperationId );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        RecoverTransactionLogState recoverTransactionLogState = mock( RecoverTransactionLogState.class );
        when(recoverTransactionLogState.findLastAppliedIndex()).thenReturn( -1L );

        final ReplicatedTransactionStateMachine listener = new ReplicatedTransactionStateMachine<>(
                localCommitProcess, globalSession, lockState( lockSessionId ), new CommittingTransactionsRegistry(),
                new InMemoryStateStorage<>( new GlobalSessionTrackerState<>() ), NullLogProvider.getInstance(),
                recoverTransactionLogState);

        // when
        listener.applyCommand( tx, 0 );

        // then
        verify( localCommitProcess, times( 1 ) ).commit( any( TransactionToApply.class ),
                any( CommitEvent.class ), any( TransactionApplicationMode.class ) );
    }

    @Test
    public void shouldOnlyCommitSameTransactionOnce() throws Exception
    {
        // given
        LocalOperationId localOperationId = new LocalOperationId( 0, 0 );
        int lockSessionId = 23;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                physicalTx( lockSessionId ), globalSession, localOperationId );

        RecoverTransactionLogState recoverTransactionLogState = mock( RecoverTransactionLogState.class );
        when(recoverTransactionLogState.findLastAppliedIndex()).thenReturn( -1L );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        ReplicatedTransactionStateMachine<CoreMember> listener = new ReplicatedTransactionStateMachine<>(
                localCommitProcess, globalSession, lockState( lockSessionId ), new CommittingTransactionsRegistry(),
                new InMemoryStateStorage<>( new GlobalSessionTrackerState<>() ), NullLogProvider.getInstance(),
                recoverTransactionLogState);

        // when
        listener.applyCommand( tx, 0 );
        listener.applyCommand( tx, 0 );

        // then
        verify( localCommitProcess ).commit( any( TransactionToApply.class ),
                any( CommitEvent.class ), eq( TransactionApplicationMode.EXTERNAL ) );
    }

    @Test
    public void shouldFailFutureForTransactionCommittedUnderWrongLockSession() throws Exception
    {
        // given
        LocalOperationId localOperationId = new LocalOperationId( 0, 0 );
        int txLockSessionId = 23;
        int currentLockSessionId = 24;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                physicalTx( txLockSessionId ), globalSession, localOperationId );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        RecoverTransactionLogState recoverTransactionLogState = mock( RecoverTransactionLogState.class );
        when(recoverTransactionLogState.findLastAppliedIndex()).thenReturn( -1L );

        CommittingTransactions committingTransactions = new CommittingTransactionsRegistry();
        final ReplicatedTransactionStateMachine listener = new ReplicatedTransactionStateMachine<>(
                localCommitProcess, globalSession, lockState( currentLockSessionId ), committingTransactions,
                new InMemoryStateStorage<>( new GlobalSessionTrackerState<>() ), NullLogProvider.getInstance(),
                recoverTransactionLogState);

        CommittingTransaction future = committingTransactions.register( localOperationId );

        // when
        listener.applyCommand( tx, 0 );

        // then
        try
        {
            future.waitUntilCommitted( 1, TimeUnit.SECONDS );
            fail( "Should have thrown exception" );
        }
        catch ( TransactionFailureException e )
        {
            assertEquals( Status.Transaction.LockSessionExpired, e.status() );
        }
    }

    @Test
    public void shouldAcceptTransactionCommittedWithNoLockManager() throws Exception
    {
        // given
        LocalOperationId localOperationId = new LocalOperationId( 0, 0 );
        int txLockSessionId = Locks.Client.NO_LOCK_SESSION_ID;
        int currentLockSessionId = 24;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                physicalTx( txLockSessionId ), globalSession, localOperationId );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        RecoverTransactionLogState recoverTransactionLogState = mock( RecoverTransactionLogState.class );
        when(recoverTransactionLogState.findLastAppliedIndex()).thenReturn( -1L );

        CommittingTransactions committingTransactions = new CommittingTransactionsRegistry();
        final ReplicatedTransactionStateMachine listener = new ReplicatedTransactionStateMachine<>(
                localCommitProcess, globalSession, lockState( currentLockSessionId ), committingTransactions,
                new InMemoryStateStorage<>( new GlobalSessionTrackerState<>() ), NullLogProvider.getInstance(),
                recoverTransactionLogState);

        CommittingTransaction future = committingTransactions.register( localOperationId );

        // when
        listener.applyCommand( tx, 0 );

        // then
        future.waitUntilCommitted( 1, TimeUnit.SECONDS );
    }

    public PhysicalTransactionRepresentation physicalTx( int lockSessionId )
    {
        PhysicalTransactionRepresentation physicalTx = mock( PhysicalTransactionRepresentation.class );
        when( physicalTx.getLockSessionId() ).thenReturn( lockSessionId );
        return physicalTx;
    }

    public <MEMBER> ReplicatedLockTokenStateMachine<MEMBER> lockState( int lockSessionId )
    {
        ReplicatedLockTokenStateMachine<MEMBER> lockState = mock( ReplicatedLockTokenStateMachine.class );
        when( lockState.currentToken() ).thenReturn( new ReplicatedLockTokenRequest<>( null, lockSessionId ) );
        return lockState;
    }
}
