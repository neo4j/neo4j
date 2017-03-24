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
package org.neo4j.causalclustering.core.state.machines.tx;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicatedTransactionStateMachineTest
{
    private final NullLogProvider logProvider = NullLogProvider.getInstance();
    private final int batchSize = 16;

    @Test
    public void shouldCommitTransaction() throws Exception
    {
        // given
        int lockSessionId = 23;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.
                createImmutableReplicatedTransaction( physicalTx( lockSessionId ) );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        ReplicatedTransactionStateMachine stateMachine =
                new ReplicatedTransactionStateMachine( lockState( lockSessionId ), batchSize, logProvider );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        // when
        stateMachine.applyCommand( tx, 0, r -> {} );
        stateMachine.ensuredApplied();

        // then
        verify( localCommitProcess, times( 1 ) ).commit( any( TransactionToApply.class ), any( CommitEvent.class ),
                any( TransactionApplicationMode.class ) );
    }

    @Test
    public void shouldFailFutureForTransactionCommittedUnderWrongLockSession() throws Exception
    {
        // given
        int txLockSessionId = 23;
        int currentLockSessionId = 24;

        ReplicatedTransaction tx =
                ReplicatedTransactionFactory.createImmutableReplicatedTransaction( physicalTx( txLockSessionId ) );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        final ReplicatedTransactionStateMachine stateMachine =
                new ReplicatedTransactionStateMachine( lockState( currentLockSessionId ), batchSize, logProvider );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        AtomicBoolean called = new AtomicBoolean();
        // when
        stateMachine.applyCommand( tx, 0, result ->
        {
            // then
            called.set( true );
            try
            {
                result.consume();
                fail( "should have thrown" );
            }
            catch ( TransactionFailureException tfe )
            {
                assertEquals( Status.Transaction.LockSessionExpired, tfe.status() );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        } );
        stateMachine.ensuredApplied();

        assertTrue( called.get() );
    }

    @Test
    public void shouldAcceptTransactionCommittedWithNoLockManager() throws Exception
    {
        // given
        int txLockSessionId = Locks.Client.NO_LOCK_SESSION_ID;
        int currentLockSessionId = 24;
        long txId = 42L;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.
                createImmutableReplicatedTransaction( physicalTx( txLockSessionId ) );

        TransactionCommitProcess localCommitProcess = createFakeTransactionCommitProcess( txId );

        ReplicatedTransactionStateMachine stateMachine =
                new ReplicatedTransactionStateMachine( lockState( currentLockSessionId ), batchSize, logProvider );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        AtomicBoolean called = new AtomicBoolean();

        // when
        stateMachine.applyCommand( tx, 0, result ->
        {
            // then
            called.set( true );
            try
            {
                assertEquals( txId, (long) result.consume() );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        } );
        stateMachine.ensuredApplied();

        assertTrue( called.get() );
    }

    private TransactionCommitProcess createFakeTransactionCommitProcess( long txId ) throws TransactionFailureException
    {
        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        when( localCommitProcess.commit(
                any( TransactionToApply.class), any( CommitEvent.class ), any( TransactionApplicationMode.class ) )
        ).thenAnswer( invocation ->
        {
            TransactionToApply txToApply = (TransactionToApply) invocation.getArguments()[0];
            txToApply.commitment( new FakeCommitment( txId, mock( TransactionIdStore.class ) ), txId );
            txToApply.commitment().publishAsCommitted();
            txToApply.commitment().publishAsClosed();
            txToApply.close();
            return txId;
        } );
        return localCommitProcess;
    }

    private PhysicalTransactionRepresentation physicalTx( int lockSessionId )
    {
        PhysicalTransactionRepresentation physicalTx = mock( PhysicalTransactionRepresentation.class );
        when( physicalTx.getLockSessionId() ).thenReturn( lockSessionId );
        return physicalTx;
    }

    private  ReplicatedLockTokenStateMachine lockState( int lockSessionId )
    {
        @SuppressWarnings( "unchecked" )
        ReplicatedLockTokenStateMachine lockState = mock( ReplicatedLockTokenStateMachine.class );
        when( lockState.currentToken() ).thenReturn( new ReplicatedLockTokenRequest( null, lockSessionId ) );
        return lockState;
    }
}
