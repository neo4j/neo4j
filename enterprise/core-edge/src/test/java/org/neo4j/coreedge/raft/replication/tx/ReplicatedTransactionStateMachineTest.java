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

import org.junit.Test;

import java.util.Optional;

import org.neo4j.coreedge.raft.RaftStateMachine;
import org.neo4j.coreedge.raft.state.Result;
import org.neo4j.coreedge.server.RaftTestMember;
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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicatedTransactionStateMachineTest
{
    @Test
    public void shouldCommitTransaction() throws Exception
    {
        // given
        int lockSessionId = 23;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.
                createImmutableReplicatedTransaction( physicalTx( lockSessionId ) );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        ReplicatedTransactionStateMachine stateMachine = new ReplicatedTransactionStateMachine<>(
                lockState( lockSessionId ), NullLogProvider.getInstance() );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        // when
        stateMachine.applyCommand( tx, 0 );

        // then
        verify( localCommitProcess, times( 1 ) ).commit( any( TransactionToApply.class ),
                any( CommitEvent.class ), any( TransactionApplicationMode.class ) );
    }

    @Test
    public void shouldFailFutureForTransactionCommittedUnderWrongLockSession() throws Exception
    {
        // given
        int txLockSessionId = 23;
        int currentLockSessionId = 24;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                physicalTx( txLockSessionId ) );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        final ReplicatedTransactionStateMachine<RaftTestMember> stateMachine = new ReplicatedTransactionStateMachine<>(
                lockState( currentLockSessionId ), NullLogProvider.getInstance() );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        // when
        Optional<Result> result = stateMachine.applyCommand( tx, 0 );

        // then
        try
        {
            assertTrue( result.isPresent() );
            result.get().consume();
        }
        catch( TransactionFailureException tfe )
        {
            assertEquals( Status.Transaction.LockSessionExpired, tfe.status() );
        }
    }

    @Test
    public void shouldAcceptTransactionCommittedWithNoLockManager() throws Exception
    {
        // given
        int txLockSessionId = Locks.Client.NO_LOCK_SESSION_ID;
        int currentLockSessionId = 24;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.
                createImmutableReplicatedTransaction( physicalTx( txLockSessionId ) );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        ReplicatedTransactionStateMachine<RaftStateMachine> stateMachine = new ReplicatedTransactionStateMachine<>(
                lockState( currentLockSessionId ), NullLogProvider.getInstance() );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        // when
        Optional<Result> result = stateMachine.applyCommand( tx, 0 );

        // then
        assertTrue( result.isPresent() );
        assertEquals( 0L, (long) result.get().consume() );
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
