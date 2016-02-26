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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.UUID;

import org.junit.Test;

import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.raft.state.InMemoryStateStorage;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.core.RecoverTransactionLogState;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.logging.NullLogProvider;

public class ReplicatedTransactionStateMachinePersistenceTest
{
    @Test
    public void shouldNotRejectUncommittedTransactionsAfterCrashEvenIfSessionTrackerSaysSo() throws Exception
    {
        // given
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );
        when( commitProcess.commit( any(), any(), any() ) ).thenThrow( new TransactionFailureException( "testing" ) )
                .thenReturn( 123L );

        RecoverTransactionLogState recoverTransactionLogState = mock( RecoverTransactionLogState.class );
        when(recoverTransactionLogState.findLastCommittedIndex()).thenReturn( 99L );

        ReplicatedTransactionStateMachine<RaftTestMember> stateMachine = stateMachine( commitProcess,
                new GlobalSessionTrackerState<>(), recoverTransactionLogState );

        ReplicatedTransaction<RaftTestMember> rtx = replicatedTx();

        // when
        try
        {
            stateMachine.applyCommand( rtx, 100 );
            fail( "test design throws exception here" );
        }
        catch ( TransactionFailureException thrownByTestDesign )
        {
            // expected
        }
        reset( commitProcess ); // ignore all previous interactions, we care what happens from now on
        stateMachine.setLastCommittedIndex( 99 );
        stateMachine.applyCommand( rtx, 100 );

        // then
        verify( commitProcess, times( 1 ) ).commit( any(), any(), any() );
    }

//    @Test
//    public void shouldUpdateSessionStateOnRecoveryEvenIfTxCommittedOnFirstTry() throws Exception
//    {
//        // given
//        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );
//
//        GlobalSessionTrackerState<RaftTestMember> sessionTracker = mock( GlobalSessionTrackerState.class );
//        when( sessionTracker.validateOperation( any(), any() ) ).thenReturn( true );
//
//        Stubber stubber = doThrow( new RuntimeException() );
//        stubber.when( sessionTracker ).update( any(), any(), anyLong() );
//        stubber.doNothing().when( sessionTracker ).update( any(), any(), anyLong() );
//
//        ReplicatedTransactionStateMachine<RaftTestMember> stateMachine = stateMachine( commitProcess, sessionTracker );
//
//        ReplicatedTransaction<RaftTestMember> rtx = replicatedTx();
//
//        // when
//        // we try to commit but fail on session update, and then try to do recovery
//        try
//        {
//            // transaction gets committed at log index 99. It will reach the tx log but not the session state
//            stateMachine.applyCommand( rtx, 99 );
//            fail( "test setup should have resulted in an exception by now" );
//        }
//        catch ( RuntimeException totallyExpectedByTestSetup )
//        {
//            // dully ignored
//        }
//        // reset state so we can do proper validation below
//        reset( commitProcess );
//
//        // now let's do recovery. The log contains the last tx, so the last committed log index is the previous: 99
//        stateMachine.setLastCommittedIndex( 99 );
//
//        // however, the raft log will give us the same tx, as we did not return successfully from the last
//        // onReplicated()
//        stateMachine.applyCommand( rtx, 99 );
//
//        // then
//        // there should be no commit of tx, but an update on the session state
//        verifyZeroInteractions( commitProcess );
//        verify( sessionTracker, times( 2 ) ).update( any(), any(), eq( 99L ) );
//    }

    @Test
    public void shouldSkipUpdatingSessionStateForSameIndexAfterSuccessfulUpdate() throws Exception
    {
        // given
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );

        GlobalSessionTrackerState<RaftTestMember> sessionTrackerState = spy( new GlobalSessionTrackerState<>() );

        RecoverTransactionLogState recoverTransactionLogState = mock( RecoverTransactionLogState.class );
        when(recoverTransactionLogState.findLastCommittedIndex()).thenReturn( -1L );

        ReplicatedTransactionStateMachine<RaftTestMember> stateMachine = stateMachine( commitProcess, sessionTrackerState, recoverTransactionLogState );

        ReplicatedTransaction<RaftTestMember> rtx = replicatedTx();

        // when
        // we commit a tx normally
        final int commitAtRaftLogIndex = 99;
        stateMachine.applyCommand( rtx, commitAtRaftLogIndex );

        // simply verify that things were properly updated
        assertEquals( commitAtRaftLogIndex, sessionTrackerState.logIndex() );

        // when the same replicated content is passed in again
        stateMachine.applyCommand( rtx, commitAtRaftLogIndex );

        // then
        verify( commitProcess, times( 1 ) ).commit( any(), any(), any() );
        verify( sessionTrackerState, times( 1 ) ).update( any(), any(), eq( 99L ) );
    }

    public ReplicatedTransactionStateMachine<RaftTestMember> stateMachine( TransactionCommitProcess commitProcess,
                                                                           GlobalSessionTrackerState<RaftTestMember> sessionTrackerState, RecoverTransactionLogState recoverTransactionLogState )
    {
        return new ReplicatedTransactionStateMachine<>(
                commitProcess,
                new GlobalSession<>( UUID.randomUUID(), RaftTestMember.member( 1 ) ),
                mock( ReplicatedLockTokenStateMachine.class, RETURNS_MOCKS ),
                new CommittingTransactionsRegistry(),
                new InMemoryStateStorage<>( sessionTrackerState ),
                NullLogProvider.getInstance(),
                recoverTransactionLogState );
    }

    private ReplicatedTransaction<RaftTestMember> replicatedTx() throws java.io.IOException
    {
        TransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.emptySet() );
        return ReplicatedTransactionFactory.createImmutableReplicatedTransaction( tx, new GlobalSession<>( UUID
                .randomUUID(), RaftTestMember.member( 2 ) ), new LocalOperationId( 1, 0 ) );
    }
}
