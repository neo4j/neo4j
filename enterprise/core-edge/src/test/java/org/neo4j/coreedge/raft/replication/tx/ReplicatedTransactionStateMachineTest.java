/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.coreedge.raft.locks.NewLeaderBarrier;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class ReplicatedTransactionStateMachineTest
{
    CoreMember coreMember = new CoreMember( address( "core:1" ), address( "raft:1" ) );
    GlobalSession globalSession = new GlobalSession( UUID.randomUUID(), coreMember );

    @Test
    public void shouldCommitTransaction() throws Exception
    {
        // given
        LocalOperationId localOperationId = new LocalOperationId( 0, 0 );

        ReplicatedTransaction tx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                mock( PhysicalTransactionRepresentation.class ), globalSession, localOperationId );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        final ReplicatedTransactionStateMachine listener = new ReplicatedTransactionStateMachine(
                localCommitProcess, globalSession, null );

        // when
        listener.onReplicated( tx, 0 );

        // then
        verify( localCommitProcess, times( 1 ) ).commit( any( TransactionToApply.class ),
                any( CommitEvent.class ), any( TransactionApplicationMode.class ) );
    }

    @Test
    public void shouldOnlyCommitSameTransactionOnce() throws Exception
    {
        // given
        LocalOperationId localOperationId = new LocalOperationId( 0, 0 );

        ReplicatedTransaction tx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                mock( PhysicalTransactionRepresentation.class ), globalSession, localOperationId );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        ReplicatedTransactionStateMachine listener = new ReplicatedTransactionStateMachine( localCommitProcess, globalSession, null );

        // when
        listener.onReplicated( tx, 0 );
        listener.onReplicated( tx, 0 );

        // then
        verify( localCommitProcess ).commit( any( TransactionToApply.class ),
                any( CommitEvent.class ), eq( TransactionApplicationMode.EXTERNAL ) );
    }

    @Test
    public void shouldRejectTransactionCommittedUnderOldLeader() throws Exception
    {
        // given
        LocalOperationId localOperationIdBefore = new LocalOperationId( 0, 0 );
        LocalOperationId localOperationIdAfter = new LocalOperationId( 0, 1 );

        long timeBefore = 1001;
        PhysicalTransactionRepresentation before = new PhysicalTransactionRepresentation( Collections.emptyList() );
        before.setHeader( null, 0, 0, timeBefore, 3, 0, 0 );
        ReplicatedTransaction txBefore = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                before, globalSession, localOperationIdBefore );

        long timeAfter = 1002;
        PhysicalTransactionRepresentation after = new PhysicalTransactionRepresentation( Collections.emptyList() );
        before.setHeader( null, 0, 0, timeAfter, 3, 0, 0 );
        ReplicatedTransaction txAfter = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                after, globalSession, localOperationIdAfter );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        final AtomicReference<TransactionRepresentation> committedTxRepresentation = new AtomicReference<>();
        when( localCommitProcess.commit( any( TransactionToApply.class ),
                any( CommitEvent.class ), any( TransactionApplicationMode.class ) ) )
                .thenAnswer( invocation -> {
                    committedTxRepresentation.set( invocation.getArgumentAt( 0,
                            TransactionToApply.class ).transactionRepresentation() );
                    return 4L;
                } );
        ReplicatedTransactionStateMachine listener = new ReplicatedTransactionStateMachine( localCommitProcess,
                globalSession, null );

        // when
        listener.onReplicated( txBefore, 0 ); // Just to get the Id
        listener.onReplicated( new NewLeaderBarrier(), 0 );
        listener.onReplicated( txAfter, 0 );

        // then
        verify( localCommitProcess ).commit( any( TransactionToApply.class ), any( CommitEvent.class ),
                eq( TransactionApplicationMode.EXTERNAL ) );
        assertEquals( timeBefore, committedTxRepresentation.get().getTimeStarted() );
        verifyNoMoreInteractions( localCommitProcess );
    }

    @Test
    public void shouldFailFutureForTransactionCommittedUnderOldLeader() throws Exception
    {
        // given
        LocalOperationId localOperationIdBefore = new LocalOperationId( 0, 0 );
        LocalOperationId localOperationIdAfter = new LocalOperationId( 0, 1 );

        ReplicatedTransaction txBefore = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                 mock( PhysicalTransactionRepresentation.class ), globalSession, localOperationIdBefore
        );
        PhysicalTransactionRepresentation after = mock( PhysicalTransactionRepresentation.class );
        when( after.getLatestCommittedTxWhenStarted() ).thenReturn( 3L );
        ReplicatedTransaction txAfter = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                after, globalSession, localOperationIdAfter
        );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        when( localCommitProcess.commit( any( TransactionToApply.class ),
                any( CommitEvent.class ), any( TransactionApplicationMode.class ) ) )
                .thenReturn( 4L );
        ReplicatedTransactionStateMachine listener = new ReplicatedTransactionStateMachine( localCommitProcess,
                globalSession, null );

        Future<Long> future = listener.getFutureTxId( localOperationIdAfter );

        // when
        listener.onReplicated( txBefore, 0 ); // Just to get the Id
        listener.onReplicated( new NewLeaderBarrier(), 0 );
        listener.onReplicated( txAfter, 0 );

        // then
        try
        {
            future.get(1, TimeUnit.SECONDS);
            fail( "Should have thrown exception" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause().getMessage(), containsString( "different leader" ) );
        }
    }
}
