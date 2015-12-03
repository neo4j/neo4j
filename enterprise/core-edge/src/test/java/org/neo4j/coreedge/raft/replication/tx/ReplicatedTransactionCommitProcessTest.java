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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.INTERNAL;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;

@SuppressWarnings("unchecked")
public class ReplicatedTransactionCommitProcessTest
{
    CoreMember coreMember = new CoreMember( address( "core:1" ), address( "raft:1" ) );

    @Test
    public void shouldReplicateOnlyOnceIfFirstAttemptSuccessful() throws Exception
    {
        // given
        Replicator replicator = mock( Replicator.class );
        ReplicatedTransactionStateMachine transactionStateMachine = mock( ReplicatedTransactionStateMachine.class );
        Future future = mock( Future.class );
        when( future.get( anyInt(), any( TimeUnit.class ) ) ).thenReturn( 23l );
        when( transactionStateMachine.getFutureTxId( any( LocalOperationId.class ) ) ).thenReturn( future );

        // when
        new ReplicatedTransactionCommitProcess( replicator, new LocalSessionPool( coreMember ),
                transactionStateMachine, Clock.SYSTEM_CLOCK, 1, 30 )
                .commit( tx(), NULL, INTERNAL );

        // then
        verify( replicator, times( 1 ) ).replicate( any( ReplicatedTransaction.class ) );
    }

    @Test
    public void shouldRetryReplicationIfFirstAttemptTimesOut() throws Exception
    {
        // given
        Replicator replicator = mock( Replicator.class );
        ReplicatedTransactionStateMachine transactionStateMachine = mock( ReplicatedTransactionStateMachine.class );
        Future future = mock( Future.class );
        when( transactionStateMachine.getFutureTxId( any( LocalOperationId.class ) ) ).thenReturn( future );
        when( future.get( anyInt(), any( TimeUnit.class ) ) ).thenThrow( TimeoutException.class ).thenReturn( 23l );

        // when
        new ReplicatedTransactionCommitProcess( replicator, new LocalSessionPool( coreMember ),
                transactionStateMachine, Clock.SYSTEM_CLOCK, 1, 30 )
                .commit( tx(), NULL, INTERNAL );

        // then
        verify( replicator, times( 2 ) ).replicate( any( ReplicatedTransaction.class ) );
    }

    private TransactionToApply tx()
    {
        TransactionRepresentation tx = mock( TransactionRepresentation.class );
        when( tx.additionalHeader() ).thenReturn( new byte[]{} );
        return new TransactionToApply( tx );
    }
}
