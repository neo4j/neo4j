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

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.core.CurrentReplicatedLockState;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
        int lockSessionId = 23;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                physicalTx( lockSessionId ), globalSession, localOperationId );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        final ReplicatedTransactionStateMachine listener = new ReplicatedTransactionStateMachine(
                localCommitProcess, globalSession, lockState( lockSessionId ) );

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
        int lockSessionId = 23;

        ReplicatedTransaction tx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction(
                physicalTx( lockSessionId ), globalSession, localOperationId );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        ReplicatedTransactionStateMachine listener = new ReplicatedTransactionStateMachine(
                localCommitProcess, globalSession, lockState( lockSessionId ) );

        // when
        listener.onReplicated( tx, 0 );
        listener.onReplicated( tx, 0 );

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

        final ReplicatedTransactionStateMachine listener = new ReplicatedTransactionStateMachine(
                localCommitProcess, globalSession, lockState( currentLockSessionId ) );

        Future<Long> future = listener.getFutureTxId( localOperationId );

        // when
        listener.onReplicated( tx, 0 );

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

    public PhysicalTransactionRepresentation physicalTx( int lockSessionId )
    {
        PhysicalTransactionRepresentation physicalTx = mock( PhysicalTransactionRepresentation.class );
        when( physicalTx.getLockSessionId() ).thenReturn( lockSessionId );
        return physicalTx;
    }

    public CurrentReplicatedLockState lockState( int lockSessionId )
    {
        CurrentReplicatedLockState lockState = mock( CurrentReplicatedLockState.class );
        when(lockState.currentLockSession()).thenReturn( new StubLockSession( lockSessionId ) );
        return lockState;
    }

    private class StubLockSession implements CurrentReplicatedLockState.LockSession
    {
        private final int id;

        private StubLockSession( int id )
        {
            this.id = id;
        }

        @Override
        public int id()
        {
            return id;
        }

        @Override
        public boolean isMine()
        {
            return false;
        }
    }
}
