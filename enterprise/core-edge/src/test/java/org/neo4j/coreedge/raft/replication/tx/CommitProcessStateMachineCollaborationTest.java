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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.core.locks.CurrentReplicatedLockState;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

public class CommitProcessStateMachineCollaborationTest
{
    @Test
    public void shouldAlwaysCompleteFutureEvenIfReplicationHappensAtUnfortunateMoment() throws Exception
    {
        // given
        final int numberOfTimesToTimeout = 1;
        AtomicInteger timeoutCounter = new AtomicInteger( numberOfTimesToTimeout );

        CoreMember coreMember = new CoreMember( new AdvertisedSocketAddress( "core:1" ),
                new AdvertisedSocketAddress( "raft:1" ) );

        TriggeredReplicator replicator = new TriggeredReplicator();
        StubCommittingTransactionsRegistry txFutures = new StubCommittingTransactionsRegistry( replicator, timeoutCounter );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        LocalSessionPool sessionPool = new LocalSessionPool( coreMember );
        CurrentReplicatedLockState lockState = lockState( 0 );
        final ReplicatedTransactionStateMachine stateMachine = new ReplicatedTransactionStateMachine(
                localCommitProcess, sessionPool.getGlobalSession(), lockState, txFutures );


        ReplicatedTransactionCommitProcess commitProcess = new ReplicatedTransactionCommitProcess(
                replicator, sessionPool, stateMachine, 100,
                NullLogService.getInstance(), txFutures );

        // when
        commitProcess.commit( tx(), NULL, INTERNAL );

        // then
        assertEquals(2, replicator.timesReplicated());
    }

    @Test
    public void shouldFailTransactionIfLockSessionChanges() throws Exception
    {
        // given
        CoreMember coreMember = new CoreMember( new AdvertisedSocketAddress( "core:1" ),
                new AdvertisedSocketAddress( "raft:1" ) );

        TriggeredReplicator replicator = new TriggeredReplicator();
        CommittingTransactions txFutures = new StubCommittingTransactionsRegistry( replicator );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        LocalSessionPool sessionPool = new LocalSessionPool( coreMember );
        CurrentReplicatedLockState lockState = lockState( 1 );

        final ReplicatedTransactionStateMachine stateMachine = new ReplicatedTransactionStateMachine(
                localCommitProcess, sessionPool.getGlobalSession(), lockState, txFutures );

        ReplicatedTransactionCommitProcess commitProcess = new ReplicatedTransactionCommitProcess(
                replicator, sessionPool, stateMachine, 1,
                NullLogService.getInstance(), txFutures );

        // when
        try
        {
            commitProcess.commit( tx(), NULL, INTERNAL );
            fail( "Should have thrown exception.");
        }
        catch( TransactionFailureException e )
        {
            // expected
        }
    }

    public CurrentReplicatedLockState lockState( int lockSessionId )
    {
        CurrentReplicatedLockState lockState = mock( CurrentReplicatedLockState.class );
        when( lockState.currentLockSession() ).thenReturn( new StubLockSession( lockSessionId ) );
        return lockState;
    }

    private class TriggeredReplicator implements Replicator
    {
        private ReplicatedContentListener listener;
        private int timesReplicated = 0;
        private ReplicatedContent content;

        @Override
        public void replicate( ReplicatedContent content ) throws ReplicationFailedException
        {
            this.content = content;
            timesReplicated++;
        }

        @Override
        public void subscribe( ReplicatedContentListener listener )
        {
            this.listener = listener;
        }

        @Override
        public void unsubscribe( ReplicatedContentListener listener )
        {
            throw new UnsupportedOperationException();
        }

        public void triggerReplication()
        {
            listener.onReplicated( content, 1 );
        }

        public int timesReplicated()
        {
            return timesReplicated;
        }
    }

    private TransactionToApply tx()
    {
        TransactionRepresentation tx = mock( TransactionRepresentation.class );
        when( tx.additionalHeader() ).thenReturn( new byte[]{} );
        return new TransactionToApply( tx );
    }

    private class StubCommittingTransactionsRegistry implements CommittingTransactions
    {
        CommittingTransactions registry = new CommittingTransactionsRegistry();

        private final TriggeredReplicator replicator;
        private final AtomicInteger timeoutCounter;

        public StubCommittingTransactionsRegistry( TriggeredReplicator replicator )
        {
            this( replicator, new AtomicInteger( 0 ) );
        }

        public StubCommittingTransactionsRegistry( TriggeredReplicator replicator, AtomicInteger timeoutCounter )
        {
            this.replicator = replicator;
            this.timeoutCounter = timeoutCounter;
        }

        @Override
        public CommittingTransaction register( LocalOperationId localOperationId )
        {
            return new FutureTxId( registry.register( localOperationId ) );
        }

        @Override
        public CommittingTransaction retrieve( LocalOperationId localOperationId )
        {
            return registry.retrieve( localOperationId );
        }

        class FutureTxId implements CommittingTransaction
        {
            private final CommittingTransaction delegate;

            public FutureTxId( CommittingTransaction delegate )
            {
                this.delegate = delegate;
            }

            @Override
            public long waitUntilCommitted( long timeout, TimeUnit unit )
                    throws InterruptedException, TimeoutException, TransactionFailureException
            {
                replicator.triggerReplication();
                if ( timeoutCounter.getAndDecrement() > 0 )
                {
                    throw new TimeoutException();
                }
                else
                {
                    return delegate.waitUntilCommitted( timeout, unit );
                }
            }

            @Override
            public void notifySuccessfullyCommitted( long txId )
            {
                delegate.notifySuccessfullyCommitted( txId );
            }

            @Override
            public void notifyCommitFailed( TransactionFailureException e )
            {
                delegate.notifyCommitFailed( e );
            }

            @Override
            public void close()
            {
                delegate.close();
            }
        }
    }
}
