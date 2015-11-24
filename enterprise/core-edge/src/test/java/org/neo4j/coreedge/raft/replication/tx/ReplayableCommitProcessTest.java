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

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.kernel.impl.api.TransactionApplicationMode.EXTERNAL;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;

public class ReplayableCommitProcessTest
{
    @Test
    public void shouldCommitTransactions() throws Exception
    {
        // given
        TransactionRepresentation newTx1 = mock( TransactionRepresentation.class );
        TransactionRepresentation newTx2 = mock( TransactionRepresentation.class );
        TransactionRepresentation newTx3 = mock( TransactionRepresentation.class );

        StubLocalDatabase localDatabase = new StubLocalDatabase( 1 );
        final ReplayableCommitProcess txListener = new ReplayableCommitProcess(
                localDatabase, localDatabase );

        // when
        txListener.commit( newTx1, new LockGroup(), NULL, EXTERNAL );
        txListener.commit( newTx2, new LockGroup(), NULL, EXTERNAL );
        txListener.commit( newTx3, new LockGroup(), NULL, EXTERNAL );

        // then
        verify( localDatabase.commitProcess, times( 3 ) ).commit( any( TransactionRepresentation.class ),
                any( LockGroup.class ), any( CommitEvent.class ), any( TransactionApplicationMode.class ) );
    }

    @Test
    public void shouldNotCommitTransactionsThatAreAlreadyCommittedLocally() throws Exception
    {
        // given
        TransactionRepresentation alreadyCommittedTx1 = mock( TransactionRepresentation.class );
        TransactionRepresentation alreadyCommittedTx2 = mock( TransactionRepresentation.class );
        TransactionRepresentation newTx = mock( TransactionRepresentation.class );

        StubLocalDatabase localDatabase = new StubLocalDatabase( 3 );
        final ReplayableCommitProcess txListener = new ReplayableCommitProcess(
                localDatabase, localDatabase );

        // when
        txListener.commit( alreadyCommittedTx1, new LockGroup(), NULL, EXTERNAL );
        txListener.commit( alreadyCommittedTx2, new LockGroup(), NULL, EXTERNAL );
        txListener.commit( newTx, new LockGroup(), NULL, EXTERNAL );

        // then
        verify( localDatabase.commitProcess, times( 1 ) ).commit( eq( newTx ), any( LockGroup.class ),
                any( CommitEvent.class ), any( TransactionApplicationMode.class ) );
        verifyNoMoreInteractions( localDatabase.commitProcess );
    }

    private static class StubLocalDatabase implements TransactionCounter, TransactionCommitProcess
    {
        long lastCommittedTransactionId;
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );

        public StubLocalDatabase( long lastCommittedTransactionId )
        {
            this.lastCommittedTransactionId = lastCommittedTransactionId;
        }

        @Override
        public long lastCommittedTransactionId()
        {
            return lastCommittedTransactionId;
        }

        @Override
        public long commit( TransactionRepresentation representation, LockGroup locks, CommitEvent commitEvent,
                            TransactionApplicationMode mode ) throws TransactionFailureException
        {
            lastCommittedTransactionId++;
            return commitProcess.commit( representation, locks, commitEvent, mode );
        }
    }
}
