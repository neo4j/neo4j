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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;

public class CommittingTransactionsRegistry implements CommittingTransactions
{
    private final Map<LocalOperationId, CommittingTransactionFuture> outstanding = new ConcurrentHashMap<>();

    @Override
    public CommittingTransaction register( LocalOperationId localOperationId )
    {
        final CommittingTransactionFuture future = new CommittingTransactionFuture( localOperationId );
        outstanding.put( localOperationId, future );
        return future;
    }

    @Override
    public CommittingTransaction retrieve( LocalOperationId localOperationId )
    {
        return outstanding.remove( localOperationId );
    }

    public class CommittingTransactionFuture implements CommittingTransaction
    {
        private final LocalOperationId localOperationId;
        private final CompletableFuture<Long> future = new CompletableFuture<>();

        CommittingTransactionFuture( LocalOperationId localOperationId )
        {
            this.localOperationId = localOperationId;
        }

        @Override
        public void close()
        {
            outstanding.remove( localOperationId );
        }

        @Override
        public long waitUntilCommitted( long timeout, TimeUnit unit )
                throws TransactionFailureException, TimeoutException, InterruptedException
        {
            try
            {
                return future.get( timeout, unit );
            }
            catch ( ExecutionException e )
            {
                throw (TransactionFailureException) e.getCause();
            }
        }

        @Override
        public void notifySuccessfullyCommitted( long txId )
        {
            future.complete( txId );
        }

        @Override
        public void notifyCommitFailed( TransactionFailureException e )
        {
            future.completeExceptionally( e );
        }
    }

}
