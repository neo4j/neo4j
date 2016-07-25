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
package org.neo4j.com.storecopy;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

class TransactionBatchCommitter implements TransactionQueue.Applier
{
    private final KernelTransactions kernelTransactions;
    private final long idReuseSafeZoneTime;
    private final TransactionCommitProcess commitProcess;

    TransactionBatchCommitter( KernelTransactions kernelTransactions, long idReuseSafeZoneTime,
            TransactionCommitProcess commitProcess )
    {
        this.kernelTransactions = kernelTransactions;
        this.idReuseSafeZoneTime = idReuseSafeZoneTime;
        this.commitProcess = commitProcess;
    }

    @Override
    public void apply( TransactionToApply first, TransactionToApply last ) throws Exception
    {
        /*
          Case 1 (Not really a problem):
           - chunk of batch is smaller than safe zone
           - tx started after activeTransactions() is called
           is safe because those transactions will see the latest state of store before chunk is applied and
           because chunk is smaller than safe zone we are guarantied to not see two different states of any record
           when applying the chunk.

             activeTransactions() is called
             |        start committing chunk
          ---|----+---|--|------> TIME
                  |      |
                  |      Start applying chunk
                  New tx starts here. Does not get terminated because not among active transactions, this is safe.

          Case 2:
           - chunk of batch is larger than safe zone
           - tx started after activeTransactions() but before apply

             activeTransactions() is called
             |        start committing chunk
          ---|--------|+-|------> TIME
                       | |
                       | Start applying chunk
                       New tx starts here. Does not get terminated because not among active transactions, but will
                       read outdated data and can be affected by reuse contamination.
         */

        if ( batchSizeExceedsSafeZone( first, last ) )
        {
            // We stop new transactions from starting to avoid problem described in (2)
            kernelTransactions.blockNewTransactions();
            try
            {
                markUnsafeTransactionsForTermination( last );
                commit( first );
            }
            finally
            {
                kernelTransactions.unblockNewTransactions();
            }
        }
        else
        {
            markUnsafeTransactionsForTermination( last );
            commit( first );
        }
    }

    private long commit( TransactionToApply first ) throws TransactionFailureException
    {
        return commitProcess.commit( first, CommitEvent.NULL, EXTERNAL );
    }

    private boolean batchSizeExceedsSafeZone( TransactionToApply first, TransactionToApply last )
    {
        long lastAppliedTimestamp = last.transactionRepresentation().getTimeCommitted();
        long firstAppliedTimestamp = first.transactionRepresentation().getTimeCommitted();
        long chunkLength = lastAppliedTimestamp - firstAppliedTimestamp;

        return chunkLength > idReuseSafeZoneTime;
    }

    private void markUnsafeTransactionsForTermination( TransactionToApply last )
    {
        long lastAppliedTimestamp = last.transactionRepresentation().getTimeCommitted();
        long earliestSafeTimestamp = lastAppliedTimestamp - idReuseSafeZoneTime;

        for ( KernelTransaction tx : kernelTransactions.activeTransactions() )
        {
            long commitTimestamp = tx.lastTransactionTimestampWhenStarted();

            if ( commitTimestamp != TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP &&
                 commitTimestamp < earliestSafeTimestamp )
            {
                tx.markForTermination( Status.Transaction.Outdated );
            }
        }
    }
}
