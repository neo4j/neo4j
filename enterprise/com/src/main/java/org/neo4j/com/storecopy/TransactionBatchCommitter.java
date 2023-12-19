/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com.storecopy;

import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.Log;

import static org.neo4j.helpers.Format.duration;
import static org.neo4j.helpers.Format.time;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

class TransactionBatchCommitter implements TransactionQueue.Applier
{
    private final KernelTransactions kernelTransactions;
    private final long idReuseSafeZoneTime;
    private final TransactionCommitProcess commitProcess;
    private final Log log;

    TransactionBatchCommitter( KernelTransactions kernelTransactions, long idReuseSafeZoneTime,
            TransactionCommitProcess commitProcess, Log log )
    {
        assert log != null;

        this.kernelTransactions = kernelTransactions;
        this.idReuseSafeZoneTime = idReuseSafeZoneTime;
        this.commitProcess = commitProcess;
        this.log = log;
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
                markUnsafeTransactionsForTermination( first, last );
                commit( first );
            }
            finally
            {
                kernelTransactions.unblockNewTransactions();
            }
        }
        else
        {
            markUnsafeTransactionsForTermination( first, last );
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

    private void markUnsafeTransactionsForTermination( TransactionToApply first, TransactionToApply last )
    {
        long firstCommittedTimestamp = first.transactionRepresentation().getTimeCommitted();
        long lastCommittedTimestamp = last.transactionRepresentation().getTimeCommitted();
        long earliestSafeTimestamp = lastCommittedTimestamp - idReuseSafeZoneTime;

        for ( KernelTransactionHandle txHandle : kernelTransactions.activeTransactions() )
        {
            long commitTimestamp = txHandle.lastTransactionTimestampWhenStarted();

            if ( commitTimestamp != TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP &&
                 commitTimestamp < earliestSafeTimestamp )
            {
                if ( txHandle.markForTermination( Status.Transaction.Outdated ) )
                {
                    log.info( "Marking transaction for termination, " +
                            "invalidated due to an upcoming batch of changes being applied:" +
                            "\n" +
                            "  Batch: firstCommittedTxId:" + first.transactionId() +
                            ", firstCommittedTimestamp:" + informativeTimestamp( firstCommittedTimestamp ) +
                            ", lastCommittedTxId:" + last.transactionId() +
                            ", lastCommittedTimestamp:" + informativeTimestamp( lastCommittedTimestamp ) +
                            ", batchTimeRange:" + informativeDuration( lastCommittedTimestamp - firstCommittedTimestamp ) +
                            ", earliestSafeTimestamp:" + informativeTimestamp( earliestSafeTimestamp ) +
                            ", safeZoneDuration:" + informativeDuration( idReuseSafeZoneTime ) +
                            "\n" +
                            "  Transaction: lastCommittedTimestamp:" +
                            informativeTimestamp( txHandle.lastTransactionTimestampWhenStarted() ) +
                            ", lastCommittedTxId:" + txHandle.lastTransactionIdWhenStarted() +
                            ", localStartTimestamp:" + informativeTimestamp( txHandle.startTime() ) );
                }
            }
        }
    }

    private static String informativeDuration( long duration )
    {
        return duration( duration ) + "/" + duration;
    }

    private static String informativeTimestamp( long timestamp )
    {
        return time( timestamp ) + "/" + timestamp;
    }
}
