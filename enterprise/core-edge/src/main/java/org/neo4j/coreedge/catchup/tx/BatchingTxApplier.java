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
package org.neo4j.coreedge.catchup.tx;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Supplier;

import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * Accepts transactions and queues them up for being applied in batches.
 */
public class BatchingTxApplier extends LifecycleAdapter implements Runnable
{
    private final int maxBatchSize;
    private final Supplier<TransactionIdStore> txIdStoreSupplier;
    private final Supplier<TransactionCommitProcess> commitProcessSupplier;
    private final Supplier<DatabaseHealth> healthSupplier;

    private final PullRequestMonitor monitor;
    private final Log log;

    private final ArrayBlockingQueue<CommittedTransactionRepresentation> txQueue;

    private TransactionQueue txBatcher;

    private volatile long lastQueuedTxId;
    private volatile long lastAppliedTxId;

    public BatchingTxApplier( int maxBatchSize, Supplier<TransactionIdStore> txIdStoreSupplier, Supplier<TransactionCommitProcess> commitProcessSupplier,
            Supplier<DatabaseHealth> healthSupplier, Monitors monitors, LogProvider logProvider )
    {
        this.maxBatchSize = maxBatchSize;
        this.txIdStoreSupplier = txIdStoreSupplier;
        this.commitProcessSupplier = commitProcessSupplier;
        this.healthSupplier = healthSupplier;

        this.log = logProvider.getLog( getClass() );
        this.monitor = monitors.newMonitor( PullRequestMonitor.class );

        this.txQueue = new ArrayBlockingQueue<>( maxBatchSize );
    }

    @Override
    public void start() throws Throwable
    {
        TransactionCommitProcess commitProcess = commitProcessSupplier.get();
        txBatcher = new TransactionQueue( maxBatchSize, ( first, last ) -> commitProcess.commit( first, NULL, EXTERNAL ) );
        lastQueuedTxId = lastAppliedTxId = txIdStoreSupplier.get().getLastCommittedTransactionId();
    }

    /**
     * Queues a transaction for application.
     *
     * @param tx The transaction to be queued for application.
     */
    public void queue( CommittedTransactionRepresentation tx )
    {
        long receivedTxId = tx.getCommitEntry().getTxId();
        long expectedTxId = lastQueuedTxId + 1;

        if ( receivedTxId != expectedTxId )
        {
            log.warn( "Out of order transaction. Received: " + receivedTxId + " Expected: " + expectedTxId );
            return;
        }

        try
        {
            txQueue.put( tx );
            lastQueuedTxId = receivedTxId;
            monitor.txPullResponse( receivedTxId );
        }
        catch ( InterruptedException e )
        {
            log.warn( "Not expecting to be interrupted", e );
        }
    }

    @Override
    public void run()
    {
        CommittedTransactionRepresentation tx = null;
        try
        {
            tx = txQueue.poll( 1, SECONDS );
        }
        catch ( InterruptedException e )
        {
            log.warn( "Not expecting to be interrupted", e );
        }

        if ( tx != null )
        {
            long txId;
            try
            {
                do
                {
                    txId = tx.getCommitEntry().getTxId();
                    txBatcher.queue( new TransactionToApply( tx.getTransactionRepresentation(), txId ) );
                }
                while ( (tx = txQueue.poll()) != null );

                txBatcher.empty();
                lastAppliedTxId = txId;
            }
            catch ( Exception e )
            {
                log.error( "Error during transaction application", e );
                healthSupplier.get().panic( e );
            }
        }
    }

    /**
     * @return True if there is pending work, and false otherwise.
     */
    boolean workPending()
    {
        return lastQueuedTxId > lastAppliedTxId;
    }

    /**
     * @return The id of the last transaction applied.
     */
    long lastAppliedTxId()
    {
        return lastAppliedTxId;
    }
}
