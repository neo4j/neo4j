/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com.storecopy;

import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.StorageEngine;

/**
 * Receives and unpacks {@link Response responses}.
 * Transaction obligations are handled by {@link TransactionObligationFulfiller} and
 * {@link TransactionStream transaction streams} are {@link TransactionCommitProcess committed to the
 * store}, in batches.
 * <p>
 * It is assumed that any {@link TransactionStreamResponse response carrying transaction data} comes from the one
 * and same thread.
 * <p>
 * SAFE ZONE EXPLAINED
 * <p>
 * PROBLEM
 * A slave can read inconsistent or corrupted data (mixed state records) because of reuse of property ids.
 * This happens when a record that has been read gets reused and then read again or possibly reused in
 * middle of reading a property chain or dynamic record chain.
 * This is guarded for in single instance with the delaying of id reuse. This does not cover the Slave
 * case because the transactions that SET, REMOVE and REUSE the record are applied in batch and thus a
 * slave transaction can see states from all of the transactions that touches the reused record during its
 * lifetime.
 * <p>
 * SOLUTION
 * Master and Slave are configured with the same safeZone time.
 * Let S = safeZone time (more about safeZone time further down)
 * <p>
 * -> Master promise to hold all deleted ids in quarantine before reusing them, (S duration).
 * He thereby creates a safe zone of transactions that among themselves are guaranteed to be free of
 * id reuse contamination.
 * -> Slave promise to not let any transactions cross the safe zone boundary.
 * Meaning all transactions that falls out of the safe zone, as updates gets applied,
 * will need to be terminated, with a hint that they can simply be restarted
 * <p>
 * Safe zone is a time frame in Masters domain. All transactions that started and finished within this
 * time frame are guarantied to not have read any mixed state records.
 * <p>
 * Example of a transaction running on slave that starts reading a dynamic property, then a batch is pulled from master
 * that deletes the property and and reuses the record in the chain, making the transaction read inconsistent data.
 * <p>
 * TX starts reading
 * tx here
 * <pre>
 * v
 * |aaaa|->|aaaa|->|aaaa|->|aaaa|
 * 1       2       3       4
 * </pre>
 * "a" string is deleted and replaced with "bbbbbbbbbbbbbbbb"
 * <pre>
 * tx here
 * v
 * |bbbb|->|bbbb|->|bbbb|->|bbbb|
 * 1       2       3       4
 * </pre>
 * TX continues reading and does not know anything is wrong,
 * returning the inconsistent string "aaaaaaaabbbbbbbb".
 * <pre>
 * tx here
 * v
 * |bbbb|->|bbbb|->|bbbb|->|bbbb|
 * 1       2       3       4
 * </pre>
 * Example of how the safe zone window moves while appying a batch
 * <pre>
 * x---------------------------------------------------------------------------------->| TIME
 * |MASTER STATE
 * |---------------------------------------------------------------------------------->|
 * |                                                          Batch to apply to slave
 * |                                  safeZone with size S  |<------------------------>|
 * |                                                  |
 * |                                                  v     A
 * |SLAVE STATE 1 (before applying batch)         |<---S--->|
 * |----------------------------------------------+-------->|
 * |                                                        |
 * |                                                        |
 * |                                                        |      B
 * |SLAVE STATE 2 (mid apply)                            |<-+-S--->|
 * |-----------------------------------------------------+--+----->|
 * |                                                        |      |
 * |                                                        |      |
 * |                                                        |      |  C
 * |SLAVE STATE 3 (mid apply / after apply)                 |<---S-+->|
 * |--------------------------------------------------------+------+->|
 * |                                                        |      |  |
 * |                                                        |      |  |
 * |                                                        |      |  |                D
 * |SLAVE STATE 4 (after apply)                             |      |  |      |<---S--->|
 * |--------------------------------------------------------+------+--+------+-------->|
 * </pre>
 * <p>
 * What we see in this diagram is a slave pulling updates from the master.
 * While doing so, the safe zone window |<---S--->| is pushed forward. NOTE that we do not see any explicit transaction
 * running on slave. Only the times (A, B, C, D) that we discuss.
 * <p>
 * slaveTx start on slave when slave is in SLAVE STATE 1
 * - Latest applied transaction on slave has timestamp A and safe zone is A-S.
 * - slaveTx.startTime = A
 * <p>
 * Scenario 1 - slaveTx finish when slave is in SLAVE STATE 2
 * Latest applied transaction in store has timestamp B and safe zone is B-S.
 * slaveTx did not cross the safe zone boundary as slaveTx.startTime = A > B-S
 * We can safely assume that slaveTx did not read any mixed state records.
 * <p>
 * Scenario 2 - slaveTx has not yet finished in SLAVE STATE 3
 * Latest applied transaction in store has timestamp C and safe zone is C-S.
 * We are just about to apply the next part of the batch and push the safe zone window forward.
 * This will make slaveTx.startTime = A < C-S. This means Tx is now in risk of reading mixed state records.
 * We will terminate slaveTx and let the user try again.
 * <p>
 * <b>NOTE ABOUT TX_COMMIT_TIMESTAMP</b>
 * commitTimestamp is used by {@link MetaDataStore} to keep track of the commit timestamp of the last committed
 * transaction. When starting up a db we can not always know what the the latest commit timestamp is but slave need it
 * to know when a transaction needs to be terminated during batch application.
 * The latest commit timestamp is an important part of "safeZone" that is explained in
 * TransactionCommittingResponseUnpacker.
 * <p>
 * Here are the different scenarios, what timestamp that is used and what it means for execution.
 * <p>
 * Empty store <br>
 * TIMESTAMP: {@link TransactionIdStore#BASE_TX_COMMIT_TIMESTAMP} <br>
 * ==> FINE. NO KILL because no previous state can have been observed anyway <br>
 * <p>
 * Upgraded store w/ tx logs <br>
 * TIMESTAMP CARRIED OVER FROM LOG <br>
 * ==> FINE <br>
 * <p>
 * Upgraded store w/o tx logs <br>
 * TIMESTAMP {@link TransactionIdStore#UNKNOWN_TX_COMMIT_TIMESTAMP} (1) <br>
 * ==> SLAVE TRANSACTIONS WILL TERMINATE WHEN FIRST PULL UPDATES HAPPENS <br>
 * <p>
 * Store on 2.3.prev, w/ tx logs (no upgrade) <br>
 * TIMESTAMP CARRIED OVER FROM LOG <br>
 * ==> FINE <br>
 * <p>
 * Store on 2.3.prev w/o tx logs (no upgrade) <br>
 * TIMESTAMP {@link TransactionIdStore#UNKNOWN_TX_COMMIT_TIMESTAMP} (1) <br>
 * ==> SLAVE TRANSACTIONS WILL TERMINATE WHEN FIRST PULL UPDATES HAPPENS <br>
 * <p>
 * Store already on 2.3.next, w/ or w/o tx logs <br>
 * TIMESTAMP CORRECT <br>
 * ==> FINE
 *
 * @see TransactionBatchCommitter
 */
public class TransactionCommittingResponseUnpacker extends LifecycleAdapter implements ResponseUnpacker
{
    /**
     * Dependencies that this {@link TransactionCommittingResponseUnpacker} has. These are called upon
     * in {@link TransactionCommittingResponseUnpacker#start()}.
     */
    public interface Dependencies
    {
        /**
         * Responsible for committing batches of transactions received from transaction stream responses.
         */
        TransactionCommitProcess commitProcess();

        /**
         * Responsible for fulfilling transaction obligations received from transaction obligation responses.
         */
        TransactionObligationFulfiller obligationFulfiller();

        /**
         * Log provider
         */
        LogService logService();

        KernelTransactions kernelTransactions();

        /**
         * Version context supplier
         */
        VersionContextSupplier versionContextSupplier();
    }

    /**
     * Common implementation which pulls out dependencies from a {@link DependencyResolver} and constructs
     * whatever components it needs from that.
     */
    private static class ResolvableDependencies implements Dependencies
    {
        private final DependencyResolver resolver;

        ResolvableDependencies( DependencyResolver resolver )
        {
            this.resolver = resolver;
        }

        @Override
        public TransactionCommitProcess commitProcess()
        {
            // We simply can't resolve the commit process here, since the commit process of a slave
            // is one that sends transactions to the master. We here, however would like to actually
            // commit transactions in this db.
            return new TransactionRepresentationCommitProcess(
                    resolver.resolveDependency( TransactionAppender.class ),
                    resolver.resolveDependency( StorageEngine.class ) );
        }

        @Override
        public TransactionObligationFulfiller obligationFulfiller()
        {
            try
            {
                return resolver.resolveDependency( TransactionObligationFulfiller.class );
            }
            catch ( UnsatisfiedDependencyException e )
            {
                return toTxId ->
                {
                    throw new UnsupportedOperationException( "Should not be called" );
                };
            }
        }

        @Override
        public LogService logService()
        {
            return resolver.resolveDependency( LogService.class );
        }

        @Override
        public KernelTransactions kernelTransactions()
        {
            return resolver.resolveDependency( KernelTransactions.class );
        }

        @Override
        public VersionContextSupplier versionContextSupplier()
        {
            return resolver.resolveDependency( VersionContextSupplier.class );
        }
    }

    public static final int DEFAULT_BATCH_SIZE = 100;

    // Assigned in constructor
    private final Dependencies dependencies;
    private final int maxBatchSize;
    private final long idReuseSafeZoneTime;

    // Assigned in start()
    private TransactionObligationFulfiller obligationFulfiller;
    private TransactionBatchCommitter batchCommitter;
    private VersionContextSupplier versionContextSupplier;
    private Log log;
    // Assigned in stop()
    private volatile boolean stopped;

    public TransactionCommittingResponseUnpacker( DependencyResolver dependencies, int maxBatchSize,
            long idReuseSafeZoneTime )
    {
        this( new ResolvableDependencies( dependencies ), maxBatchSize, idReuseSafeZoneTime );
    }

    public TransactionCommittingResponseUnpacker( Dependencies dependencies, int maxBatchSize,
            long idReuseSafeZoneTime )
    {
        this.dependencies = dependencies;
        this.maxBatchSize = maxBatchSize;
        this.idReuseSafeZoneTime = idReuseSafeZoneTime;
    }

    @Override
    public void unpackResponse( Response<?> response, TxHandler txHandler ) throws Exception
    {
        if ( stopped )
        {
            throw new IllegalStateException( "Component is currently stopped" );
        }

        BatchingResponseHandler responseHandler = new BatchingResponseHandler( maxBatchSize,
                batchCommitter, obligationFulfiller, txHandler, versionContextSupplier, log );
        try
        {
            response.accept( responseHandler );
        }
        finally
        {
            responseHandler.applyQueuedTransactions();
        }
    }

    @Override
    public void start()
    {
        this.obligationFulfiller = dependencies.obligationFulfiller();
        this.log = dependencies.logService().getInternalLog( BatchingResponseHandler.class );
        this.versionContextSupplier = dependencies.versionContextSupplier();
        this.batchCommitter = new TransactionBatchCommitter( dependencies.kernelTransactions(), idReuseSafeZoneTime,
                dependencies.commitProcess(), log );
        this.stopped = false;
    }

    @Override
    public void stop()
    {
        this.stopped = true;
    }
}
