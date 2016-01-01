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

import java.io.IOException;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.function.Function;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.api.BatchingTransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogRotation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.util.Access;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.kernel.impl.api.TransactionApplicationMode.EXTERNAL;

/**
 * Receives and unpacks {@link Response responses}.
 * Transaction obligations are handled by {@link TransactionObligationFulfiller} and
 * {@link TransactionStream transaction streams} are {@link TransactionRepresentationStoreApplier applied to the
 * store},
 * in batches.
 * <p/>
 * It is assumed that any {@link TransactionStreamResponse response carrying transaction data} comes from the one
 * and same thread.
 */
public class TransactionCommittingResponseUnpacker implements ResponseUnpacker, Lifecycle
{
    // Visits all queued transactions, committing them
    private final TransactionVisitor batchCommitter = new TransactionVisitor()
    {
        @Override
        public void visit( CommittedTransactionRepresentation transaction, TxHandler handler,
                Access<Commitment> commitmentAccess ) throws IOException
        {
            // Tuck away the Commitment returned from the call to append. We'll use each Commitment right before
            // applying each transaction.
            Commitment commitment = appender.append( transaction.getTransactionRepresentation(),
                    transaction.getCommitEntry().getTxId() );
            commitmentAccess.set( commitment );
        }
    };
    // Visits all queued, and recently appended, transactions, applying them to the store
    private final TransactionVisitor batchApplier = new TransactionVisitor()
    {
        @Override
        public void visit( CommittedTransactionRepresentation transaction, TxHandler handler,
                Access<Commitment> commitmentAccess ) throws IOException
        {
            long transactionId = transaction.getCommitEntry().getTxId();
            TransactionRepresentation representation = transaction.getTransactionRepresentation();
            commitmentAccess.get().publishAsCommitted();
            try ( LockGroup locks = new LockGroup();
                  ValidatedIndexUpdates indexUpdates = indexUpdatesValidator.validate( representation ) )
            {
                storeApplier.apply( representation, indexUpdates, locks, transactionId, EXTERNAL );
                handler.accept( transaction );
            }
        }
    };
    private final TransactionVisitor batchCloser = new TransactionVisitor()
    {
        @Override
        public void visit( CommittedTransactionRepresentation transaction, TxHandler handler,
                Access<Commitment> commitmentAccess ) throws IOException
        {
            if ( commitmentAccess.get().markedAsCommitted() )
            {
                transactionIdStore.transactionClosed( transaction.getCommitEntry().getTxId() );
            }
        }
    };

    static final String msg = "Kernel panic detected: pulled transactions cannot be applied to a non-healthy database. "
                              + "In order to resolve this issue a manual restart of this instance is required.";

    private final DependencyResolver resolver;
    private final Function<DependencyResolver,IndexUpdatesValidator> validatorFunction;
    private final Function<DependencyResolver,BatchingTransactionRepresentationStoreApplier> applierFunction;
    private final TransactionQueue transactionQueue;

    private TransactionAppender appender;
    private BatchingTransactionRepresentationStoreApplier storeApplier;
    private IndexUpdatesValidator indexUpdatesValidator;
    private TransactionIdStore transactionIdStore;
    private TransactionObligationFulfiller obligationFulfiller;
    private LogFile logFile;
    private LogRotation logRotation;
    private KernelHealth kernelHealth;
    private StringLogger log;

    private volatile boolean stopped;

    public TransactionCommittingResponseUnpacker( DependencyResolver resolver, int maxBatchSize )
    {
        this( resolver, maxBatchSize,
                new DefaultIndexUpdatesValidatorCreator(), new DefaultBatchingStoreApplierCreator() );
    }

    // test only
    public TransactionCommittingResponseUnpacker( DependencyResolver resolver, int maxBatchSize,
            // These two are just for testing purposes, to inject different behaviour.
            // It's made hard by the fact this class is not accepting dependencies in its constructor,
            // rather has a weird place in the lifecycle of things where it needs to pull things
            // out in #start()
            Function<DependencyResolver,IndexUpdatesValidator> validatorFunction,
            Function<DependencyResolver,BatchingTransactionRepresentationStoreApplier> applierFunction )
    {
        this.resolver = resolver;
        this.validatorFunction = validatorFunction;
        this.applierFunction = applierFunction;
        this.transactionQueue = new TransactionQueue( maxBatchSize );
    }

    private static TransactionObligationFulfiller resolveTransactionObligationFulfiller( DependencyResolver resolver )
    {
        try
        {
            return resolver.resolveDependency( TransactionObligationFulfiller.class );
        }
        catch ( IllegalArgumentException e )
        {
            return new TransactionObligationFulfiller()
            {
                @Override
                public void fulfill( long toTxId )
                {
                    throw new UnsupportedOperationException( "Should not be called" );
                }
            };
        }
    }

    @Override
    public void unpackResponse( Response<?> response, final TxHandler txHandler ) throws IOException
    {
        if ( stopped )
        {
            throw new IllegalStateException( "Component is currently stopped" );
        }

        try
        {
            response.accept( new BatchingResponseHandler( txHandler ) );
        }
        finally
        {
            if ( response.hasTransactionsToBeApplied() )
            {
                applyQueuedTransactions();
            }
        }
    }

    private void applyQueuedTransactions() throws IOException
    {
        // Synchronize to guard for concurrent shutdown
        synchronized ( logFile )
        {
            // Check rotation explicitly, since the version of append that we're calling isn't doing that.
            logRotation.rotateLogIfNeeded( LogAppendEvent.NULL );

            // Check kernel health after log rotation
            if ( !kernelHealth.isHealthy() )
            {
                Throwable causeOfPanic = kernelHealth.getCauseOfPanic();
                log.error( msg + " Original kernel panic cause was:\n" + causeOfPanic.getMessage() );
                throw new IOException( msg, causeOfPanic );
            }

            try
            {
                // Apply whatever is in the queue
                if ( transactionQueue.accept( batchCommitter ) > 0 )
                {
                    // TODO if this instance is set to "slave_only" then we can actually skip the force call here.
                    // Reason being that even if there would be a reordering in some layer where a store file would be
                    // changed before that change would have ended up in the log, it would be fine sine as a slave
                    // you would pull that transaction again anyhow before making changes to (after reading) any record.
                    appender.force();
                    try
                    {
                        // Apply all transactions to the store. Only apply, i.e. mark as committed, not closed.
                        // We mark as closed below.
                        transactionQueue.accept( batchApplier );
                        // Ensure that all changes are flushed to the store, we're doing some batching of transactions
                        // here so some shortcuts are taken in places. Although now comes the time where we must
                        // ensure that all pending changes are applied and flushed properly.
                        storeApplier.closeBatch();
                    }
                    finally
                    {
                        // Mark the applied transactions as closed. We must do this as a separate step after
                        // applying them, with a closeBatch() call in between, otherwise there might be
                        // threads waiting for transaction obligations to be fulfilled and since they are looking
                        // at last closed transaction id they might get notified to continue before all data
                        // has actually been flushed properly.
                        transactionQueue.accept( batchCloser );
                    }
                }
            }
            finally
            {
                transactionQueue.clear();
            }
        }
    }

    @Override
    public void init() throws Throwable
    {   // Nothing to init
    }

    @Override
    public void start() throws Throwable
    {
        this.appender = resolver.resolveDependency( LogicalTransactionStore.class ).getAppender();
        this.storeApplier = applierFunction.apply( resolver );
        this.indexUpdatesValidator = validatorFunction.apply( resolver );
        this.transactionIdStore = resolver.resolveDependency( TransactionIdStore.class );
        this.obligationFulfiller = resolveTransactionObligationFulfiller( resolver );
        this.logFile = resolver.resolveDependency( LogFile.class );
        this.logRotation = resolver.resolveDependency( LogRotation.class );
        this.kernelHealth = resolver.resolveDependency( KernelHealth.class );
        this.log = resolver.resolveDependency( Logging.class ).getMessagesLog( getClass() );
        this.stopped = false;
    }

    @Override
    public void stop() throws Throwable
    {
        this.stopped = true;
    }

    @Override
    public void shutdown() throws Throwable
    {   // Nothing to shut down
    }

    private class BatchingResponseHandler implements Response.Handler,
            Visitor<CommittedTransactionRepresentation,IOException>
    {
        private final TxHandler txHandler;

        private BatchingResponseHandler( TxHandler txHandler )
        {
            this.txHandler = txHandler;
        }

        @Override
        public void obligation( long txId ) throws IOException
        {
            if ( txId == TransactionIdStore.BASE_TX_ID )
            {   // Means "empty" response
                return;
            }

            try
            {
                obligationFulfiller.fulfill( txId );
            }
            catch ( IllegalStateException e )
            {
                throw new ComException( "Failed to pull updates", e )
                        .traceComException( log, "BatchingResponseHandler.obligation" );
            }
            catch ( InterruptedException e )
            {
                throw new IOException( e );
            }
        }

        @Override
        public Visitor<CommittedTransactionRepresentation,IOException> transactions()
        {
            return this;
        }

        @Override
        public boolean visit( CommittedTransactionRepresentation transaction ) throws IOException
        {
            if ( transactionQueue.queue( transaction, txHandler ) )
            {
                applyQueuedTransactions();
            }
            return false;
        }
    }
}
