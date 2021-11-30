/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscChunkedArrayQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Health;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.TransactionIdStore;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.neo4j.configuration.GraphDatabaseSettings.max_concurrent_transactions;
import static org.neo4j.internal.helpers.Exceptions.throwIfUnchecked;
import static org.neo4j.kernel.impl.api.TransactionToApply.TRANSACTION_ID_NOT_SPECIFIED;

public class TransactionLogQueue extends LifecycleAdapter
{
    private static final int CONSUMER_MAX_BATCH = 1024;
    private static final int INITIAL_CAPACITY = 128;
    private final LogFiles logFiles;
    private final LogRotation logRotation;
    private final TransactionIdStore transactionIdStore;
    private final Health databaseHealth;
    private final TransactionMetadataCache transactionMetadataCache;
    private final MpscChunkedArrayQueue<TxQueueElement> txAppendQueue;
    private final JobScheduler jobScheduler;
    private final Log log;
    private JobHandle<?> jobHandle;
    private TransactionWriter transactionWriter;
    private volatile boolean stopped;

    public TransactionLogQueue( LogFiles logFiles, TransactionIdStore transactionIdStore, Health databaseHealth,
            TransactionMetadataCache transactionMetadataCache, Config config, JobScheduler jobScheduler, LogProvider logProvider )
    {
        this.logFiles = logFiles;
        this.logRotation = logFiles.getLogFile().getLogRotation();
        this.transactionIdStore = transactionIdStore;
        this.databaseHealth = databaseHealth;
        this.transactionMetadataCache = transactionMetadataCache;
        this.txAppendQueue = new MpscChunkedArrayQueue<>( INITIAL_CAPACITY, config.get( max_concurrent_transactions ) );
        this.jobScheduler = jobScheduler;
        this.stopped = true;
        this.log = logProvider.getLog( getClass() );
    }

    public Future<Long> submit( TransactionToApply batch, LogAppendEvent logAppendEvent ) throws IOException
    {
        if ( stopped )
        {
            return CompletableFuture.failedFuture( new DatabaseShutdownException() );
        }
        TxQueueElement txQueueElement = new TxQueueElement( batch, logAppendEvent );
        while ( !txAppendQueue.offer( txQueueElement ) )
        {
            if ( stopped )
            {
                return CompletableFuture.failedFuture( new DatabaseShutdownException() );
            }
            parkNanos( MILLISECONDS.toNanos( 10 ) );
        }
        return txQueueElement.resultFuture;
    }

    @Override
    public synchronized void start()
    {
        transactionWriter =
                new TransactionWriter( txAppendQueue, logFiles.getLogFile(), transactionIdStore, databaseHealth, transactionMetadataCache, logRotation, log );
        jobHandle = jobScheduler.schedule( Group.LOG_WRITER, transactionWriter );
        stopped = false;
    }

    @Override
    public synchronized void shutdown() throws ExecutionException, InterruptedException
    {
        stopped = true;
        TransactionWriter writer = this.transactionWriter;
        JobHandle<?> handle = this.jobHandle;

        if ( writer != null )
        {
            writer.stop();
        }
        if ( handle != null )
        {
            handle.cancel();
            try
            {
                handle.waitTermination();
            }
            catch ( CancellationException ignore )
            {
            }
        }
    }

    private static class TxQueueElement
    {
        final TransactionToApply batch;
        final LogAppendEvent logAppendEvent;
        final CompletableFuture<Long> resultFuture;

        TxQueueElement( TransactionToApply batch, LogAppendEvent logAppendEvent )
        {
            this.batch = batch;
            this.logAppendEvent = logAppendEvent;
            this.resultFuture = new CompletableFuture<>();
        }
    }

    private static class TransactionWriter implements Runnable
    {
        private final MpscChunkedArrayQueue<TxQueueElement> txQueue;
        private final TransactionLogWriter transactionLogWriter;
        private final LogFile logFile;
        private final TransactionIdStore transactionIdStore;
        private final Health databaseHealth;
        private final TransactionMetadataCache transactionMetadataCache;
        private final LogRotation logRotation;
        private final Log log;
        private final int checksum;
        private volatile boolean stopped;
        private final MessagePassingQueue.WaitStrategy waitStrategy;

        TransactionWriter( MpscChunkedArrayQueue<TxQueueElement> txQueue, LogFile logFile, TransactionIdStore transactionIdStore, Health databaseHealth,
                TransactionMetadataCache transactionMetadataCache, LogRotation logRotation, Log log )
        {
            this.txQueue = txQueue;
            this.transactionLogWriter = logFile.getTransactionLogWriter();
            this.logFile = logFile;
            this.checksum = transactionIdStore.getLastCommittedTransaction().checksum();
            this.transactionIdStore = transactionIdStore;
            this.databaseHealth = databaseHealth;
            this.transactionMetadataCache = transactionMetadataCache;
            this.logRotation = logRotation;
            this.log = log;
            this.waitStrategy = new SleepingWaitingStrategy();
        }

        @Override
        public void run()
        {
            TxConsumer txConsumer = new TxConsumer( databaseHealth, transactionIdStore, transactionLogWriter, checksum, transactionMetadataCache );

            int idleCounter = 0;
            while ( !stopped )
            {
                try
                {
                    int drainedElements = txQueue.drain( txConsumer, CONSUMER_MAX_BATCH );
                    if ( drainedElements > 0 )
                    {
                        idleCounter = 0;
                        txConsumer.processBatch();

                        LogAppendEvent logAppendEvent = txConsumer.txElements[drainedElements - 1].logAppendEvent;
                        boolean logRotated = logRotation.locklessRotateLogIfNeeded( logAppendEvent );
                        logAppendEvent.setLogRotated( logRotated );
                        if ( !logRotated )
                        {
                            logFile.locklessForce( logAppendEvent );
                        }

                        txConsumer.complete();
                    }
                    else
                    {
                        idleCounter = waitStrategy.idle( idleCounter );
                    }
                }
                catch ( Exception e )
                {
                    log.error( "Transaction log applier failure.", e );
                    databaseHealth.panic( e );
                    txConsumer.cancelBatch( e );
                }
            }

            DatabaseShutdownException databaseShutdownException = new DatabaseShutdownException();
            TxQueueElement element;
            while ( (element = txQueue.poll()) != null )
            {
                element.resultFuture.completeExceptionally( databaseShutdownException );
            }
        }

        private static class TxConsumer implements MessagePassingQueue.Consumer<TxQueueElement>
        {
            private final Health databaseHealth;
            private final TransactionIdStore transactionIdStore;
            private final TransactionLogWriter transactionLogWriter;
            private final TransactionMetadataCache transactionMetadataCache;

            private int checksum;
            private final TxQueueElement[] txElements = new TransactionLogQueue.TxQueueElement[CONSUMER_MAX_BATCH];
            private final long[] txIds = new long[CONSUMER_MAX_BATCH];
            private int index;

            TxConsumer( Health databaseHealth, TransactionIdStore transactionIdStore, TransactionLogWriter transactionLogWriter, int checksum,
                    TransactionMetadataCache transactionMetadataCache )
            {
                this.transactionMetadataCache = transactionMetadataCache;
                this.databaseHealth = databaseHealth;
                this.transactionIdStore = transactionIdStore;
                this.transactionLogWriter = transactionLogWriter;
                this.checksum = checksum;
            }

            @Override
            public void accept( TxQueueElement txQueueElement )
            {
                txElements[index++] = txQueueElement;
            }

            private void processBatch() throws IOException
            {
                databaseHealth.assertHealthy( IOException.class );
                int drainedElements = index;
                for ( int i = 0; i < drainedElements; i++ )
                {
                    TxQueueElement txQueueElement = txElements[i];
                    LogAppendEvent logAppendEvent = txQueueElement.logAppendEvent;
                    long lastTransactionId = TransactionIdStore.BASE_TX_ID;
                    try ( var appendEvent = logAppendEvent.beginAppendTransaction( drainedElements ) )
                    {
                        TransactionToApply tx = txQueueElement.batch;
                        while ( tx != null )
                        {
                            long transactionId = transactionIdStore.nextCommittingTransactionId();

                            // If we're in a scenario where we're merely replicating transactions, i.e. transaction
                            // id have already been generated by another entity we simply check that our id
                            // that we generated match that id. If it doesn't we've run into a problem we can't ´
                            // really recover from and would point to a bug somewhere.
                            matchAgainstExpectedTransactionIdIfAny( transactionId, tx );

                            TransactionCommitment commitment = appendToLog( tx.transactionRepresentation(), transactionId, logAppendEvent, checksum );
                            checksum = commitment.getTransactionChecksum();
                            tx.commitment( commitment, transactionId );
                            tx.logPosition( commitment.logPosition() );
                            tx = tx.next();
                            lastTransactionId = transactionId;
                        }
                        txIds[i] = lastTransactionId;
                    }
                    catch ( Exception e )
                    {
                        txQueueElement.resultFuture.completeExceptionally( e );
                        throwIfUnchecked( e );
                        throw new RuntimeException( e );
                    }
                }
            }

            private void matchAgainstExpectedTransactionIdIfAny( long transactionId, TransactionToApply tx )
            {
                long expectedTransactionId = tx.transactionId();
                if ( TRANSACTION_ID_NOT_SPECIFIED != expectedTransactionId )
                {
                    if ( transactionId != expectedTransactionId )
                    {
                        throw new IllegalStateException(
                                "Received " + tx.transactionRepresentation() + " with txId:" + expectedTransactionId +
                                        " to be applied, but appending it ended up generating an unexpected txId:" +
                                        transactionId );
                    }
                }
            }

            private TransactionCommitment appendToLog( TransactionRepresentation transaction, long transactionId, LogAppendEvent logAppendEvent,
                    int previousChecksum ) throws IOException
            {
                var logPositionBeforeCommit = transactionLogWriter.getCurrentPosition();
                int checksum = transactionLogWriter.append( transaction, transactionId, previousChecksum );
                var logPositionAfterCommit = transactionLogWriter.getCurrentPosition();
                logAppendEvent.appendToLogFile( logPositionBeforeCommit, logPositionAfterCommit );

                transactionMetadataCache.cacheTransactionMetadata( transactionId, logPositionBeforeCommit );

                return new TransactionCommitment( transactionId, checksum, transaction.getTimeCommitted(), logPositionAfterCommit, transactionIdStore );
            }

            public void complete()
            {
                for ( int i = 0; i < index; i++ )
                {
                    txElements[i].resultFuture.complete( txIds[i] );
                }
                Arrays.fill( txElements, 0, index, null );
                index = 0;
            }

            public void cancelBatch( Exception e )
            {
                for ( int i = 0; i < index; i++ )
                {
                    txElements[i].resultFuture.completeExceptionally( e );
                }
                Arrays.fill( txElements, 0, index, null );
                index = 0;
            }
        }

        public void stop()
        {
            stopped = true;
        }
    }

    private static class SleepingWaitingStrategy implements MessagePassingQueue.WaitStrategy
    {
        private static final int YIELD_THRESHOLD = 100;
        private static final int PARK_MILLIS = 10;

        @Override
        public int idle( int idleCounter )
        {
            if ( idleCounter < YIELD_THRESHOLD )
            {
                Thread.yield();
            }
            else
            {
                parkNanos( MILLISECONDS.toNanos( PARK_MILLIS ) );
            }
            return idleCounter + 1;
        }
    }
}
