/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart.checksum;

/**
 * Concurrently appends transactions to the transaction log, while coordinating with the log rotation and forcing the
 * log file in batches.
 */
class BatchingTransactionAppender implements TransactionAppender
{
    private static class ThreadLink
    {
        final Thread thread;
        volatile ThreadLink next;
        volatile boolean done;

        public ThreadLink( Thread thread )
        {
            this.thread = thread;
        }

        public void unpark()
        {
            LockSupport.unpark( thread );
        }

        static final ThreadLink END = new ThreadLink( null );
        static {
            END.next = END;
        }
    }

    // For the graph store and schema indexes order-of-updates are managed by the high level entity locks
    // such that changes are applied to the affected records in the same order that they are written to the
    // log. For the legacy indexes there are no such locks, and hence no such ordering. This queue below
    // is introduced to manage just that and is only used for transactions that contain any legacy index changes.
    private final IdOrderingQueue legacyIndexTransactionOrdering;

    private final AtomicReference<ThreadLink> threadLinkHead = new AtomicReference<>( ThreadLink.END );
    private final WritableLogChannel channel;
    private final TransactionMetadataCache transactionMetadataCache;
    private final LogFile logFile;
    private final LogRotation logRotation;
    private final TransactionIdStore transactionIdStore;
    private final TransactionLogWriter transactionLogWriter;
    private final LogPositionMarker positionMarker = new LogPositionMarker();
    private final IndexCommandDetector indexCommandDetector;
    private final KernelHealth kernelHealth;
    private final Lock forceLock;

    public BatchingTransactionAppender( LogFile logFile, LogRotation logRotation,
                                        TransactionMetadataCache transactionMetadataCache,
                                        TransactionIdStore transactionIdStore,
                                        IdOrderingQueue legacyIndexTransactionOrdering,
                                        KernelHealth kernelHealth )
    {
        this.logFile = logFile;
        this.logRotation = logRotation;
        this.transactionIdStore = transactionIdStore;
        this.legacyIndexTransactionOrdering = legacyIndexTransactionOrdering;
        this.kernelHealth = kernelHealth;
        this.channel = logFile.getWriter();
        this.transactionMetadataCache = transactionMetadataCache;
        this.indexCommandDetector = new IndexCommandDetector( new CommandWriter( channel ) );
        this.transactionLogWriter = new TransactionLogWriter( new LogEntryWriter( channel, indexCommandDetector ) );
        forceLock = new ReentrantLock();
    }

    @Override
    public long append( TransactionRepresentation transaction, LogAppendEvent logAppendEvent ) throws IOException
    {
        long transactionId = -1;
        int phase = 0;
        // We put log rotation check outside the private append method since it must happen before
        // we generate the next transaction id
        logAppendEvent.setLogRotated( logRotation.rotateLogIfNeeded( logAppendEvent ) );

        TransactionCommitment commit;
        try
        {
            // Synchronized with logFile to get absolute control over concurrent rotations happening
            synchronized ( logFile )
            {
                try ( SerializeTransactionEvent serialiseEvent = logAppendEvent.beginSerializeTransaction() )
                {
                    transactionId = transactionIdStore.nextCommittingTransactionId();
                    phase = 1;
                    commit = appendToLog( transaction, transactionId );
                }
            }

            forceAfterAppend( logAppendEvent );
            commit.publishAsCommitted();
            orderLegacyIndexChanges( commit );
            phase = 2;
            return transactionId;
        }
        finally
        {
            if ( phase == 1 )
            {
                // So we end up here if we enter phase 1, but fails to reach phase 2, which means that
                // we told TransactionIdStore that we committed transaction, but something failed right after
                transactionIdStore.transactionClosed( transactionId );
            }
        }
    }

    @Override
    public Commitment append( TransactionRepresentation transaction, long expectedTransactionId ) throws IOException
    {
        // TODO this method is supposed to only be called from a single thread we should
        // be able to remove this synchronized block. The only reason it's here now is that LogFile exposes
        // a checkRotation, which any thread could call at any time. Although that method was added to
        // be able to test a certain thing, so it should go away actually.

        // Synchronized with logFile to get absolute control over concurrent rotations happening
        synchronized ( logFile )
        {
            long transactionId = transactionIdStore.nextCommittingTransactionId();
            if ( transactionId != expectedTransactionId )
            {
                throw new ThisShouldNotHappenError( "Zhen Li and Mattias Persson",
                        "Received " + transaction + " with txId:" + expectedTransactionId +
                        " to be applied, but appending it ended up generating an unexpected txId:" + transactionId );
            }
            return appendToLog( transaction, transactionId );
        }
    }

    private static class TransactionCommitment implements Commitment
    {
        private final boolean hasLegacyIndexChanges;
        private final long transactionId;
        private final long transactionChecksum;
        private final TransactionIdStore transactionIdStore;

        TransactionCommitment( boolean hasLegacyIndexChanges, long transactionId, long transactionChecksum,
                               TransactionIdStore transactionIdStore )
        {
            this.hasLegacyIndexChanges = hasLegacyIndexChanges;
            this.transactionId = transactionId;
            this.transactionChecksum = transactionChecksum;
            this.transactionIdStore = transactionIdStore;
        }

        @Override
        public void publishAsCommitted()
        {
            transactionIdStore.transactionCommitted( transactionId, transactionChecksum );
        }
    }

    /**
     * @return A TransactionCommitment instance with metadata about the committed transaction, such as whether or not this transaction
     * contains any legacy index changes.
     */
    private TransactionCommitment appendToLog(
            TransactionRepresentation transaction, long transactionId ) throws IOException
    {
        // Reset command writer so that we, after we've written the transaction, can ask it whether or
        // not any legacy index command was written. If so then there's additional ordering to care about below.
        indexCommandDetector.reset();

        // The outcome of this try block is either of:
        // a) transaction successfully appended, at which point we return a Commitment to be used after force
        // b) transaction failed to be appended, at which point a kernel panic is issued
        // The reason that we issue a kernel panic on failure in here is that at this point we're still
        // holding the logFile monitor, and a failure to append needs to be communicated with potential
        // log rotation, which will wait for all transactions closed or fail on kernel panic.
        try
        {
            LogPosition logPosition;
            synchronized ( channel )
            {
                logPosition = channel.getCurrentPosition( positionMarker ).newPosition();
                transactionLogWriter.append( transaction, transactionId );
            }

            long transactionChecksum = checksum(
                    transaction.additionalHeader(), transaction.getMasterId(), transaction.getAuthorId() );
            transactionMetadataCache.cacheTransactionMetadata(
                    transactionId, logPosition, transaction.getMasterId(), transaction.getAuthorId(),
                    transactionChecksum );

            boolean hasLegacyIndexChanges = indexCommandDetector.hasWrittenAnyLegacyIndexCommand();
            if ( hasLegacyIndexChanges )
            {
                // Offer this transaction id to the queue so that the legacy index applier can take part in the ordering
                legacyIndexTransactionOrdering.offer( transactionId );
            }
            return new TransactionCommitment(
                    hasLegacyIndexChanges, transactionId, transactionChecksum, transactionIdStore );
        }
        catch ( final Throwable panic )
        {
            kernelHealth.panic( panic );
            throw panic;
        }
    }

    private void orderLegacyIndexChanges( TransactionCommitment commit ) throws IOException
    {
        if ( commit.hasLegacyIndexChanges )
        {
            try
            {
                legacyIndexTransactionOrdering.waitFor( commit.transactionId );
            }
            catch ( InterruptedException e )
            {
                throw new IOException( "Interrupted while waiting for applying legacy index updates", e );
            }
        }
    }

    /**
     * Called by the appender that just appended a transaction to the log.
     * @param logAppendEvent A trace event for the given log append operation.
     */
    protected void forceAfterAppend( LogAppendEvent logAppendEvent ) throws IOException // 'protected' because of LogRotationDeadlockTest
    {
        // There's a benign race here, where we add our link before we update our next pointer.
        // This is okay, however, because unparkAll() spins when it sees a null next pointer.
        ThreadLink threadLink = new ThreadLink( Thread.currentThread() );
        threadLink.next = threadLinkHead.getAndSet( threadLink );

        try ( LogForceWaitEvent logForceWaitEvent = logAppendEvent.beginLogForceWait() )
        {
            do
            {
                if ( forceLock.tryLock() )
                {
                    try
                    {
                        forceLog( logAppendEvent );
                    }
                    finally
                    {
                        forceLock.unlock();

                        // We've released the lock, so unpark anyone who might have decided park while we were working.
                        // The most recently parked thread is the one most likely to still have warm caches, so that's
                        // the one we would prefer to unpark. Luckily, the stack nature of the ThreadLinks makes it easy
                        // to get to.
                        ThreadLink nextWaiter = threadLinkHead.get();
                        nextWaiter.unpark();
                    }
                }
                else
                {
                    waitForLogForce();
                }
            }
            while ( !threadLink.done );
        }
    }

    private void forceLog( LogAppendEvent logAppendEvent ) throws IOException
    {
        ThreadLink links = threadLinkHead.getAndSet( ThreadLink.END );

        try ( LogForceEvent logForceEvent = logAppendEvent.beginLogForce() )
        {
            force();
        }

        unparkAll( links );
    }

    private void unparkAll( ThreadLink links )
    {
        do
        {
            links.done = true;
            links.unpark();
            ThreadLink tmp;
            do
            {
                // Spin on this because of the racy update when consing.
                tmp = links.next;
            }
            while ( tmp == null );
            links = tmp;
        }
        while ( links != ThreadLink.END );
    }

    private void waitForLogForce()
    {
        long parkTime = TimeUnit.MILLISECONDS.toNanos( 100 );
        LockSupport.parkNanos( this, parkTime );
    }

    @Override
    public void force() throws IOException
    {
        // Empty buffer into channel. We want to synchronize with appenders somehow so that they
        // don't append while we're doing that. The way rotation is coordinated we can't synchronize
        // on logFile because it would cause deadlocks. Synchronizing on channel assumes that appenders
        // also synchronize on channel.
        synchronized ( channel )
        {
            channel.emptyBufferIntoChannelAndClearIt();
        }

        channel.force();
    }
}
