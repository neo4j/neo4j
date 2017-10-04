/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.Flushable;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvents;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.api.TransactionToApply.TRANSACTION_ID_NOT_SPECIFIED;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart.checksum;

/**
 * Concurrently appends transactions to the transaction log, while coordinating with the log rotation and forcing the
 * log file in batches for higher throughput in a concurrent scenario.
 */
public class BatchingTransactionAppender extends LifecycleAdapter implements TransactionAppender
{
    // For the graph store and schema indexes order-of-updates are managed by the high level entity locks
    // such that changes are applied to the affected records in the same order that they are written to the
    // log. For the explicit indexes there are no such locks, and hence no such ordering. This queue below
    // is introduced to manage just that and is only used for transactions that contain any explicit index changes.
    private final IdOrderingQueue explicitIndexTransactionOrdering;

    private final AtomicReference<ThreadLink> threadLinkHead = new AtomicReference<>( ThreadLink.END );
    private final TransactionMetadataCache transactionMetadataCache;
    private final LogFile logFile;
    private final LogRotation logRotation;
    private final TransactionIdStore transactionIdStore;
    private final LogPositionMarker positionMarker = new LogPositionMarker();
    private final DatabaseHealth databaseHealth;
    private final Lock forceLock = new ReentrantLock();

    private FlushablePositionAwareChannel writer;
    private TransactionLogWriter transactionLogWriter;
    private IndexCommandDetector indexCommandDetector;

    public BatchingTransactionAppender( LogFile logFile, LogRotation logRotation,
            TransactionMetadataCache transactionMetadataCache, TransactionIdStore transactionIdStore,
            IdOrderingQueue explicitIndexTransactionOrdering, DatabaseHealth databaseHealth )
    {
        this.logFile = logFile;
        this.logRotation = logRotation;
        this.transactionIdStore = transactionIdStore;
        this.explicitIndexTransactionOrdering = explicitIndexTransactionOrdering;
        this.databaseHealth = databaseHealth;
        this.transactionMetadataCache = transactionMetadataCache;
    }

    @Override
    public void start() throws Throwable
    {
        this.writer = logFile.getWriter();
        this.indexCommandDetector = new IndexCommandDetector();
        this.transactionLogWriter = new TransactionLogWriter( new LogEntryWriter( writer ) );
    }

    @Override
    public long append( TransactionToApply batch, LogAppendEvent logAppendEvent ) throws IOException
    {
        // Assigned base tx id just to make compiler happy
        long lastTransactionId = TransactionIdStore.BASE_TX_ID;
        // Synchronized with logFile to get absolute control over concurrent rotations happening
        synchronized ( logFile )
        {
            // Assert that kernel is healthy before making any changes
            databaseHealth.assertHealthy( IOException.class );
            try ( SerializeTransactionEvent serialiseEvent = logAppendEvent.beginSerializeTransaction() )
            {
                // Append all transactions in this batch to the log under the same logFile monitor
                TransactionToApply tx = batch;
                while ( tx != null )
                {
                    long transactionId = transactionIdStore.nextCommittingTransactionId();

                    // If we're in a scenario where we're merely replicating transactions, i.e. transaction
                    // id have already been generated by another entity we simply check that our id
                    // that we generated match that id. If it doesn't we've run into a problem we can't ´
                    // really recover from and would point to a bug somewhere.
                    matchAgainstExpectedTransactionIdIfAny( transactionId, tx );

                    TransactionCommitment commitment = appendToLog( tx.transactionRepresentation(), transactionId );
                    tx.commitment( commitment, transactionId );
                    tx.logPosition( commitment.logPosition() );
                    tx = tx.next();
                    lastTransactionId = transactionId;
                }
            }
        }

        // At this point we've appended all transactions in this batch, but we can't mark any of them
        // as committed since they haven't been forced to disk yet. So here we force, or potentially
        // piggy-back on another force, but anyway after this call below we can be sure that all our transactions
        // in this batch exist durably on disk.
        if ( forceAfterAppend( logAppendEvent ) )
        {
            // We got lucky and were the one forcing the log. It's enough if ones of all doing concurrent committerss
            // checks the need for log rotation.
            boolean logRotated = logRotation.rotateLogIfNeeded( logAppendEvent );
            logAppendEvent.setLogRotated( logRotated );
        }

        // Mark all transactions as committed
        publishAsCommitted( batch );

        return lastTransactionId;
    }

    private void matchAgainstExpectedTransactionIdIfAny( long transactionId, TransactionToApply tx )
    {
        long expectedTransactionId = tx.transactionId();
        if ( expectedTransactionId != TRANSACTION_ID_NOT_SPECIFIED )
        {
            if ( transactionId != expectedTransactionId )
            {
                IllegalStateException ex = new IllegalStateException(
                        "Received " + tx.transactionRepresentation() + " with txId:" + expectedTransactionId +
                                " to be applied, but appending it ended up generating an unexpected txId:" +
                                transactionId );
                databaseHealth.panic( ex );
                throw ex;
            }
        }
    }

    private void publishAsCommitted( TransactionToApply batch )
    {
        while ( batch != null )
        {
            batch.commitment().publishAsCommitted();
            batch = batch.next();
        }
    }

    @Override
    public void checkPoint( LogPosition logPosition, LogCheckPointEvent logCheckPointEvent ) throws IOException
    {
        try
        {
            // Synchronized with logFile to get absolute control over concurrent rotations happening
            synchronized ( logFile )
            {
                transactionLogWriter.checkPoint( logPosition );
            }
        }
        catch ( Throwable cause )
        {
            databaseHealth.panic( cause );
            throw cause;
        }

        forceAfterAppend( logCheckPointEvent );
    }

    /**
     * @return A TransactionCommitment instance with metadata about the committed transaction, such as whether or not
     * this transaction contains any explicit index changes.
     */
    private TransactionCommitment appendToLog( TransactionRepresentation transaction, long transactionId )
            throws IOException
    {
        // Reset command writer so that we, after we've written the transaction, can ask it whether or
        // not any explicit index command was written. If so then there's additional ordering to care about below.
        indexCommandDetector.reset();

        // The outcome of this try block is either of:
        // a) transaction successfully appended, at which point we return a Commitment to be used after force
        // b) transaction failed to be appended, at which point a kernel panic is issued
        // The reason that we issue a kernel panic on failure in here is that at this point we're still
        // holding the logFile monitor, and a failure to append needs to be communicated with potential
        // log rotation, which will wait for all transactions closed or fail on kernel panic.
        try
        {
            LogPosition logPositionBeforeCommit = writer.getCurrentPosition( positionMarker ).newPosition();
            transactionLogWriter.append( transaction, transactionId );
            LogPosition logPositionAfterCommit = writer.getCurrentPosition( positionMarker ).newPosition();

            long transactionChecksum =
                    checksum( transaction.additionalHeader(), transaction.getMasterId(), transaction.getAuthorId() );
            transactionMetadataCache
                    .cacheTransactionMetadata( transactionId, logPositionBeforeCommit, transaction.getMasterId(),
                            transaction.getAuthorId(), transactionChecksum, transaction.getTimeCommitted() );

            transaction.accept( indexCommandDetector );
            boolean hasExplicitIndexChanges = indexCommandDetector.hasWrittenAnyExplicitIndexCommand();
            if ( hasExplicitIndexChanges )
            {
                // Offer this transaction id to the queue so that the explicit index applier can take part in the ordering
                explicitIndexTransactionOrdering.offer( transactionId );
            }
            return new TransactionCommitment( hasExplicitIndexChanges, transactionId, transactionChecksum,
                    transaction.getTimeCommitted(), logPositionAfterCommit, transactionIdStore );
        }
        catch ( final Throwable panic )
        {
            databaseHealth.panic( panic );
            throw panic;
        }
    }

    /**
     * Called by the appender that just appended a transaction to the log.
     *
     * @param logForceEvents A trace event for the given log append operation.
     * @return {@code true} if we got lucky and were the ones forcing the log.
     */
    protected boolean forceAfterAppend( LogForceEvents logForceEvents ) throws IOException
    {
        // There's a benign race here, where we add our link before we update our next pointer.
        // This is okay, however, because unparkAll() spins when it sees a null next pointer.
        ThreadLink threadLink = new ThreadLink( Thread.currentThread() );
        threadLink.next = threadLinkHead.getAndSet( threadLink );
        boolean attemptedForce = false;

        try ( LogForceWaitEvent logForceWaitEvent = logForceEvents.beginLogForceWait() )
        {
            do
            {
                if ( forceLock.tryLock() )
                {
                    attemptedForce = true;
                    try
                    {
                        forceLog( logForceEvents );
                        // In the event of any failure a database panic will be raised and thrown here
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

            // If there were many threads committing simultaneously and I wasn't the lucky one
            // actually doing the forcing (where failure would throw panic exception) I need to
            // explicitly check if everything is OK before considering this transaction committed.
            if ( !attemptedForce )
            {
                databaseHealth.assertHealthy( IOException.class );
            }
        }
        return attemptedForce;
    }

    private void forceLog( LogForceEvents logForceEvents ) throws IOException
    {
        ThreadLink links = threadLinkHead.getAndSet( ThreadLink.END );
        try ( LogForceEvent logForceEvent = logForceEvents.beginLogForce() )
        {
            force();
        }
        catch ( final Throwable panic )
        {
            databaseHealth.panic( panic );
            throw panic;
        }
        finally
        {
            unparkAll( links );
        }
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
                // Spin because of the race:y update when consing.
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

    private void force() throws IOException
    {
        // Empty buffer into writer. We want to synchronize with appenders somehow so that they
        // don't append while we're doing that. The way rotation is coordinated we can't synchronize
        // on logFile because it would cause deadlocks. Synchronizing on writer assumes that appenders
        // also synchronize on writer.
        Flushable flushable;
        synchronized ( logFile )
        {
            flushable = writer.prepareForFlush();
        }
        // Force the writer outside of the lock.
        // This allows other threads access to the buffer while the writer is being forced.
        try
        {
            flushable.flush();
        }
        catch ( ClosedChannelException ignored )
        {
            // This is ok, we were already successful in emptying the buffer, so the channel being closed here means
            // that some other thread is rotating the log and has closed the underlying channel. But since we were
            // successful in emptying the buffer *UNDER THE LOCK* we know that the rotating thread included the changes
            // we emptied into the channel, and thus it is already flushed by that thread.
        }
    }
}
