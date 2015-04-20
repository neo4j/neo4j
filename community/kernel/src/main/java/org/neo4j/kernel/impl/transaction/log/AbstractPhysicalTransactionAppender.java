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

import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriterv1;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart.checksum;

abstract class AbstractPhysicalTransactionAppender implements TransactionAppender
{
    protected final WritableLogChannel channel;
    private final TransactionMetadataCache transactionMetadataCache;
    protected final LogFile logFile;
    private final LogRotation logRotation;
    private final TransactionIdStore transactionIdStore;
    private final TransactionLogWriter transactionLogWriter;
    private final LogPositionMarker positionMarker = new LogPositionMarker();
    private final IndexCommandDetector indexCommandDetector;
    private final KernelHealth kernelHealth;

    // For the graph store and schema indexes order-of-updates are managed by the high level entity locks
    // such that changes are applied to the affected records in the same order that they are written to the
    // log. For the legacy indexes there are no such locks, and hence no such ordering. This queue below
    // is introduced to manage just that and is only used for transactions that contain any legacy index changes.
    protected final IdOrderingQueue legacyIndexTransactionOrdering;

    protected AbstractPhysicalTransactionAppender( LogFile logFile, LogRotation logRotation,
            TransactionMetadataCache transactionMetadataCache, TransactionIdStore transactionIdStore,
            IdOrderingQueue legacyIndexTransactionOrdering, KernelHealth kernelHealth )
    {
        this.logFile = logFile;
        this.logRotation = logRotation;
        this.transactionIdStore = transactionIdStore;
        this.legacyIndexTransactionOrdering = legacyIndexTransactionOrdering;
        this.kernelHealth = kernelHealth;
        this.channel = logFile.getWriter();
        this.transactionMetadataCache = transactionMetadataCache;
        this.indexCommandDetector = new IndexCommandDetector( new CommandWriter( channel ) );
        this.transactionLogWriter = new TransactionLogWriter( new LogEntryWriterv1( channel, indexCommandDetector ) );
    }

    /**
     * @return whether or not this transaction contains any legacy index changes.
     */
    private TransactionCommitment append0( TransactionRepresentation transaction, long transactionId )
            throws IOException
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
            long transactionChecksum = checksum( transaction.additionalHeader(), transaction.getMasterId(),
                    transaction.getAuthorId() );
            transactionMetadataCache.cacheTransactionMetadata( transactionId, logPosition, transaction.getMasterId(),
                    transaction.getAuthorId(), transactionChecksum );
            emptyBufferIntoChannel();
            boolean containsLegacyIndexCommands = indexCommandDetector.hasWrittenAnyLegacyIndexCommand();
            if ( containsLegacyIndexCommands )
            {
                // Offer this transaction id to the queue so that the legacy index applier can take part in the ordering
                legacyIndexTransactionOrdering.offer( transactionId );
            }
            return new TransactionCommitment( containsLegacyIndexCommands, transactionId, transactionChecksum );
        }
        catch ( final Throwable panic )
        {
            kernelHealth.panic( panic );
            throw panic;
        }
    }

    protected abstract void emptyBufferIntoChannel() throws IOException;

    @Override
    public long append( TransactionRepresentation transaction, LogAppendEvent logAppendEvent ) throws IOException
    {
        long transactionId = -1;
        int phase = 0;
        // We put log rotation check outside the private append method since it must happen before
        // we generate the next transaction id
        logAppendEvent.setLogRotated( logRotation.rotateLogIfNeeded( logAppendEvent ) );

        TransactionCommitment commit = null;
        try
        {
            // Synchronized with logFile to get absolute control over concurrent rotations happening
            synchronized ( logFile )
            {
                try ( SerializeTransactionEvent serialiseEvent = logAppendEvent.beginSerializeTransaction() )
                {
                    transactionId = transactionIdStore.nextCommittingTransactionId();
                    phase = 1;
                    commit = append0( transaction, transactionId );
                }
            }

            forceAfterAppend( logAppendEvent );
            commit.complete();
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
            return append0( transaction, transactionId );
        }
    }

    private class TransactionCommitment implements Commitment
    {
        private final boolean hasLegacyIndexChanges;
        private final long transactionId;
        private final long transactionChecksum;
        private boolean markedAsCommitted;

        TransactionCommitment( boolean hasLegacyIndexChanges, long transactionId, long transactionChecksum )
        {
            this.hasLegacyIndexChanges = hasLegacyIndexChanges;
            this.transactionId = transactionId;
            this.transactionChecksum = transactionChecksum;
        }

        void complete() throws IOException
        {
            transactionCommitted();
            coordinateMultipleThreadsApplyingLegacyIndexChanges( hasLegacyIndexChanges, transactionId );
        }

        @Override
        public void transactionCommitted()
        {
            transactionIdStore.transactionCommitted( transactionId, transactionChecksum );
            markedAsCommitted = true;
        }
    }

    @Override
    public void force() throws IOException
    {
        forceChannel();
    }

    /**
     * Called as part of append.
     * @param logAppendEvent A trace event for the given log append operation.
     */
    protected abstract void forceAfterAppend( LogAppendEvent logAppendEvent ) throws IOException;

    protected final void forceChannel() throws IOException
    {
        synchronized ( channel )
        {
            channel.force();
        }
    }

    private void coordinateMultipleThreadsApplyingLegacyIndexChanges( boolean hasLegacyIndexChanges , long transactionId  )
            throws IOException
    {
        if ( hasLegacyIndexChanges )
        {
            try
            {
                legacyIndexTransactionOrdering.waitFor( transactionId );
            }
            catch ( InterruptedException e )
            {
                throw new IOException( "Interrupted while waiting for applying legacy index updates", e );
            }
        }
    }
}
