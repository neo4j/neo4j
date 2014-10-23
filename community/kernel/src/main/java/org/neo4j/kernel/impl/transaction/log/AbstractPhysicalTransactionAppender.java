/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriterv1;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

abstract class AbstractPhysicalTransactionAppender implements TransactionAppender
{
    protected final WritableLogChannel channel;
    private final TransactionMetadataCache transactionMetadataCache;
    private final LogFile logFile;
    private final TransactionIdStore transactionIdStore;
    private final TransactionLogWriter transactionLogWriter;
    private final LogPositionMarker positionMarker = new LogPositionMarker();
    private final IndexCommandDetector indexCommandDetector;

    // For the graph store and schema indexes order-of-updates are managed by the high level entity locks
    // such that changes are applied to the affected records in the same order that they are written to the
    // log. For the legacy indexes there are no such locks, and hence no such ordering. This queue below
    // is introduced to manage just that and is only used for transactions that contain any legacy index changes.
    protected final IdOrderingQueue legacyIndexTransactionOrdering;

    protected AbstractPhysicalTransactionAppender( LogFile logFile,
            TransactionMetadataCache transactionMetadataCache, TransactionIdStore transactionIdStore,
            IdOrderingQueue legacyIndexTransactionOrdering )
    {
        this.logFile = logFile;
        this.transactionIdStore = transactionIdStore;
        this.legacyIndexTransactionOrdering = legacyIndexTransactionOrdering;
        this.channel = logFile.getWriter();
        this.transactionMetadataCache = transactionMetadataCache;
        this.indexCommandDetector = new IndexCommandDetector( new CommandWriter( channel ) );
        this.transactionLogWriter = new TransactionLogWriter( new LogEntryWriterv1( channel, indexCommandDetector ) );
    }

    /**
     * @return whether or not this transaction contains any legacy index changes.
     */
    private boolean append0( TransactionRepresentation transaction, long transactionId ) throws IOException
    {
        channel.getCurrentPosition( positionMarker );
        LogPosition logPosition = positionMarker.newPosition();

        // Reset command writer so that we, after we've written the transaction, can ask it whether or
        // not any legacy index command was written. If so then there's additional ordering to care about below.
        indexCommandDetector.reset();
        transactionLogWriter.append( transaction, transactionId );

        transactionMetadataCache.cacheTransactionMetadata( transactionId, logPosition, transaction.getMasterId(),
                transaction.getAuthorId(), LogEntryStart.checksum( transaction.additionalHeader(),
                        transaction.getMasterId(), transaction.getAuthorId() ) );

        channel.emptyBufferIntoChannelAndClearIt();

        // Offer this transaction id to the queue so that the legacy index applier can take part in the ordering
        if ( indexCommandDetector.hasWrittenAnyLegacyIndexCommand() )
        {
            legacyIndexTransactionOrdering.offer( transactionId );
        }
        return indexCommandDetector.hasWrittenAnyLegacyIndexCommand();
    }

    @Override
    public long append( TransactionRepresentation transaction ) throws IOException
    {
        long transactionId = -1;
        boolean hasLegacyIndexChanges, rotated;
        // Synchronized with logFile to get absolute control over concurrent rotations happening
        synchronized ( logFile )
        {
            // We put log rotation check outside the private append method since it must happen before
            // we generate the next transaction id
            rotated = logFile.checkRotation();
            transactionId = transactionIdStore.nextCommittingTransactionId();
            hasLegacyIndexChanges = append0( transaction, transactionId );
        }

        forceAfterAppend( );
        pruneIfRotated( rotated );
        coordinateMultipleThreadsApplyingLegacyIndexChanges( hasLegacyIndexChanges, transactionId );
        return transactionId;
    }

    @Override
    public void append( TransactionRepresentation transaction, long expectedTransactionId ) throws IOException
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
            append0( transaction, transactionId );
        }
    }

    @Override
    public void force() throws IOException
    {
        forceChannel();
    }
    /**
     * Called as part of append.
     */
    protected abstract void forceAfterAppend( ) throws IOException;

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

    private void pruneIfRotated( boolean rotated )
    {
        if ( rotated )
        {
            logFile.prune();
        }
    }

    @Override
    public void close()
    {   // do nothing
    }
}
