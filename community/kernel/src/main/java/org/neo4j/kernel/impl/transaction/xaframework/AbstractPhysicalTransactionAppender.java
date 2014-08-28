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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryWriterv1;

abstract class AbstractPhysicalTransactionAppender implements TransactionAppender
{
    protected final WritableLogChannel channel;
    private final TxIdGenerator txIdGenerator;
    private final TransactionMetadataCache transactionMetadataCache;
    private final LogFile logFile;
    private final TransactionIdStore transactionIdStore;
    private final TransactionLogWriter transactionLogWriter;
    private final LogPositionMarker positionMarker = new LogPositionMarker();
    private final CommandWriter commandWriter;

    // For the graph store and schema indexes order-of-updates are managed by the high level entity locks
    // such that changes are applied to the affected records in the same order that they are written to the
    // log. For the legacy indexes there are no such locks, and hence no such ordering. This queue below
    // is introduced to manage just that and is only used for transactions that contain any legacy index changes.
    protected final IdOrderingQueue legacyIndexTransactionOrdering;

    public AbstractPhysicalTransactionAppender( LogFile logFile, TxIdGenerator txIdGenerator,
            TransactionMetadataCache transactionMetadataCache, TransactionIdStore transactionIdStore,
            IdOrderingQueue legacyIndexTransactionOrdering )
    {
        this.logFile = logFile;
        this.transactionIdStore = transactionIdStore;
        this.legacyIndexTransactionOrdering = legacyIndexTransactionOrdering;
        this.channel = logFile.getWriter();
        this.txIdGenerator = txIdGenerator;
        this.transactionMetadataCache = transactionMetadataCache;
        this.commandWriter = new CommandWriter( channel );
        this.transactionLogWriter = new TransactionLogWriter( new LogEntryWriterv1( channel, commandWriter ) );
    }

    /**
     * @return whether or not this transaction contains any legacy index changes.
     */
    private boolean append( TransactionRepresentation transaction, long transactionId ) throws IOException
    {
        channel.getCurrentPosition( positionMarker );
        LogPosition logPosition = positionMarker.newPosition();

        // Reset command writer so that we, after we've written the transaction, can ask it whether or
        // not any legacy index command was written. If so then there's additional ordering to care about below.
        commandWriter.reset();
        transactionLogWriter.append( transaction, transactionId );

        transactionMetadataCache.cacheTransactionMetadata( transactionId, logPosition, transaction.getMasterId(),
                transaction.getAuthorId(), LogEntryStart.checksum( transaction.additionalHeader(),
                        transaction.getMasterId(), transaction.getAuthorId() ) );

        channel.emptyBufferIntoChannelAndClearIt();
        
        // Offer this transaction id to the queue so that the legacy index applier can take part in the ordering
        boolean hasLegacyIndexChanges = commandWriter.hasWrittenAnyLegacyIndexCommand();
        if ( hasLegacyIndexChanges )
        {
            legacyIndexTransactionOrdering.offer( transactionId );
        }
        return hasLegacyIndexChanges;
    }

    @Override
    public long append( TransactionRepresentation transaction ) throws TransactionAppendException
    {
        long transactionId = -1;
        try
        {
            long ticket;
            boolean hasLegacyIndexChanges;
            synchronized ( this )
            {
                // We put log rotation check outside the private append method since it must happen before
                // we generate the next transaction id
                logFile.checkRotation();
                transactionId = txIdGenerator.generate( transaction );
                hasLegacyIndexChanges = append( transaction, transactionId );
                ticket = getCurrentTicket();
            }
            
            force( ticket );
            afterForce( transactionId, hasLegacyIndexChanges );
            return transactionId;
        }
        catch ( Exception e )
        {
            throw new TransactionAppendException( e, transactionId );
        }
    }

    private void afterForce( long transactionId, boolean hasLegacyIndexChanges )
    {
        if ( hasLegacyIndexChanges )
        {
            legacyIndexTransactionOrdering.awaitHead( transactionId );
        }
    }

    protected long getCurrentTicket()
    {
        return 0;
    }

    protected abstract void force( long ticket ) throws IOException;

    @Override
    public boolean append( CommittedTransactionRepresentation transaction )
            throws TransactionAppendException
    {
        long transactionId = -1;
        try
        {
            boolean result = false, hasLegacyIndexChanges;
            long ticket;
            synchronized ( this )
            {
                logFile.checkRotation();
                long lastCommittedTxId = transactionIdStore.getLastCommittedTransactionId();
                long candidateTransactionId = transaction.getCommitEntry().getTxId();
                if ( lastCommittedTxId + 1 == candidateTransactionId )
                {
                    transactionId = txIdGenerator.generate( transaction.getTransactionRepresentation() );
                    hasLegacyIndexChanges = append( transaction.getTransactionRepresentation(), transactionId );
                    ticket = getCurrentTicket();
                    result = true;
                }
                else if ( lastCommittedTxId + 1 < candidateTransactionId )
                {
                    throw new IOException( "Tried to apply transaction with txId=" + candidateTransactionId +
                            " but last committed txId=" + lastCommittedTxId );
                }
                else
                {
                    return false;
                }
            }
            force( ticket );
            afterForce( transactionId, hasLegacyIndexChanges );
            return result;
        }
        catch ( Exception e )
        {
            throw new TransactionAppendException( e, transactionId );
        }
    }
    
    @Override
    public void close()
    {   // do nothing
    }
}
