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

    public AbstractPhysicalTransactionAppender( LogFile logFile, TxIdGenerator txIdGenerator,
            TransactionMetadataCache transactionMetadataCache, TransactionIdStore transactionIdStore )
    {
        this.logFile = logFile;
        this.transactionIdStore = transactionIdStore;
        this.channel = logFile.getWriter();
        this.txIdGenerator = txIdGenerator;
        this.transactionMetadataCache = transactionMetadataCache;

        LogEntryWriterv1 logEntryWriter = new LogEntryWriterv1( channel, new CommandWriter( channel ) );
        this.transactionLogWriter = new TransactionLogWriter( logEntryWriter );
    }

    private void append( TransactionRepresentation transaction, long transactionId ) throws IOException
    {
        channel.getCurrentPosition( positionMarker );
        LogPosition logPosition = positionMarker.newPosition();

        transactionLogWriter.append( transaction, transactionId );

        transactionMetadataCache.cacheTransactionMetadata( transactionId, logPosition, transaction.getMasterId(),
                transaction.getAuthorId(), LogEntryStart.checksum( transaction.additionalHeader(),
                        transaction.getMasterId(), transaction.getAuthorId() ) );

        channel.emptyBufferIntoChannelAndClearIt();
    }

    @Override
    public long append( TransactionRepresentation transaction ) throws IOException
    {
        long transactionId = -1;
        long ticket;
        // Synchronized with logFile to get absolute control over concurrent rotations happening
        synchronized ( logFile )
        {
            // We put log rotation check outside the private append method since it must happen before
            // we generate the next transaction id
            logFile.checkRotation();
            transactionId = txIdGenerator.generate( transaction );
            append( transaction, transactionId );
            ticket = getCurrentTicket();
        }

        force( ticket );
        transactionIdStore.transactionCommitted( transactionId );
        return transactionId;
    }

    protected long getCurrentTicket()
    {
        return 0;
    }

    protected abstract void force( long ticket ) throws IOException;

    @Override
    public boolean append( CommittedTransactionRepresentation transaction ) throws IOException
    {
        long transactionId;
        boolean result = false;
        long ticket;
        // Synchronized with logFile to get absolute control over concurrent rotations happening
        synchronized ( logFile )
        {
            logFile.checkRotation();
            long lastCommittedTxId = transactionIdStore.getLastCommittedTransactionId();
            long candidateTransactionId = transaction.getCommitEntry().getTxId();
            if ( lastCommittedTxId + 1 == candidateTransactionId )
            {
                transactionId = txIdGenerator.generate( transaction.getTransactionRepresentation() );
                append( transaction.getTransactionRepresentation(), transactionId );
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
                // Return here straight away since we didn't actually append this transaction to the log
                // so we want to prevent the code below from running.
                return false;
            }
        }
        force( ticket );
        transactionIdStore.transactionCommitted( transactionId );
        return result;
    }

    @Override
    public void close()
    {   // do nothing
    }
}
