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
import java.util.concurrent.Future;

import org.neo4j.helpers.FutureAdapter;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;

public class PhysicalTransactionAppender implements TransactionAppender
{
    private final WritableLogChannel channel;
    private final TxIdGenerator txIdGenerator;
    private final TransactionMetadataCache transactionMetadataCache;
    private final LogFile logFile;
    private final TransactionIdStore transactionIdStore;
    private final TransactionLogWriter transactionLogWriter;
    private final LogPositionMarker positionMarker = new LogPositionMarker();

    public PhysicalTransactionAppender( LogFile logFile, TxIdGenerator txIdGenerator,
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

    private void append( TransactionRepresentation transaction,
            long transactionId ) throws IOException
    {
        channel.getCurrentPosition( positionMarker );
        LogPosition logPosition = positionMarker.newPosition();

        transactionLogWriter.append( transaction, transactionId );

        transactionMetadataCache.cacheTransactionMetadata( transactionId, logPosition, transaction.getMasterId(),
                transaction.getAuthorId(), LogEntry.Start.checksum( transaction.additionalHeader(),
                        transaction.getMasterId(), transaction.getAuthorId() ) );

        channel.force();
    }

    @Override
    public synchronized Future<Long> append( TransactionRepresentation transaction ) throws IOException
    {
        // We put log rotation check outside the private append method since it must happen before
        // we generate the next transaction id
        logFile.checkRotation();
        long transactionId = txIdGenerator.generate( transaction );
        append( transaction, transactionId );
        return FutureAdapter.present( transactionId );
    }

    @Override
    public synchronized boolean append( CommittedTransactionRepresentation transaction )
            throws IOException
    {
        logFile.checkRotation();
        long txId = transaction.getCommitEntry().getTxId();
        long lastCommittedTxId = transactionIdStore.getLastCommittingTransactionId();
        if ( lastCommittedTxId + 1 == txId )
        {
            txIdGenerator.generate( transaction.getTransactionRepresentation() );
            append( transaction.getTransactionRepresentation(), txId );
            return true;
        }
        else if ( lastCommittedTxId + 1 < txId )
        {
            throw new IOException( "Tried to apply transaction with txId=" + txId +
                    " but last committed txId=" + lastCommittedTxId );
        }
        return false;
    }
}
