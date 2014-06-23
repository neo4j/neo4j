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

public class PhysicalTransactionAppender implements TransactionAppender
{
    private final WritableLogChannel channel;
    private final TxIdGenerator txIdGenerator;
    private final LogEntryWriter logEntryWriter;
    private final TransactionMetadataCache transactionMetadataCache;
    private final LogFile logFile;

    public PhysicalTransactionAppender( LogFile logFile, TxIdGenerator txIdGenerator,
                                        TransactionMetadataCache transactionMetadataCache )
    {
        this.logFile = logFile;
        this.channel = logFile.getWriter();
        this.txIdGenerator = txIdGenerator;
        this.transactionMetadataCache = transactionMetadataCache;
        this.logEntryWriter = new LogEntryWriterv1( channel, new CommandWriter( channel ) );
    }

    @Override
    public synchronized Future<Long> append( TransactionRepresentation transaction ) throws IOException
    {
        logFile.checkRotation();

        // Write start record
        LogPosition logPosition = channel.getCurrentPosition();
        logEntryWriter.writeStartEntry( transaction.getMasterId(), transaction.getAuthorId(),
                transaction.getTimeWritten(), transaction.getLatestCommittedTxWhenStarted(),
                transaction.additionalHeader() );

        // Write all the commands to the log channel
        logEntryWriter.serialize( transaction );

        // Write commit record
        long transactionId = txIdGenerator.generate( transaction );
        logEntryWriter.writeCommitEntry( transactionId, transaction.getTimeWritten() );
        transactionMetadataCache.cacheTransactionMetadata( transactionId, logPosition, transaction.getMasterId(),
                transaction.getAuthorId(), LogEntry.Start.checksum( transaction.additionalHeader(),
                        transaction.getMasterId(), transaction.getAuthorId() ) );

        // force
        channel.force();

        return FutureAdapter.present( transactionId );
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
