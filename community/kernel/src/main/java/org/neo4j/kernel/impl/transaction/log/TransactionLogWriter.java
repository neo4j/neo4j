/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;

import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.util.VisibleForTesting;

public class TransactionLogWriter
{
    private final FlushablePositionAwareChecksumChannel channel;
    private final LogEntryWriterFactory logEntryWriterFactory;

    public TransactionLogWriter( FlushablePositionAwareChecksumChannel channel, LogEntryWriterFactory logEntryWriterFactory )
    {
        this.channel = channel;
        this.logEntryWriterFactory = logEntryWriterFactory;
    }

    /**
     * Append a transaction to the transaction log file
     * @return checksum of the transaction
     */
    public int append( TransactionRepresentation transaction, long transactionId, int previousChecksum ) throws IOException
    {
        LogEntryWriter<FlushablePositionAwareChecksumChannel> writer = logEntryWriterFactory.createEntryWriter( channel );
        writer.writeStartEntry( transaction.getTimeStarted(), transaction.getLatestCommittedTxWhenStarted(), previousChecksum, transaction.additionalHeader() );

        // Write all the commands to the log channel
        writer.serialize( transaction );

        // Write commit record
        return writer.writeCommitEntry( transactionId, transaction.getTimeCommitted() );
    }

    @VisibleForTesting
    public void legacyCheckPoint( LogPosition logPosition ) throws IOException
    {
        LogEntryWriter<FlushablePositionAwareChecksumChannel> writer = logEntryWriterFactory.createEntryWriter( channel );
        writer.writeLegacyCheckPointEntry( logPosition );
    }

    public LogPosition getCurrentPosition() throws IOException
    {
        return channel.getCurrentPosition();
    }

    public LogPositionMarker getCurrentPosition( LogPositionMarker logPositionMarker ) throws IOException
    {
        return channel.getCurrentPosition( logPositionMarker );
    }

    @VisibleForTesting
    public FlushablePositionAwareChecksumChannel getChannel()
    {
        return channel;
    }

    @VisibleForTesting
    public LogEntryWriter<FlushablePositionAwareChecksumChannel> getWriter()
    {
        return logEntryWriterFactory.createEntryWriter( channel );
    }
}
