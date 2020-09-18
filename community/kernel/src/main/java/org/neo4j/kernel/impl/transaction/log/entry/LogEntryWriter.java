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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.StorageCommand;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.COMMAND;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.LEGACY_CHECK_POINT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

public class LogEntryWriter<T extends WritableChecksumChannel>
{
    private final Visitor<StorageCommand,IOException> serializer;
    protected final T channel;
    private final byte parserSetVersion;

    public LogEntryWriter( T channel, LogEntryParserSet parserSet )
    {
        this.channel = channel;
        this.parserSetVersion = parserSet.versionByte();
        this.serializer = new StorageCommandSerializer( channel, this );
    }

    public void writeLogEntryHeader( byte type, WritableChannel channel ) throws IOException
    {
        channel.put( parserSetVersion ).put( type );
    }

    private void writeStartEntry( LogEntryStart entry ) throws IOException
    {
        writeStartEntry( entry.getTimeWritten(), entry.getLastCommittedTxWhenTransactionStarted(),
                entry.getPreviousChecksum(), entry.getAdditionalHeader() );
    }

    public void writeStartEntry( long timeWritten, long latestCommittedTxWhenStarted,
            int previousChecksum, byte[] additionalHeaderData ) throws IOException
    {
        channel.beginChecksum();
        writeLogEntryHeader( TX_START, channel );
        channel.putLong( timeWritten )
                .putLong( latestCommittedTxWhenStarted )
                .putInt( previousChecksum )
                .putInt( additionalHeaderData.length )
                .put( additionalHeaderData, additionalHeaderData.length );
    }

    private void writeCommitEntry( LogEntryCommit entry ) throws IOException
    {
        writeCommitEntry( entry.getTxId(), entry.getTimeWritten() );
    }

    public int writeCommitEntry( long transactionId, long timeWritten ) throws IOException
    {
        writeLogEntryHeader( TX_COMMIT, channel );
        channel.putLong( transactionId )
                .putLong( timeWritten );
        return channel.putChecksum();
    }

    public void serialize( TransactionRepresentation tx ) throws IOException
    {
        tx.accept( serializer );
    }

    public void serialize( CommittedTransactionRepresentation tx ) throws IOException
    {
        writeStartEntry( tx.getStartEntry() );
        serialize( tx.getTransactionRepresentation() );
        writeCommitEntry( tx.getCommitEntry() );
    }

    public void serialize( Collection<StorageCommand> commands ) throws IOException
    {
        for ( StorageCommand command : commands )
        {
            serializer.visit( command );
        }
    }

    public void serialize( StorageCommand command ) throws IOException
    {
        serializer.visit( command );
    }

    public void writeLegacyCheckPointEntry( LogPosition logPosition ) throws IOException
    {
        channel.beginChecksum();
        writeLogEntryHeader( LEGACY_CHECK_POINT, channel );
        channel.putLong( logPosition.getLogVersion() )
                .putLong( logPosition.getByteOffset() );
        channel.putChecksum();
    }

    public T getChannel()
    {
        return channel;
    }

    private static class StorageCommandSerializer implements Visitor<StorageCommand,IOException>
    {
        private final WritableChannel channel;
        private final LogEntryWriter entryWriter;

        StorageCommandSerializer( WritableChannel channel, LogEntryWriter entryWriter )
        {
            this.channel = channel;
            this.entryWriter = entryWriter;
        }

        @Override
        public boolean visit( StorageCommand command ) throws IOException
        {
            entryWriter.writeLogEntryHeader( COMMAND, channel );
            command.serialize( channel );
            return false;
        }
    }
}
