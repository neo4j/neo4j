/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.StorageCommand;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.CHECK_POINT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.COMMAND;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_START;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion.CURRENT;

public class LogEntryWriter
{
    private final FlushableChannel channel;
    private final Visitor<StorageCommand,IOException> serializer;

    /**
     * Create a writer that uses {@link LogEntryVersion#CURRENT} for versioning.
     * @param channel underlying channel
     */
    public LogEntryWriter( FlushableChannel channel )
    {
        this.channel = channel;
        this.serializer = new StorageCommandSerializer( channel );
    }

    protected void writeLogEntryHeader( byte type ) throws IOException
    {
        channel.put( CURRENT.byteCode() ).put( type );
    }

    public void writeStartEntry( LogEntryStart entry ) throws IOException
    {
        writeStartEntry( entry.getMasterId(), entry.getLocalId(), entry.getTimeWritten(), entry.getLastCommittedTxWhenTransactionStarted(),
                entry.getAdditionalHeader() );
    }

    public void writeStartEntry( int masterId, int authorId, long timeWritten, long latestCommittedTxWhenStarted,
                                 byte[] additionalHeaderData ) throws IOException
    {
        writeLogEntryHeader( TX_START );
        channel.putInt( masterId ).putInt( authorId ).putLong( timeWritten ).putLong( latestCommittedTxWhenStarted )
               .putInt( additionalHeaderData.length ).put( additionalHeaderData, additionalHeaderData.length );
    }

    public void writeCommitEntry( LogEntryCommit entry ) throws IOException
    {
        writeCommitEntry( entry.getTxId(), entry.getTimeWritten() );
    }

    public void writeCommitEntry( long transactionId, long timeWritten ) throws IOException
    {
        writeLogEntryHeader( TX_COMMIT );
        channel.putLong( transactionId ).putLong( timeWritten );
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

    public void writeCheckPointEntry( LogPosition logPosition ) throws IOException
    {
        writeLogEntryHeader( CHECK_POINT );
        channel.putLong( logPosition.getLogVersion() ).
                putLong( logPosition.getByteOffset() );
    }

    private class StorageCommandSerializer implements Visitor<StorageCommand,IOException>
    {
        private final FlushableChannel channel;

        StorageCommandSerializer( FlushableChannel channel )
        {
            this.channel = channel;
        }

        @Override
        public boolean visit( StorageCommand command ) throws IOException
        {
            writeLogEntryHeader( COMMAND );
            command.serialize( channel );
            return false;
        }
    }
}
