/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.CHECK_POINT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.COMMAND;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_1P_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_START;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion.CURRENT;

public class LogEntryWriter
{
    private final WritableLogChannel channel;
    private final Visitor<Command,IOException> serializer;

    public LogEntryWriter( WritableLogChannel channel, final CommandHandler commandWriter )
    {
        this.channel = channel;
        this.serializer = new Visitor<Command,IOException>()
        {
            @Override
            public boolean visit( Command command ) throws IOException
            {
                writeLogEntryHeader( COMMAND );
                command.handle( commandWriter );
                return false;
            }
        };
    }

    private void writeLogEntryHeader( byte type ) throws IOException
    {
        channel.put( CURRENT.byteCode() ).put( type );
    }

    public void writeStartEntry( int masterId, int authorId, long timeWritten, long latestCommittedTxWhenStarted,
                                 byte[] additionalHeaderData ) throws IOException
    {
        writeLogEntryHeader( TX_START );
        channel.putInt( masterId ).putInt( authorId ).putLong( timeWritten ).putLong( latestCommittedTxWhenStarted )
               .putInt( additionalHeaderData.length ).put( additionalHeaderData, additionalHeaderData.length );
    }

    public void writeCommitEntry( long transactionId, long timeWritten ) throws IOException
    {
        writeLogEntryHeader( TX_1P_COMMIT );
        channel.putLong( transactionId ).putLong( timeWritten );
    }

    public void serialize( TransactionRepresentation tx ) throws IOException
    {
        tx.accept( serializer );
    }

    public void writeCheckPointEntry( LogPosition logPosition ) throws IOException
    {
        writeLogEntryHeader( CHECK_POINT );
        channel.putLong( logPosition.getLogVersion() ).
                putLong( logPosition.getByteOffset() );
    }
}
