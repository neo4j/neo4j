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
package org.neo4j.kernel.impl.transaction.xaframework.log.entry;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandHandler;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.WritableLogChannel;

import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryByteCodeV11111110.COMMAND;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryByteCodeV11111110.TX_1P_COMMIT;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryByteCodeV11111110.TX_START;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryVersions.CURRENT_LOG_ENTRY_VERSION;

public class LogEntryWriterv1 implements LogEntryWriter
{
    private final WritableLogChannel channel;
    private final NeoCommandHandler commandWriter;

    public LogEntryWriterv1( WritableLogChannel channel, NeoCommandHandler commandWriter )
    {
        this.channel = channel;
        this.commandWriter = commandWriter;
    }

    private void writeLogEntryHeader( byte type ) throws IOException
    {
        channel.put( CURRENT_LOG_ENTRY_VERSION ).put( type );
    }

    @Override
    public void writeStartEntry( int masterId, int authorId, long timeWritten, long latestCommittedTxWhenStarted,
            byte[] additionalHeaderData ) throws IOException
    {
        writeLogEntryHeader( TX_START );
        channel.putInt( masterId ).putInt( authorId ).putLong( timeWritten ).putLong( latestCommittedTxWhenStarted )
                .putInt( additionalHeaderData.length ).put( additionalHeaderData, additionalHeaderData.length );
    }

    @Override
    public void writeCommitEntry( long transactionId, long timeWritten ) throws IOException
    {
        writeLogEntryHeader( TX_1P_COMMIT );
        channel.putLong( transactionId ).putLong( timeWritten );
    }

    @Override
    public void serialize( TransactionRepresentation tx ) throws IOException
    {
        tx.accept( new CommandSerializer() );
    }

    public void writeCommandEntry( Command command ) throws IOException
    {
        writeLogEntryHeader( COMMAND );
        command.handle( commandWriter );
    }

    private class CommandSerializer implements Visitor<Command, IOException>
    {
        @Override
        public boolean visit( Command command ) throws IOException
        {
            writeCommandEntry( command );
            return true;
        }
    }
}
