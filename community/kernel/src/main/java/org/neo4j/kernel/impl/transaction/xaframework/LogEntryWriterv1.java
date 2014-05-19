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
import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.LabelTokenCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NeoStoreCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.PropertyKeyTokenCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipTypeTokenCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandVisitor;

public class LogEntryWriterv1 implements LogEntryWriter
{
    private static final short CURRENT_FORMAT_VERSION = ( LogEntry.CURRENT_LOG_VERSION) & 0xFF;
    static final int LOG_HEADER_SIZE = 16;
    private final WritableLogChannel channel;
    private final NeoCommandVisitor commandWriter;

    public LogEntryWriterv1( WritableLogChannel channel, NeoCommandVisitor commandWriter )
    {
        this.channel = channel;
        this.commandWriter = commandWriter;
    }

    public static ByteBuffer writeLogHeader( ByteBuffer buffer, long logVersion,
            long previousCommittedTxId )
    {
        buffer.clear();
        buffer.putLong( logVersion | ( ( (long) CURRENT_FORMAT_VERSION ) << 56 ) );
        buffer.putLong( previousCommittedTxId );
        buffer.flip();
        return buffer;
    }

    private void writeLogEntryHeader( byte type ) throws IOException
    {
        channel.put( LogEntry.CURRENT_LOG_ENTRY_VERSION ).put( type );
    }

    @Override
    public void writeStartEntry( int masterId, int authorId, long timeWritten, long latestCommittedTxWhenStarted,
            byte[] additionalHeaderData ) throws IOException
    {
        writeLogEntryHeader( LogEntry.TX_START );
        channel.putInt( masterId ).putInt( authorId ).putLong( timeWritten ).putLong( latestCommittedTxWhenStarted )
               .putInt( additionalHeaderData.length ).put( additionalHeaderData, additionalHeaderData.length );
    }

    @Override
    public void writeCommitEntry( long transactionId, long timeWritten ) throws IOException
    {
        writeLogEntryHeader( LogEntry.TX_1P_COMMIT );
        channel.putLong( transactionId ).putLong( timeWritten );
    }

    @Override
    public void writeCommandEntry( Command command ) throws IOException
    {
        writeLogEntryHeader( LogEntry.COMMAND );
        command.accept( commandWriter );
    }

    @Override
    public boolean visitNodeCommand( NodeCommand command ) throws IOException
    {
        writeCommandEntry( command );
        return true;
    }

    @Override
    public boolean visitRelationshipCommand( RelationshipCommand command ) throws IOException
    {
        writeCommandEntry( command );
        return true;
    }

    @Override
    public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
    {
        writeCommandEntry( command );
        return true;
    }

    @Override
    public boolean visitRelationshipGroupCommand( RelationshipGroupCommand command ) throws IOException
    {
        writeCommandEntry( command );
        return true;
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( RelationshipTypeTokenCommand command ) throws IOException
    {
        writeCommandEntry( command );
        return true;
    }

    @Override
    public boolean visitLabelTokenCommand( LabelTokenCommand command ) throws IOException
    {
        writeCommandEntry( command );
        return true;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( PropertyKeyTokenCommand command ) throws IOException
    {
        writeCommandEntry( command );
        return true;
    }

    @Override
    public boolean visitSchemaRuleCommand( SchemaRuleCommand command ) throws IOException
    {
        writeCommandEntry( command );
        return true;
    }

    @Override
    public boolean visitNeoStoreCommand( NeoStoreCommand command ) throws IOException
    {
        writeCommandEntry( command );
        return true;
    }
}
