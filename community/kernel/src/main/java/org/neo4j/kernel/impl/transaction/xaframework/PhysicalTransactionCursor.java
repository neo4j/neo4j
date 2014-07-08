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
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.nioneo.xa.command.Command;

public class PhysicalTransactionCursor implements IOCursor<CommittedTransactionRepresentation>
{
    private final ReadableLogChannel channel;
    private final LogEntryReader<ReadableLogChannel> entryReader;
    private CommittedTransactionRepresentation current;

    public PhysicalTransactionCursor( ReadableLogChannel channel, LogEntryReader<ReadableLogChannel> entryReader)
    {
        this.channel = channel;
        this.entryReader = entryReader;
    }

    protected List<Command> commandList()
    {
        return new ArrayList<>();
    }

    @Override
    public CommittedTransactionRepresentation get()
    {
        return current;
    }

    @Override
    public boolean next() throws IOException
    {
        LogEntry entry = entryReader.readLogEntry( channel );
        if ( entry == null )
        {
            return false;
        }

        assert entry instanceof LogEntry.Start : "Expected Start entry, read " + entry + " instead";
        LogEntry.Start startEntry = (LogEntry.Start) entry;
        LogEntry.Commit commitEntry;

        List<Command> entries = commandList();
        while ( true )
        {
            entry = entryReader.readLogEntry( channel );
            if ( entry == null )
            {
                return false;
            }
            if ( entry instanceof LogEntry.Commit )
            {
                commitEntry = (LogEntry.Commit) entry;
                break;
            }

            entries.add( ((LogEntry.Command) entry).getXaCommand() );
        }

        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( entries );
        transaction.setHeader( startEntry.getAdditionalHeader(), startEntry.getMasterId(),
                startEntry.getLocalId(), startEntry.getTimeWritten(),
                startEntry.getLastCommittedTxWhenTransactionStarted() );
        current = new CommittedTransactionRepresentation( startEntry, transaction, commitEntry );
        return true;
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
