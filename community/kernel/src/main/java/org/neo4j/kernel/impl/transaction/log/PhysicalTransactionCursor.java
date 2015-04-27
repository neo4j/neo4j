/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

public class PhysicalTransactionCursor<T extends ReadableLogChannel>
        implements IOCursor<CommittedTransactionRepresentation>
{
    private final T channel;
    private final LogEntryReader<T> entryReader;
    private CommittedTransactionRepresentation current;

    public PhysicalTransactionCursor( T channel, LogEntryReader<T> entryReader )
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

        assert entry instanceof LogEntryStart : "Expected Start entry, read " + entry + " instead";
        LogEntryStart startEntry = (LogEntryStart) entry;
        LogEntryCommit commitEntry;

        List<Command> entries = commandList();
        while ( true )
        {
            entry = entryReader.readLogEntry( channel );
            if ( entry == null )
            {
                return false;
            }
            if ( entry instanceof LogEntryCommit )
            {
                commitEntry = entry.as();
                break;
            }

            entries.add( entry.<LogEntryCommand>as().getXaCommand() );
        }

        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( entries );
        transaction.setHeader( startEntry.getAdditionalHeader(), startEntry.getMasterId(),
                startEntry.getLocalId(), startEntry.getTimeWritten(),
                startEntry.getLastCommittedTxWhenTransactionStarted(), commitEntry.getTimeWritten(), -1 );
        current = new CommittedTransactionRepresentation( startEntry, transaction, commitEntry );
        return true;
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
