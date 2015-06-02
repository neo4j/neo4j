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
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

public class PhysicalTransactionCursor<T extends ReadableLogChannel>
        implements IOCursor<CommittedTransactionRepresentation>
{
    private final LogEntryCursor<T> logEntryCursor;
    private final Marker<T> marker;

    private CommittedTransactionRepresentation current;
    private long lastKnownGoodPosition;

    public PhysicalTransactionCursor( T channel, LogEntryReader<T> entryReader ) throws IOException
    {
        this.marker = new Marker<>( channel );
        this.lastKnownGoodPosition = marker.currentPosition();
        this.logEntryCursor = new LogEntryCursor<>( channel, entryReader );
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
        while ( true )
        {
            if ( !logEntryCursor.next() )
            {
                return false;
            }

            LogEntry entry = logEntryCursor.get();
            if ( entry instanceof CheckPoint )
            {
                // this is a good position anyhow
                lastKnownGoodPosition = marker.currentPosition();
                continue;
            }

            assert entry instanceof LogEntryStart : "Expected Start entry, read " + entry + " instead";
            LogEntryStart startEntry = entry.as();
            LogEntryCommit commitEntry;

            List<Command> entries = commandList();
            while ( true )
            {
                if ( !logEntryCursor.next() )
                {
                    return false;
                }

                entry = logEntryCursor.get();
                if ( entry instanceof LogEntryCommit )
                {
                    commitEntry = entry.as();
                    break;
                }

                LogEntryCommand command = entry.as();
                entries.add( command.getXaCommand() );
            }

            PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( entries );
            transaction.setHeader( startEntry.getAdditionalHeader(), startEntry.getMasterId(),
                    startEntry.getLocalId(), startEntry.getTimeWritten(),
                    startEntry.getLastCommittedTxWhenTransactionStarted(), commitEntry.getTimeWritten(), -1 );
            current = new CommittedTransactionRepresentation( startEntry, transaction, commitEntry );
            lastKnownGoodPosition = marker.currentPosition();
            return true;
        }
    }

    @Override
    public void close() throws IOException
    {
        logEntryCursor.close();
    }

    public long lastKnownGoodPosition()
    {
        return lastKnownGoodPosition;
    }

    private static class Marker<T extends ReadableLogChannel>
    {
        private final LogPositionMarker marker = new LogPositionMarker();
        private final T channel;

        public Marker( T channel )
        {
            this.channel = channel;
        }

        public long currentPosition() throws IOException
        {
            return channel.getCurrentPosition( marker ).getByteOffset();
        }
    }
}
