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
package org.neo4j.kernel.impl.storemigration.legacylogs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.IdentifiableLogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;

class LogEntrySortingCursor implements IOCursor<LogEntry>
{
    private final ReadableVersionableLogChannel channel;
    private final LogEntryReader<ReadableVersionableLogChannel> reader;
    // identifier -> log entry
    private final Map<Integer, List<LogEntry>> idToEntries = new HashMap<>();

    private LogEntry toReturn;
    private int idToFetchFrom = -1;

    LogEntrySortingCursor( LogEntryReader<ReadableVersionableLogChannel> reader,
                           ReadableVersionableLogChannel channel )
    {
        this.reader = reader;
        this.channel = channel;
    }

    @Override
    public LogEntry get()
    {
        return toReturn;
    }

    @Override
    public boolean next() throws IOException
    {
        perhapsFetchEntriesFromChannel();

        if ( idToFetchFrom < 0 )
        {
            // nothing to available either from channel or from map...
            toReturn = null;
            return false;
        }

        final List<LogEntry> entries = idToEntries.get( idToFetchFrom );
        toReturn = entries.remove( 0 );
        if ( entries.isEmpty() )
        {
            idToEntries.remove( idToFetchFrom );
            idToFetchFrom = -1;
        }

        return true;
    }

    private void perhapsFetchEntriesFromChannel() throws IOException
    {
        if ( idToFetchFrom > 0 )
        {
            // we still have entry to return from the map...
            return;
        }

        LogEntry entry;
        while ( (entry = reader.readLogEntry( channel )) != null )
        {
            if ( !(entry instanceof IdentifiableLogEntry) )
            {
                throw new IllegalStateException( "reading from a log which is not a legacy one???" );
            }

            final IdentifiableLogEntry identifiableLogEntry = (IdentifiableLogEntry) entry;
            final int identifier = identifiableLogEntry.getIdentifier();
            final LogEntry inner = identifiableLogEntry.getEntry();

            List<LogEntry> list = provideList( idToEntries, identifier );
            list.add( inner );

            if ( inner instanceof LogEntryCommit )
            {
                idToFetchFrom = identifier;
                break;
            }
        }
    }

    private List<LogEntry> provideList( Map<Integer, List<LogEntry>> idToEntries, int identifier )
    {
        List<LogEntry> list = idToEntries.get( identifier );
        if ( list == null )
        {
            list = new ArrayList<>();
            idToEntries.put( identifier, list );
        }
        return list;
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
