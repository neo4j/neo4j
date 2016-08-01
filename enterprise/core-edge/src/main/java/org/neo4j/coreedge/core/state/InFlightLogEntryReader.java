/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.core.state;

import java.io.IOException;

import org.neo4j.coreedge.core.consensus.log.RaftLogCursor;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;
import org.neo4j.coreedge.core.consensus.log.ReadableRaftLog;
import org.neo4j.coreedge.core.consensus.log.segmented.InFlightMap;

import static java.lang.String.format;

public class InFlightLogEntryReader implements AutoCloseable
{
    private final ReadableRaftLog raftLog;
    private final InFlightMap<Long, RaftLogEntry> inFlightMap;
    private final boolean removeReadIndexFromMap;

    private RaftLogCursor cursor;
    private boolean useInFlightMap = true;

    public InFlightLogEntryReader( ReadableRaftLog raftLog, InFlightMap<Long,RaftLogEntry> inFlightMap,
            boolean removeReadIndexFromMap )
    {
        this.raftLog = raftLog;
        this.inFlightMap = inFlightMap;
        this.removeReadIndexFromMap = removeReadIndexFromMap;
    }

    public RaftLogEntry get( long logIndex ) throws IOException
    {
        RaftLogEntry entry = null;

        if ( useInFlightMap )
        {
            entry = inFlightMap.retrieve( logIndex );
        }

        if ( entry == null )
        {
            useInFlightMap = false;
            entry = getUsingCursor( logIndex );
        }

        if ( removeReadIndexFromMap )
        {
            inFlightMap.unregister( logIndex );
        }

        return entry;
    }

    private RaftLogEntry getUsingCursor( long logIndex ) throws IOException
    {
        if ( cursor == null )
        {
            cursor = raftLog.getEntryCursor( logIndex );
        }

        if ( cursor.next() )
        {
            assert cursor.index() == logIndex : format( "expected index %d but was %s", logIndex, cursor.index() );
            return cursor.get();
        }
        else
        {
            return null;
        }
    }

    @Override
    public void close() throws IOException
    {
        if ( cursor != null )
        {
            cursor.close();
        }
    }
}
