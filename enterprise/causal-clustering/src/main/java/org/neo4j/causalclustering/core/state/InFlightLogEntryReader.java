/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;

import static java.lang.String.format;

public class InFlightLogEntryReader implements AutoCloseable
{
    private final ReadableRaftLog raftLog;
    private final InFlightCache inFlightCache;
    private final boolean pruneAfterRead;

    private RaftLogCursor cursor;
    private boolean useCache = true;

    public InFlightLogEntryReader( ReadableRaftLog raftLog, InFlightCache inFlightCache,
            boolean pruneAfterRead )
    {
        this.raftLog = raftLog;
        this.inFlightCache = inFlightCache;
        this.pruneAfterRead = pruneAfterRead;
    }

    public RaftLogEntry get( long logIndex ) throws IOException
    {
        RaftLogEntry entry = null;

        if ( useCache )
        {
            entry = inFlightCache.get( logIndex );
        }

        if ( entry == null )
        {
            /*
             * N.B.
             * This fallback code is strictly necessary since getUsingCursor() requires
             * that entries are accessed in strictly increasing order using a single cursor.
             */
            useCache = false;
            entry = getUsingCursor( logIndex );
        }

        if ( pruneAfterRead )
        {
            inFlightCache.prune( logIndex );
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
            if ( cursor.index() != logIndex )
            {
                throw new IllegalStateException( format( "expected index %d but was %s", logIndex, cursor.index() ) );
            }
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
