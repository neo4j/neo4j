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
package org.neo4j.causalclustering.core.consensus.outcome;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.logging.Log;

import static java.lang.String.format;

public class BatchAppendLogEntries implements RaftLogCommand
{
    public final long baseIndex;
    public final int offset;
    public final RaftLogEntry[] entries;

    public BatchAppendLogEntries( long baseIndex, int offset, RaftLogEntry[] entries )
    {
        this.baseIndex = baseIndex;
        this.offset = offset;
        this.entries = entries;
    }

    @Override
    public void dispatch( Handler handler ) throws IOException
    {
        handler.append( baseIndex + offset, Arrays.copyOfRange( entries, offset, entries.length ) );
    }

    @Override
    public void applyTo( RaftLog raftLog, Log log ) throws IOException
    {
        long lastIndex = baseIndex + offset;
        if ( lastIndex <= raftLog.appendIndex() )
        {
            throw new IllegalStateException( "Attempted to append over an existing entry starting at index " + lastIndex );
        }

        raftLog.append( Arrays.copyOfRange( entries, offset, entries.length ) );
    }

    @Override
    public void applyTo( InFlightCache inFlightCache, Log log )
    {
        for ( int i = offset; i < entries.length; i++ )
        {
            inFlightCache.put( baseIndex + i , entries[i]);
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        BatchAppendLogEntries that = (BatchAppendLogEntries) o;
        return baseIndex == that.baseIndex &&
               offset == that.offset &&
               Arrays.equals( entries, that.entries );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( baseIndex, offset, entries );
    }

    @Override
    public String toString()
    {
        return format( "BatchAppendLogEntries{baseIndex=%d, offset=%d, entries=%s}", baseIndex, offset, Arrays.toString( entries ) );
    }
}
