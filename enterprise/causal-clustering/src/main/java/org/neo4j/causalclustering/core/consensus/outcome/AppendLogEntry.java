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
import java.util.Objects;

import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.logging.Log;

public class AppendLogEntry implements RaftLogCommand
{
    public final long index;
    public final RaftLogEntry entry;

    public AppendLogEntry( long index, RaftLogEntry entry )
    {
        this.index = index;
        this.entry = entry;
    }

    @Override
    public void applyTo( RaftLog raftLog, Log log ) throws IOException
    {
        if ( index <= raftLog.appendIndex() )
        {
            throw new IllegalStateException( "Attempted to append over an existing entry at index " + index );
        }
        raftLog.append( entry );
    }

    @Override
    public void applyTo( InFlightCache inFlightCache, Log log ) throws IOException
    {
        inFlightCache.put( index, entry );
    }

    @Override
    public void dispatch( Handler handler ) throws IOException
    {
        handler.append( index, entry );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        AppendLogEntry that = (AppendLogEntry) o;
        return index == that.index &&
               Objects.equals( entry, that.entry );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( index, entry );
    }

    @Override
    public String toString()
    {
        return "AppendLogEntry{" +
                "index=" + index +
                ", entry=" + entry +
                '}';
    }
}
