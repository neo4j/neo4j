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
package org.neo4j.coreedge.raft.outcome;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;

public class TruncateLogCommand implements RaftLogCommand
{
    public final long fromIndex;

    public TruncateLogCommand( long fromIndex )
    {
        this.fromIndex = fromIndex;
    }

    @Override
    public void dispatch( Handler handler ) throws IOException
    {
        handler.truncate( fromIndex );
    }

    @Override
    public void applyTo( RaftLog raftLog ) throws IOException
    {
        raftLog.truncate( fromIndex );
    }

    @Override
    public void applyTo( InFlightMap<Long,RaftLogEntry> inFlightMap ) throws IOException
    {
        long truncateIndex = fromIndex;
        while ( inFlightMap.unregister( truncateIndex ) )
        {
            truncateIndex++;
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        TruncateLogCommand that = (TruncateLogCommand) o;
        return fromIndex == that.fromIndex;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( fromIndex );
    }

    @Override
    public String toString()
    {
        return "TruncateLogCommand{" +
                "fromIndex=" + fromIndex +
                '}';
    }
}
