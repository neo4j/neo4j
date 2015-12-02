/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Objects;

import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftStorageException;

import static java.lang.String.format;

public class BatchAppendLogEntries implements LogCommand
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
    public void applyTo( RaftLog raftLog ) throws RaftStorageException
    {
        if ( raftLog.entryExists( baseIndex + offset ) )
        {
            throw new IllegalStateException( "Attempted to append over an existing entry starting at index " + baseIndex + offset );
        }

        for ( int i = offset; i < entries.length; i++ )
        {
            raftLog.append( entries[i] );
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
