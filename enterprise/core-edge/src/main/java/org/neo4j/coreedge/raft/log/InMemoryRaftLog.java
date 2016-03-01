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
package org.neo4j.coreedge.raft.log;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.neo4j.cursor.IOCursor;

public class InMemoryRaftLog implements RaftLog
{
    private final Map<Long, RaftLogEntry> raftLog = new HashMap<>();

    private long appendIndex = -1;
    private long commitIndex = -1;
    private long term = -1;

    @Override
    public long append( RaftLogEntry logEntry ) throws IOException
    {
        Objects.requireNonNull( logEntry );
        if ( logEntry.term() >= term )
        {
            term = logEntry.term();
        }
        else
        {
            throw new IllegalStateException( String.format( "Non-monotonic term %d for in entry %s in term %d",
                    logEntry.term(), logEntry.toString(), term ) );
        }

        appendIndex++;
        raftLog.put( appendIndex, logEntry );
        return appendIndex;
    }

    @Override
    public void commit( long commitIndex )
    {
        if ( commitIndex > appendIndex )
        {
            commitIndex = appendIndex;
        }
        this.commitIndex = commitIndex;
    }

    @Override
    public long appendIndex()
    {
        return appendIndex;
    }

    @Override
    public long commitIndex()
    {
        return commitIndex;
    }

    @Override
    public RaftLogEntry readLogEntry( long logIndex )
    {
        if ( logIndex < 0 )
        {
            throw new IllegalArgumentException( "logIndex must not be negative" );
        }
        if ( logIndex > appendIndex )
        {
            throw new IllegalArgumentException(
                    String.format( "cannot read past last appended index (lastAppended=%d, readIndex=%d)",
                            appendIndex, logIndex ) );
        }
        return raftLog.get( logIndex );
    }

    @Override
    public long readEntryTerm( long logIndex )
    {
        if ( logIndex < 0 || logIndex > appendIndex )
        {
            return -1;
        }
        return readLogEntry( logIndex ).term();
    }

    @Override
    public synchronized void truncate( long fromIndex )
    {
        if ( fromIndex <= commitIndex )
        {
            throw new IllegalArgumentException( "cannot truncate before the commit index" );
        }

        for ( long i = appendIndex; i >= fromIndex; --i )
        {
            raftLog.remove( i );
        }

        if ( appendIndex >= fromIndex )
        {
            appendIndex = fromIndex - 1;
        }
        term = readEntryTerm( appendIndex );
    }

    @Override
    public boolean entryExists( long logIndex )
    {
        return raftLog.containsKey( logIndex );
    }

    @Override
    public IOCursor<RaftLogEntry> getEntryCursor( long fromIndex ) throws IOException
    {
        return new IOCursor<RaftLogEntry>()
        {
            private long currentIndex = fromIndex - 1; // the cursor starts "before" the first entry

            @Override
            public boolean next() throws IOException
            {
                currentIndex++;
                return currentIndex <= appendIndex;
            }

            @Override
            public void close() throws IOException
            {
            }

            @Override
            public RaftLogEntry get()
            {
                return ( currentIndex <= appendIndex ) ? readLogEntry( currentIndex ) : null;
            }
        };
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        InMemoryRaftLog that = (InMemoryRaftLog) o;
        return Objects.equals( appendIndex, that.appendIndex ) &&
                Objects.equals( commitIndex, that.commitIndex ) &&
                Objects.equals( term, that.term ) &&
                Objects.equals( raftLog, that.raftLog );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( raftLog, appendIndex, commitIndex, term );
    }
}
