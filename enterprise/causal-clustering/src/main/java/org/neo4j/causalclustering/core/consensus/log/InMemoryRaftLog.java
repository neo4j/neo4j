/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class InMemoryRaftLog implements RaftLog
{
    private final Map<Long, RaftLogEntry> raftLog = new HashMap<>();

    private long prevIndex = -1;
    private long prevTerm = -1;

    private long appendIndex = -1;
    private long commitIndex = -1;
    private long term = -1;

    @Override
    public synchronized long append( RaftLogEntry... entries )
    {
        long newAppendIndex = appendIndex;
        for ( RaftLogEntry entry : entries )
        {
            newAppendIndex = appendSingle( entry );
        }
        return newAppendIndex;
    }

    private synchronized long appendSingle( RaftLogEntry logEntry )
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
    public synchronized long prune( long safeIndex )
    {
        if ( safeIndex > prevIndex )
        {
            long removeIndex = prevIndex + 1;

            prevTerm = readEntryTerm( safeIndex );
            prevIndex = safeIndex;

            do
            {
                raftLog.remove( removeIndex );
                removeIndex++;
            }
            while ( removeIndex <= safeIndex );
        }

        return prevIndex;
    }

    @Override
    public synchronized long appendIndex()
    {
        return appendIndex;
    }

    @Override
    public synchronized long prevIndex()
    {
        return prevIndex;
    }

    @Override
    public synchronized long readEntryTerm( long logIndex )
    {
        if ( logIndex == prevIndex )
        {
            return prevTerm;
        }
        else if ( logIndex < prevIndex || logIndex > appendIndex )
        {
            return -1;
        }
        return raftLog.get( logIndex ).term();
    }

    @Override
    public synchronized void truncate( long fromIndex )
    {
        if ( fromIndex <= commitIndex )
        {
            throw new IllegalArgumentException( "cannot truncate before the commit index" );
        }
        else if ( fromIndex > appendIndex )
        {
            throw new IllegalArgumentException(
                    "Cannot truncate at index " + fromIndex + " when append index is " + appendIndex );
        }
        else if ( fromIndex <= prevIndex )
        {
            prevIndex = -1;
            prevTerm = -1;
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
    public synchronized RaftLogCursor getEntryCursor( long fromIndex )
    {
        return new RaftLogCursor()
        {
            private long currentIndex = fromIndex - 1; // the cursor starts "before" the first entry
            RaftLogEntry current;

            @Override
            public boolean next()
            {
                currentIndex++;
                boolean hasNext;

                synchronized ( InMemoryRaftLog.this )
                {
                    hasNext = currentIndex <= appendIndex;
                    if ( hasNext )
                    {
                        if ( currentIndex <= prevIndex || currentIndex > appendIndex )
                        {
                            return false;
                        }
                        current = raftLog.get( currentIndex );
                    }
                    else
                    {
                        current = null;
                    }
                }
                return hasNext;
            }

            @Override
            public void close()
            {
            }

            @Override
            public long index()
            {
                return currentIndex;
            }

            @Override
            public RaftLogEntry get()
            {
                return current;
            }
        };
    }

    @Override
    public synchronized long skip( long index, long term )
    {
        if ( index > appendIndex )
        {
            raftLog.clear();

            appendIndex = index;
            prevIndex = index;
            prevTerm = term;
        }
        return appendIndex;
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
