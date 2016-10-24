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
package org.neo4j.coreedge.core.consensus.log;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * In-memory raft log with a fixed maximum length, backed by an array.
 *
 * When entries are appended that would exceed this log's capacity, it discards old entries, with behaviour equivalent
 * to {@link #prune(long)}.
 * This behaviour is suitable for caching the suffix of a RaftLog, where all entries are duplicated in more durable
 * underlying raft log. It is therefore fine to prune this log aggressively and for it to prune itself, because any
 * discarded entries will still be present in the underlying log.
 */
public class BoundedArrayBackedRaftLog implements RaftLog
{
    private final RaftLogEntry[] array;

    private long prevIndex = -1;
    private long prevTerm = -1;

    private long appendIndex = -1;
    private long commitIndex = -1;
    private long term = -1;

    public BoundedArrayBackedRaftLog( int maxLength )
    {
        array = new RaftLogEntry[maxLength];
    }

    @Override
    public synchronized long append( RaftLogEntry... entries ) throws IOException
    {
        long newAppendIndex = appendIndex;
        for ( RaftLogEntry entry : entries )
        {
            newAppendIndex = appendSingle( entry );
        }
        return newAppendIndex;
    }

    private synchronized long appendSingle( RaftLogEntry logEntry ) throws IOException
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

        if ( appendIndex > prevIndex && offset( prevIndex + 1 ) == offset( appendIndex + 1 ) )
        {
            prune( prevIndex + 1 );
        }
        appendIndex++;
        array[offset( appendIndex )] = logEntry;
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
                array[offset( removeIndex )] = null;
                removeIndex++;
            } while ( removeIndex <= safeIndex );
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
        return array[offset( logIndex )].term();
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
            throw new IllegalArgumentException( "Cannot truncate at index " + fromIndex + " when append index is " +
                    appendIndex );
        }
        else if ( fromIndex <= prevIndex )
        {
            prevIndex = -1;
            prevTerm = -1;
        }

        for ( long i = appendIndex; i >= fromIndex; --i )
        {
            array[offset( i )] = null;
        }

        if ( appendIndex >= fromIndex )
        {
            appendIndex = fromIndex - 1;
        }
        term = readEntryTerm( appendIndex );
    }

    @Override
    public synchronized RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
    {
        return new RaftLogCursor()
        {
            private long currentIndex = fromIndex - 1; // the cursor starts "before" the first entry
            RaftLogEntry current = null;

            @Override
            public boolean next() throws IOException
            {
                currentIndex++;
                boolean hasNext;

                synchronized ( BoundedArrayBackedRaftLog.this )
                {
                    hasNext = currentIndex <= appendIndex;
                    if ( hasNext )
                    {
                        if ( currentIndex <= prevIndex || currentIndex > appendIndex )
                        {
                            return false;
                        }
                        current = array[offset( currentIndex )];
                    }
                    else
                    {
                        current = null;
                    }
                }
                return hasNext;
            }

            @Override
            public void close() throws IOException
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
            Arrays.setAll( array, i -> null );

            appendIndex = index;
            prevIndex = index;
            prevTerm = term;
        }
        return appendIndex;
    }

    private int offset( long logIndex )
    {
        return (int) (logIndex % array.length);
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
        BoundedArrayBackedRaftLog that = (BoundedArrayBackedRaftLog) o;
        return prevIndex == that.prevIndex &&
                prevTerm == that.prevTerm &&
                appendIndex == that.appendIndex &&
                commitIndex == that.commitIndex &&
                term == that.term &&
                Arrays.equals( array, that.array );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( array, prevIndex, prevTerm, appendIndex, commitIndex, term );
    }
}
