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
package org.neo4j.coreedge.raft.log.physical;

import java.util.LinkedList;

import static java.lang.String.format;

/**
 * This data structure maintains a mapping from a log index value to the log version that contains it. This is
 * maintained while entries are appended, log files are pruned and logs are truncated. To achieve this, there are
 * two update methods - {@link #add(long, long)} for notifying of log creation and {@link #pruneVersion(long)} for
 * notification of log pruning.
 *
 * The main query method is {@link #versionForIndex(long)} which returns the log version which contains the specified
 * entry index. An abstraction to keep in mind is that the last log version is assumed to contain all entries
 * greater than the prevIndex argument of the {@link #add(long, long)} call with the largest version argument so far.
 * Practically, this means that asking for a log index that is beyond the current append index will return the current
 * log version.
 *
 * A special word for truncations. They are communicated by calling {@link #add(long, long)} with the prevIndex at one
 * less than the truncate index. This will update the previous version with the proper upper index. In the case where
 * this process would leave one or more versions empty (for example, the prevIndex is less than the prevIndex with which
 * a previous version was added) that version will be removed since logically is has been truncated away entirely.
 */
public class VersionIndexRanges
{
    /*
     * This list is treated like a deque with the head (first entry, small list indexes) holding the lowest log
     * index and the tail (last entry, highest list index) being the latest addition (i.e. largest log index).
     */
    private final LinkedList<VersionIndexRange> ranges = new LinkedList<>();

    public void add( long version, long prevIndex )
    {
        // Ensure strictly monotonic additions
        if ( !ranges.isEmpty() && ranges.peekLast().version >= version )
        {
            throw new IllegalArgumentException( format( "Cannot accept range for version %d while having " +
                    "already accepted %d", version, ranges.peek().version ) );
        }
        // Update the upper index of the stack's head, or completely remove it if it's been truncated away entirely
        while ( !ranges.isEmpty() )
        {
            VersionIndexRange range = ranges.peekLast();
            if ( range.prevIndex >= prevIndex )
            {
                ranges.removeLast();
            }
            else
            {
                range.endAt( prevIndex );
                break;
            }
        }
        // Finally, add the new range at the stack's top
        ranges.add( new VersionIndexRange( version, prevIndex ) );
    }

    public void pruneVersion( long version )
    {
        // Keep removing at the queue's head until we reach the specified version
        while( !ranges.isEmpty() && ranges.getFirst().version <= version )
        {
            ranges.removeFirst();
        }
    }

    public VersionIndexRange versionForIndex( long index )
    {
        // Start at the stack's head, keep going backwards, since we assume most queries will be for recent entries
        for ( int i = ranges.size() - 1; i >= 0; i-- )
        {
            VersionIndexRange range = ranges.get( i );
            if ( range.includes( index ) )
            {
                return range;
            }
        }
        return VersionIndexRange.OUT_OF_RANGE;
    }

    @Override
    public String toString()
    {
        return format( "RaftLogVersionRanges{ranges=%s}", ranges );
    }

    public long highestVersion()
    {
        return ranges.isEmpty() ? 0 : Math.max(0, ranges.peekLast().version);
    }

    public long lowestVersion()
    {
        return ranges.isEmpty() ? 0 :  Math.max(0, ranges.peekFirst().version);
    }
}
