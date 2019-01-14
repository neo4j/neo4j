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


import java.util.Objects;
import java.util.function.LongPredicate;

import org.neo4j.helpers.collection.LruCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

public class RaftLogMetadataCache
{
    private final LruCache<Long /*tx id*/, RaftLogEntryMetadata> raftLogEntryCache;

    public RaftLogMetadataCache( int logEntryCacheSize )
    {
        this.raftLogEntryCache = new LruCache<>( "Raft log entry cache", logEntryCacheSize );
    }

    public void clear()
    {
        raftLogEntryCache.clear();
    }

    /**
     * Returns the metadata for the entry at position {@param logIndex}, null if the metadata is not present in the cache
     */
    public RaftLogEntryMetadata getMetadata( long logIndex )
    {
        return raftLogEntryCache.get( logIndex );
    }

    public RaftLogEntryMetadata cacheMetadata( long logIndex, long entryTerm, LogPosition position )
    {
        RaftLogEntryMetadata result = new RaftLogEntryMetadata( entryTerm, position );
        raftLogEntryCache.put( logIndex, result );
        return result;
    }

    public void removeUpTo( long upTo )
    {
        remove( key -> key <= upTo );
    }

    public void removeUpwardsFrom( long startingFrom )
    {
        remove( key -> key >= startingFrom );
    }

    private void remove( LongPredicate predicate )
    {
        raftLogEntryCache.keySet().removeIf( predicate::test );
    }

    public static class RaftLogEntryMetadata
    {
        private final long entryTerm;
        private final LogPosition startPosition;

        public RaftLogEntryMetadata( long entryTerm, LogPosition startPosition )
        {
            Objects.requireNonNull( startPosition );
            this.entryTerm = entryTerm;
            this.startPosition = startPosition;
        }

        public long getEntryTerm()
        {
            return entryTerm;
        }

        public LogPosition getStartPosition()
        {
            return startPosition;
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

            RaftLogEntryMetadata that = (RaftLogEntryMetadata) o;

            if ( entryTerm != that.entryTerm )
            {
                return false;
            }
            return startPosition.equals( that.startPosition );

        }

        @Override
        public int hashCode()
        {
            int result = (int) (entryTerm ^ (entryTerm >>> 32));
            result = 31 * result + startPosition.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return "RaftLogEntryMetadata{" +
                    "entryTerm=" + entryTerm +
                    ", startPosition=" + startPosition +
                    '}';
        }
    }
}
