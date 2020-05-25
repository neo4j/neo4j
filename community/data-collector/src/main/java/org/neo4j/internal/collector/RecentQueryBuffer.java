/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.collector;

import java.util.function.Consumer;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * Bounded buffer containing meta data about the most recent query invocations across the dbms.
 */
public class RecentQueryBuffer
{
    private final RingRecentBuffer<TruncatedQuerySnapshot> queries;
    private final MemoryTracker memoryTracker;

    private static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( RecentQueryBuffer.class ) +
                                             HeapEstimator.shallowSizeOfInstance( Consumer.class );

    public RecentQueryBuffer( int maxRecentQueryCount, MemoryTracker memoryTracker )
    {
        this.memoryTracker = memoryTracker;
        // Round down to the nearest power of 2
        int queryBufferSize = Integer.highestOneBit( maxRecentQueryCount );
        queries = new RingRecentBuffer<>( queryBufferSize, discarded -> memoryTracker.releaseHeap( discarded.estimatedHeap ) );
        memoryTracker.allocateHeap( queries.estimatedHeapUsage() + SHALLOW_SIZE );
    }

    public long numSilentQueryDrops()
    {
        return queries.numSilentQueryDrops();
    }

    /**
     * Produce a new query into the buffer.
     */
    public void produce( TruncatedQuerySnapshot query )
    {
        Preconditions.checkArgument( query.databaseId != null,
                                     "Only queries targeting a specific database are expected in the recent query buffer." );

        memoryTracker.allocateHeap( query.estimatedHeap );
        queries.produce( query );
    }

    /**
     * Clear all query meta data for the given database from this buffer.
     */
    public void clear( NamedDatabaseId databaseId )
    {
        Preconditions.checkArgument( databaseId != null,
                                     "Only queries targeting a specific database are expected in the recent query buffer, " +
                                     "clearing non-database queries will have no effect.");

        queries.clearIf( q -> databaseId.equals( q.databaseId ) );
    }

    /**
     * Apply the consumer on each query in this buffer which targeted the given database.
     */
    public void foreach( NamedDatabaseId databaseId, Consumer<TruncatedQuerySnapshot> consumer )
    {
        queries.foreach( q ->
                         {
                             if ( q.databaseId.equals( databaseId ) )
                             {
                                 consumer.accept( q );
                             }
                         } );
    }
}
