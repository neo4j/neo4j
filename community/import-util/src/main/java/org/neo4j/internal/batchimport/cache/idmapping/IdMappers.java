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
package org.neo4j.internal.batchimport.cache.idmapping;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

import org.neo4j.internal.batchimport.PropertyValueLookup;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.cache.idmapping.string.EncodingIdMapper;
import org.neo4j.internal.batchimport.cache.idmapping.string.LongCollisionValues;
import org.neo4j.internal.batchimport.cache.idmapping.string.LongEncoder;
import org.neo4j.internal.batchimport.cache.idmapping.string.Radix;
import org.neo4j.internal.batchimport.cache.idmapping.string.StringCollisionValues;
import org.neo4j.internal.batchimport.cache.idmapping.string.StringEncoder;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.internal.batchimport.cache.idmapping.string.EncodingIdMapper.NO_MONITOR;
import static org.neo4j.internal.batchimport.cache.idmapping.string.TrackerFactories.dynamic;

/**
 * Place to instantiate common {@link IdMapper} implementations.
 */
public class IdMappers
{
    private static class ActualIdMapper implements IdMapper
    {
        @Override
        public void put( Object inputId, long actualId, Group group )
        {   // No need to remember anything
        }

        @Override
        public boolean needsPreparation()
        {
            return false;
        }

        @Override
        public void prepare( PropertyValueLookup inputIdLookup, Collector collector, ProgressListener progress )
        {   // No need to prepare anything
        }

        @Override
        public long get( Object inputId, Group group )
        {
            return (Long) inputId;
        }

        @Override
        public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
        {   // No memory usage
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }

        @Override
        public void close()
        {   // Nothing to close
        }

        @Override
        public MemoryStatsVisitor.Visitable memoryEstimation( long numberOfNodes )
        {
            return MemoryStatsVisitor.NONE;
        }

        @Override
        public LongIterator leftOverDuplicateNodesIds()
        {
            return ImmutableEmptyLongIterator.INSTANCE;
        }
    }

    private IdMappers()
    {
    }

    /**
     * An {@link IdMapper} that doesn't touch the input ids, but just asserts that node ids arrive in ascending order.
     * This is for advanced usage and puts constraints on the input in that all node ids given as input
     * must be valid. There will not be further checks, other than that for order of the ids.
     */
    public static IdMapper actual()
    {
        return new ActualIdMapper();
    }

    /**
     * An {@link IdMapper} capable of mapping {@link String strings} to long ids.
     *
     * @param cacheFactory {@link NumberArrayFactory} for allocating memory for the cache used by this index.
     * @param groups {@link Groups} containing all id groups.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return {@link IdMapper} for when input ids are strings.
     */
    public static IdMapper strings( NumberArrayFactory cacheFactory, ReadableGroups groups, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        return new EncodingIdMapper( cacheFactory, new StringEncoder(), Radix.STRING, NO_MONITOR, dynamic( memoryTracker ), groups,
                numberOfCollisions -> new StringCollisionValues( cacheFactory, numberOfCollisions, memoryTracker ), pageCacheTracer, memoryTracker );
    }

    /**
     * An {@link IdMapper} capable of mapping {@link Long arbitrary longs} to long ids.
     *
     * @param cacheFactory {@link NumberArrayFactory} for allocating memory for the cache used by this index.
     * @param groups {@link Groups} containing all id groups.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return {@link IdMapper} for when input ids are numbers.
     */
    public static IdMapper longs( NumberArrayFactory cacheFactory, ReadableGroups groups, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        return new EncodingIdMapper( cacheFactory, new LongEncoder(), Radix.LONG, NO_MONITOR, dynamic( memoryTracker ), groups,
                numberOfCollisions -> new LongCollisionValues( cacheFactory, numberOfCollisions, memoryTracker ), pageCacheTracer, memoryTracker );
    }
}
