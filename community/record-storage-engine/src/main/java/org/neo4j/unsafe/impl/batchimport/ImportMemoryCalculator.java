/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.input.Input;

import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor.highestMemoryUsageOf;
import static org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor.totalMemoryUsageOf;

/**
 * Aims to collect logic for calculating memory usage, optimal heap size and more, mostly based on
 * {@link org.neo4j.unsafe.impl.batchimport.input.Input.Estimates}. The reason why we're trying so hard to calculate
 * these things is that for large imports... getting the balance between heap and off-heap memory just right
 * will allow the importer to use available memory and can mean difference between a failed and successful import.
 *
 * The calculated numbers are a bit on the defensive side, generally adding 10% to the numbers.
 */
public class ImportMemoryCalculator
{
    public static long estimatedStoreSize( Input.Estimates estimates, RecordFormats recordFormats )
    {
        long nodeSize = estimates.numberOfNodes() * recordFormats.node().getRecordSize( NO_STORE_HEADER );
        long relationshipSize = estimates.numberOfRelationships() * recordFormats.relationship().getRecordSize( NO_STORE_HEADER );
        long propertySize = estimates.sizeOfNodeProperties() + estimates.sizeOfRelationshipProperties();
        long tempIdPropertySize = estimates.numberOfNodes() * recordFormats.property().getRecordSize( NO_STORE_HEADER );

        return defensivelyPadMemoryEstimate( nodeSize + relationshipSize + propertySize + tempIdPropertySize );
    }

    public static long estimatedCacheSize( MemoryStatsVisitor.Visitable baseMemory, MemoryStatsVisitor.Visitable... memoryUsers )
    {
        long neoStoreSize = totalMemoryUsageOf( baseMemory );
        long importCacheSize = highestMemoryUsageOf( memoryUsers );
        return neoStoreSize + defensivelyPadMemoryEstimate( importCacheSize );
    }

    /**
     * Calculates optimal and minimal heap size for an import. A minimal heap for an import has enough room for some amount
     * of working memory and the part of the page cache meta data living in the heap.
     *
     * At the time of writing this the heap size is really only a function of store size, where parts of the page cache
     * meta data lives in the heap. For reference page cache meta data of a store of ~18TiB takes up ~10GiB of heap,
     * so pageCacheHeapUsage ~= storeSize / 2000. On top of that there must be some good old working memory of ~1-2 GiB
     * for handling objects created and operating during the import.
     *
     * @param estimates input estimates.
     * @param recordFormats {@link RecordFormats}, containing record sizes.
     * @return an optimal minimal heap size to use for this import.
     */
    public static long optimalMinimalHeapSize( Input.Estimates estimates, RecordFormats recordFormats )
    {
        long estimatedStoreSize = estimatedStoreSize( estimates, recordFormats );

        return // working memory
               gibiBytes( 1 ) +
               // page cache meta data, see outline of this number above
               estimatedStoreSize / 2_000;
    }

    public static long defensivelyPadMemoryEstimate( long bytes )
    {
        return (long) (bytes * 1.1);
    }

    public static long defensivelyPadMemoryEstimate( MemoryStatsVisitor.Visitable... memoryUsers )
    {
        return defensivelyPadMemoryEstimate( totalMemoryUsageOf( memoryUsers ) );
    }
}
