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

import java.util.function.LongSupplier;

import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.input.Input;

import static org.neo4j.io.os.OsBeanUtil.VALUE_UNAVAILABLE;
import static org.neo4j.unsafe.impl.batchimport.ImportMemoryCalculator.estimatedCacheSize;
import static org.neo4j.unsafe.impl.batchimport.ImportMemoryCalculator.optimalMinimalHeapSize;

/**
 * Sanity checking of {@link org.neo4j.unsafe.impl.batchimport.input.Input.Estimates} against heap size and free memory.
 * Registers warnings onto a {@link ImportLogic.Monitor}.
 */
class HeapSizeSanityChecker
{
    private final ImportLogic.Monitor monitor;
    private final LongSupplier freeMemoryLookup;
    private final LongSupplier actualHeapSizeLookup;

    HeapSizeSanityChecker( ImportLogic.Monitor monitor )
    {
        this( monitor, OsBeanUtil::getFreePhysicalMemory, Runtime.getRuntime()::maxMemory );
    }

    HeapSizeSanityChecker( ImportLogic.Monitor monitor, LongSupplier freeMemoryLookup, LongSupplier actualHeapSizeLookup )
    {
        this.monitor = monitor;
        this.freeMemoryLookup = freeMemoryLookup;
        this.actualHeapSizeLookup = actualHeapSizeLookup;
    }

    void sanityCheck( Input.Estimates inputEstimates, RecordFormats recordFormats, MemoryStatsVisitor.Visitable baseMemory,
            MemoryStatsVisitor.Visitable... memoryVisitables )
    {
        // At this point in time the store hasn't started so it won't show up in free memory reported from OS,
        // i.e. we have to include it here in the calculations.
        long estimatedCacheSize = estimatedCacheSize( baseMemory, memoryVisitables );
        long freeMemory = freeMemoryLookup.getAsLong();
        long optimalMinimalHeapSize = optimalMinimalHeapSize( inputEstimates, recordFormats );
        long actualHeapSize = actualHeapSizeLookup.getAsLong();
        boolean freeMemoryIsKnown = freeMemory != VALUE_UNAVAILABLE;

        // Check if there's enough memory for the import
        if ( freeMemoryIsKnown && actualHeapSize + freeMemory < estimatedCacheSize + optimalMinimalHeapSize )
        {
            monitor.insufficientAvailableMemory( estimatedCacheSize, optimalMinimalHeapSize, freeMemory );
            return; // there's likely not available memory, no need to warn about anything else
        }

        // Check if the heap is big enough to handle the import
        if ( actualHeapSize < optimalMinimalHeapSize )
        {
            monitor.insufficientHeapSize( optimalMinimalHeapSize, actualHeapSize );
            return; // user have been warned about heap size issue
        }

        // Check if heap size could be tweaked
        if ( (!freeMemoryIsKnown || freeMemory < estimatedCacheSize) && actualHeapSize > optimalMinimalHeapSize * 1.2 )
        {
            monitor.abundantHeapSize( optimalMinimalHeapSize, actualHeapSize );
        }
    }
}
