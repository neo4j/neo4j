/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.importer;

import static org.neo4j.io.ByteUnit.bytesToString;

import java.io.PrintStream;
import org.neo4j.batchimport.api.Monitor;

public class PrintingImportLogicMonitor implements Monitor {
    private final PrintStream out;
    private final PrintStream err;

    public PrintingImportLogicMonitor(PrintStream out, PrintStream err) {
        this.out = out;
        this.err = err;
    }

    @Override
    public void doubleRelationshipRecordUnitsEnabled() {
        out.println("Will use double record units for all relationships");
    }

    @Override
    public void mayExceedNodeIdCapacity(long capacity, long estimatedCount) {
        err.printf(
                "WARNING: estimated number of relationships %d may exceed capacity %d of selected record format%n",
                estimatedCount, capacity);
    }

    @Override
    public void mayExceedRelationshipIdCapacity(long capacity, long estimatedCount) {
        err.printf(
                "WARNING: estimated number of nodes %d may exceed capacity %d of selected record format%n",
                estimatedCount, capacity);
    }

    @Override
    public void insufficientHeapSize(long optimalMinimalHeapSize, long heapSize) {
        err.printf(
                "WARNING: heap size %s may be too small to complete this import. Suggested heap size is %s",
                bytesToString(heapSize), bytesToString(optimalMinimalHeapSize));
    }

    @Override
    public void abundantHeapSize(long optimalMinimalHeapSize, long heapSize) {
        err.printf(
                "WARNING: heap size %s is unnecessarily large for completing this import.%n"
                        + "The abundant heap memory will leave less memory for off-heap importer caches. Suggested heap size is %s",
                bytesToString(heapSize), bytesToString(optimalMinimalHeapSize));
    }

    @Override
    public void insufficientAvailableMemory(
            long estimatedCacheSize, long optimalMinimalHeapSize, long availableMemory) {
        err.printf(
                "WARNING: %s memory may not be sufficient to complete this import. Suggested memory distribution is:%n"
                        + "heap size: %s%n"
                        + "minimum free and available memory excluding heap size: %s",
                bytesToString(availableMemory),
                bytesToString(optimalMinimalHeapSize),
                bytesToString(estimatedCacheSize));
    }
}
