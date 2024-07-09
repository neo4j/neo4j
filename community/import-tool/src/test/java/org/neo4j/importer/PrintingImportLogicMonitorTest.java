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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.io.ByteUnit.gibiBytes;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Monitor;

/**
 * Why test a silly thing like this? This implementation contains some printf calls that needs to get arguments correct
 * or will otherwise throw exception. It's surprisingly easy to get those wrong.
 */
class PrintingImportLogicMonitorTest {
    private final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    private final PrintStream out = new PrintStream(outBuffer);
    private final ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
    private final PrintStream err = new PrintStream(errBuffer);
    private final Monitor monitor = new PrintingImportLogicMonitor(out, err);

    @Test
    void mayExceedNodeIdCapacity() {
        // given
        long capacity = 10_000_000;
        long estimatedCount = 12_000_000;

        // when
        monitor.mayExceedNodeIdCapacity(capacity, estimatedCount);

        // then
        String text = errBuffer.toString();
        assertTrue(text.contains("WARNING"));
        assertTrue(text.contains("exceed"));
        assertTrue(text.contains(String.valueOf(capacity)));
        assertTrue(text.contains(String.valueOf(estimatedCount)));
    }

    @Test
    void mayExceedRelationshipIdCapacity() {
        // given
        long capacity = 10_000_000;
        long estimatedCount = 12_000_000;

        // when
        monitor.mayExceedRelationshipIdCapacity(capacity, estimatedCount);

        // then
        String text = errBuffer.toString();
        assertTrue(text.contains("WARNING"));
        assertTrue(text.contains("exceed"));
        assertTrue(text.contains(String.valueOf(capacity)));
        assertTrue(text.contains(String.valueOf(estimatedCount)));
    }

    @Test
    void insufficientHeapSize() {
        // given
        long optimalHeapSize = gibiBytes(2);
        long heapSize = gibiBytes(1);

        // when
        monitor.insufficientHeapSize(optimalHeapSize, heapSize);

        // then
        String text = errBuffer.toString();
        assertTrue(text.contains("WARNING"));
        assertTrue(text.contains("too small"));
        assertTrue(text.contains(bytesToString(heapSize)));
        assertTrue(text.contains(bytesToString(optimalHeapSize)));
    }

    @Test
    void abundantHeapSize() {
        // given
        long optimalHeapSize = gibiBytes(2);
        long heapSize = gibiBytes(10);

        // when
        monitor.abundantHeapSize(optimalHeapSize, heapSize);

        // then
        String text = errBuffer.toString();
        assertTrue(text.contains("WARNING"));
        assertTrue(text.contains("unnecessarily large"));
        assertTrue(text.contains(bytesToString(heapSize)));
        assertTrue(text.contains(bytesToString(optimalHeapSize)));
    }

    @Test
    void insufficientAvailableMemory() {
        // given
        long estimatedCacheSize = gibiBytes(2);
        long optimalHeapSize = gibiBytes(2);
        long availableMemory = gibiBytes(1);

        // when
        monitor.insufficientAvailableMemory(estimatedCacheSize, optimalHeapSize, availableMemory);

        // then
        String text = errBuffer.toString();
        assertTrue(text.contains("WARNING"));
        assertTrue(text.contains("may not be sufficient"));
        assertTrue(text.contains(bytesToString(estimatedCacheSize)));
        assertTrue(text.contains(bytesToString(optimalHeapSize)));
        assertTrue(text.contains(bytesToString(availableMemory)));
    }
}
