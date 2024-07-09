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
package org.neo4j.internal.batchimport;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;

import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;

class HeapSizeSanityCheckerTest {
    private final LongSupplier freeMemorySupplier = mock(LongSupplier.class);
    private final LongSupplier actualHeapSizeSupplier = mock(LongSupplier.class);
    private final Monitor monitor = mock(Monitor.class);
    private final HeapSizeSanityChecker checker =
            new HeapSizeSanityChecker(monitor, freeMemorySupplier, actualHeapSizeSupplier);
    private final LongSupplier baseMemorySupplier = mock(LongSupplier.class);
    private final MemoryStatsVisitor.Visitable baseMemory =
            visitor -> visitor.offHeapUsage(baseMemorySupplier.getAsLong());
    private final LongSupplier memoryUser1Supplier = mock(LongSupplier.class);
    private final MemoryStatsVisitor.Visitable memoryUser1 =
            visitor -> visitor.offHeapUsage(memoryUser1Supplier.getAsLong());
    private final LongSupplier memoryUser2Supplier = mock(LongSupplier.class);
    private final MemoryStatsVisitor.Visitable memoryUser2 =
            visitor -> visitor.offHeapUsage(memoryUser2Supplier.getAsLong());

    @Test
    void shouldReportInsufficientAvailableMemory() {
        // given
        when(freeMemorySupplier.getAsLong()).thenReturn(gibiBytes(2));
        when(actualHeapSizeSupplier.getAsLong()).thenReturn(gibiBytes(1));
        when(baseMemorySupplier.getAsLong()).thenReturn(gibiBytes(1));
        when(memoryUser1Supplier.getAsLong()).thenReturn(gibiBytes(1));
        when(memoryUser2Supplier.getAsLong()).thenReturn(gibiBytes(1));
        Input.Estimates estimates = Input.knownEstimates(
                1_000_000_000, 10_000_000_000L, 2_000_000_000L, 0, gibiBytes(50), gibiBytes(100), 0);

        // when
        checker.sanityCheck(estimates, defaultFormat(), baseMemory, memoryUser1, memoryUser2);

        // then
        verify(monitor).insufficientAvailableMemory(anyLong(), anyLong(), anyLong());
        verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldReportInsufficientHeapSize() {
        // given
        when(freeMemorySupplier.getAsLong()).thenReturn(gibiBytes(20));
        when(actualHeapSizeSupplier.getAsLong()).thenReturn(gibiBytes(1));
        when(baseMemorySupplier.getAsLong()).thenReturn(gibiBytes(1));
        when(memoryUser1Supplier.getAsLong()).thenReturn(gibiBytes(1));
        when(memoryUser2Supplier.getAsLong()).thenReturn(gibiBytes(1));
        Input.Estimates estimates = Input.knownEstimates(
                1_000_000_000, 10_000_000_000L, 2_000_000_000L, 0, gibiBytes(50), gibiBytes(100), 0);

        // when
        checker.sanityCheck(estimates, defaultFormat(), baseMemory, memoryUser1, memoryUser2);

        // then
        verify(monitor).insufficientHeapSize(anyLong(), anyLong());
        verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldReportAbundantHeapSize() {
        // given
        when(freeMemorySupplier.getAsLong()).thenReturn(gibiBytes(2));
        when(actualHeapSizeSupplier.getAsLong()).thenReturn(gibiBytes(20));
        when(baseMemorySupplier.getAsLong()).thenReturn(gibiBytes(1));
        when(memoryUser1Supplier.getAsLong()).thenReturn(gibiBytes(1));
        when(memoryUser2Supplier.getAsLong()).thenReturn(gibiBytes(1));
        Input.Estimates estimates = Input.knownEstimates(
                1_000_000_000, 10_000_000_000L, 2_000_000_000L, 0, gibiBytes(50), gibiBytes(100), 0);

        // when
        checker.sanityCheck(estimates, defaultFormat(), baseMemory, memoryUser1, memoryUser2);

        // then
        verify(monitor).abundantHeapSize(anyLong(), anyLong());
        verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldReportNothingOnGoodSetup() {
        // given
        when(freeMemorySupplier.getAsLong()).thenReturn(gibiBytes(10));
        when(baseMemorySupplier.getAsLong()).thenReturn(gibiBytes(1));
        when(memoryUser1Supplier.getAsLong()).thenReturn(gibiBytes(1));
        when(memoryUser2Supplier.getAsLong()).thenReturn(gibiBytes(1));

        when(actualHeapSizeSupplier.getAsLong()).thenReturn(gibiBytes(2));
        Input.Estimates estimates = Input.knownEstimates(
                1_000_000_000, 10_000_000_000L, 2_000_000_000L, 0, gibiBytes(50), gibiBytes(100), 0);

        // when
        checker.sanityCheck(estimates, defaultFormat(), baseMemory, memoryUser1, memoryUser2);

        // then
        verifyNoMoreInteractions(monitor);
    }
}
