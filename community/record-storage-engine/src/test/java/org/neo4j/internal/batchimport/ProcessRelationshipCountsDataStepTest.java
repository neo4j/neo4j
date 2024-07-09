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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.ByteUnit.tebiBytes;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.staging.SimpleStageControl;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class ProcessRelationshipCountsDataStepTest {
    @Test
    void shouldLetProcessorsBeZeroIfEnoughMemory() {
        // given
        ProcessRelationshipCountsDataStep step = instantiateStep(10, 10, 10_000, 4, mebiBytes(10));

        // then
        assertEquals(0, step.maxProcessors());
    }

    @Test
    void shouldNotOverflowWhenTooMuchMemoryAvailable() {
        // given
        ProcessRelationshipCountsDataStep step = instantiateStep(1, 1, 10_000, 64, tebiBytes(10));

        // then
        assertEquals(0, step.maxProcessors());
    }

    @Test
    void shouldLimitProcessorsIfScarceMemory() {
        // given labels/types amounting to ~360k, 2MiB max mem and 1MiB in use by node-label cache
        ProcessRelationshipCountsDataStep step = instantiateStep(100, 220, mebiBytes(1), 4, mebiBytes(2));

        // then
        assertEquals(2, step.maxProcessors());
    }

    @Test
    void shouldAtLeastHaveOneProcessorEvenIfLowMemory() {
        // given labels/types amounting to ~1.6MiB, 2MiB max mem and 1MiB in use by node-label cache
        ProcessRelationshipCountsDataStep step = instantiateStep(1_000, 1_000, mebiBytes(1), 4, mebiBytes(2));

        // then
        assertEquals(1, step.maxProcessors());
    }

    private static ProcessRelationshipCountsDataStep instantiateStep(
            int highLabelId, int highRelationshipTypeId, long labelCacheSize, int maxProcessors, long maxMemory) {
        StageControl control = new SimpleStageControl();
        NodeLabelsCache cache = nodeLabelsCache(labelCacheSize);
        Configuration config = mock(Configuration.class);
        when(config.maxNumberOfWorkerThreads()).thenReturn(maxProcessors);
        when(config.maxOffHeapMemory()).thenReturn(maxMemory);
        return new ProcessRelationshipCountsDataStep(
                control,
                cache,
                config,
                highLabelId,
                highRelationshipTypeId,
                mock(CountsUpdater.class),
                NumberArrayFactories.OFF_HEAP,
                ProgressListener.NONE,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                any -> StoreCursors.NULL,
                INSTANCE);
    }

    private static NodeLabelsCache nodeLabelsCache(long sizeInBytes) {
        NodeLabelsCache cache = mock(NodeLabelsCache.class);
        doAnswer(invocation -> {
                    MemoryStatsVisitor visitor = invocation.getArgument(0);
                    visitor.offHeapUsage(sizeInBytes);
                    return null;
                })
                .when(cache)
                .acceptMemoryStatsVisitor(any());
        return cache;
    }
}
