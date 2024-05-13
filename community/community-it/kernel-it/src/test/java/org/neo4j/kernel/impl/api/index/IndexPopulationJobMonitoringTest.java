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
package org.neo4j.kernel.impl.api.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.internal.kernel.api.IndexMonitor.NO_MONITOR;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import org.junit.jupiter.api.Test;
import org.neo4j.common.Subject;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.schema.SchemaTestUtil;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobMonitoringParams;

class IndexPopulationJobMonitoringTest {
    private final MultipleIndexPopulator populator = mock(MultipleIndexPopulator.class);
    private final MemoryTracker memoryTracker = mock(MemoryTracker.class);
    private final StoreScan scan = mock(StoreScan.class);
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Test
    void testPopulationOfSingleIndex() {
        when(populator.createStoreScan(any())).thenReturn(scan);
        when(populator.hasPopulators()).thenReturn(true);
        when(scan.getProgress())
                .thenReturn(
                        PopulationProgress.NONE,
                        PopulationProgress.single(9, 1000),
                        PopulationProgress.single(99, 1000),
                        PopulationProgress.single(999, 1000),
                        PopulationProgress.DONE);
        var job = new IndexPopulationJob(
                populator,
                NO_MONITOR,
                CONTEXT_FACTORY,
                memoryTracker,
                "Test DB",
                new Subject("Test User"),
                NODE,
                Config.defaults());

        addIndex(job, "the ONE");

        var monitoringParams = job.getMonitoringParams();

        assertThat(monitoringParams.getSubmitter().describe()).isEqualTo("Test User");
        assertThat(monitoringParams.getTargetDatabaseName()).isEqualTo("Test DB");
        assertThat(monitoringParams.getDescription()).isEqualTo("Population of index 'the ONE'");

        verifyCurrentState(monitoringParams, "Total progress: 0.0%");

        job.run();

        verifyCurrentState(monitoringParams, "Total progress: 0.0%");
        verifyCurrentState(monitoringParams, "Total progress: 0.9%");
        verifyCurrentState(monitoringParams, "Total progress: 9.9%");
        verifyCurrentState(monitoringParams, "Total progress: 99.9%");
        verifyCurrentState(monitoringParams, "Total progress: 100.0%");
    }

    @Test
    void testPopulationOfMultipleIndexes() {
        when(populator.createStoreScan(any())).thenReturn(scan);
        when(populator.hasPopulators()).thenReturn(true);
        when(scan.getProgress())
                .thenReturn(
                        PopulationProgress.NONE,
                        PopulationProgress.single(9, 1000),
                        PopulationProgress.single(99, 1000),
                        PopulationProgress.single(999, 1000),
                        PopulationProgress.DONE);
        var job = new IndexPopulationJob(
                populator,
                NO_MONITOR,
                CONTEXT_FACTORY,
                memoryTracker,
                "Another Test DB",
                new Subject("Another Test User"),
                NODE,
                Config.defaults());

        addIndex(job, "index 1");
        addIndex(job, "index 2");
        addIndex(job, "index 3");

        var monitoringParams = job.getMonitoringParams();

        assertThat(monitoringParams.getSubmitter().describe()).isEqualTo("Another Test User");
        assertThat(monitoringParams.getTargetDatabaseName()).isEqualTo("Another Test DB");
        assertThat(monitoringParams.getDescription()).isEqualTo("Population of 3 'NODE' indexes");

        verifyCurrentState(
                monitoringParams, "Population of indexes 'index 1','index 2','index 3'; Total progress: 0.0%");

        job.run();

        verifyCurrentState(
                monitoringParams, "Population of indexes 'index 1','index 2','index 3'; Total progress: 0.0%");
        verifyCurrentState(
                monitoringParams, "Population of indexes 'index 1','index 2','index 3'; Total progress: 0.9%");
        verifyCurrentState(
                monitoringParams, "Population of indexes 'index 1','index 2','index 3'; Total progress: 9.9%");
        verifyCurrentState(
                monitoringParams, "Population of indexes 'index 1','index 2','index 3'; Total progress: 99.9%");
        verifyCurrentState(
                monitoringParams, "Population of indexes 'index 1','index 2','index 3'; Total progress: 100.0%");
    }

    private static void addIndex(IndexPopulationJob job, String indexName) {
        var idxPrototype =
                IndexPrototype.forSchema(mock(SchemaDescriptor.class)).withName(indexName);
        var indexDescriptor = idxPrototype.materialise(99);
        var indexProxyInfo = new ValueIndexProxyStrategy(
                indexDescriptor, mock(IndexStatisticsStore.class), SchemaTestUtil.SIMPLE_NAME_LOOKUP);
        job.addPopulator(null, indexProxyInfo, null);
    }

    private static void verifyCurrentState(
            JobMonitoringParams monitoringParams, String expectedCurrentStateDescription) {
        assertThat(monitoringParams.getCurrentStateDescription()).isEqualTo(expectedCurrentStateDescription);
    }
}
