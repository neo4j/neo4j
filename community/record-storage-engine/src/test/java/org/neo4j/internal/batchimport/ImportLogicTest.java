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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.batchimport.api.Configuration.DEFAULT;
import static org.neo4j.batchimport.api.Monitor.NO_MONITOR;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.internal.batchimport.store.BatchingNeoStores.batchingNeoStoresWithExternalPageCache;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.logging.internal.NullLogService.getInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

@PageCacheExtension
@Neo4jLayoutExtension
@ExtendWith(RandomExtension.class)
class ImportLogicTest {
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private RandomSupport random;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    @Test
    void closeImporterWithoutDiagnosticState() throws IOException {
        ExecutionMonitor monitor = mock(ExecutionMonitor.class);
        IndexImporterFactory factory = mock(IndexImporterFactory.class);
        CursorContextFactory contextFactory = new CursorContextFactory(NULL, EMPTY_CONTEXT_SUPPLIER);
        try (BatchingNeoStores stores = batchingNeoStoresWithExternalPageCache(
                fileSystem,
                pageCache,
                NULL,
                CONTEXT_FACTORY,
                databaseLayout,
                DEFAULT,
                getInstance(),
                DefaultAdditionalIds.EMPTY,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                defaults(),
                INSTANCE)) {
            //noinspection EmptyTryBlock
            try (ImportLogic logic = new ImportLogic(
                    databaseLayout,
                    stores,
                    DEFAULT,
                    defaults(),
                    getInstance(),
                    monitor,
                    Collector.EMPTY,
                    NO_MONITOR,
                    contextFactory,
                    factory,
                    NULL,
                    EmptyMemoryTracker.INSTANCE)) {
                // nothing to run in this import
                logic.success();
            }
        }

        verify(monitor).done(eq(true), anyLong(), contains("Data statistics is not available."));
    }

    @Test
    void shouldSplitUpRelationshipTypesInBatches() {
        // GIVEN
        int denseNodeThreshold = 5;
        int numberOfNodes = 100;
        int numberOfTypes = 10;
        NodeRelationshipCache cache =
                new NodeRelationshipCache(NumberArrayFactories.HEAP, denseNodeThreshold, EmptyMemoryTracker.INSTANCE);
        cache.setNodeCount(numberOfNodes + 1);
        Direction[] directions = Direction.values();
        for (int i = 0; i < numberOfNodes; i++) {
            int count = random.nextInt(1, denseNodeThreshold * 2);
            cache.setCount(i, count, random.nextInt(numberOfTypes), random.among(directions));
        }
        cache.countingCompleted();
        List<DataStatistics.RelationshipTypeCount> types = new ArrayList<>();
        int numberOfRelationships = 0;
        for (int i = 0; i < numberOfTypes; i++) {
            int count = random.nextInt(1, 100);
            types.add(new DataStatistics.RelationshipTypeCount(i, count));
            numberOfRelationships += count;
        }
        types.sort((t1, t2) -> Long.compare(t2.getCount(), t1.getCount()));
        DataStatistics typeDistribution =
                new DataStatistics(0, 0, types.toArray(new DataStatistics.RelationshipTypeCount[0]));

        // WHEN enough memory for all types
        {
            long memory = cache.calculateMaxMemoryUsage(numberOfRelationships) * numberOfTypes;
            int upToType = ImportLogic.nextSetOfTypesThatFitInMemory(
                    typeDistribution, 0, memory, cache.getNumberOfDenseNodes());

            // THEN
            assertEquals(types.size(), upToType);
        }

        // and WHEN less than enough memory for all types
        {
            long memory = cache.calculateMaxMemoryUsage(numberOfRelationships) * numberOfTypes / 3;
            int startingFromType = 0;
            int rounds = 0;
            while (startingFromType < types.size()) {
                rounds++;
                startingFromType = ImportLogic.nextSetOfTypesThatFitInMemory(
                        typeDistribution, startingFromType, memory, cache.getNumberOfDenseNodes());
            }
            assertEquals(types.size(), startingFromType);
            assertThat(rounds).isGreaterThan(1);
        }
    }

    @Test
    void shouldUseDataStatisticsCountsForPrintingFinalStats() throws IOException {
        // given
        ExecutionMonitor monitor = mock(ExecutionMonitor.class);
        IndexImporterFactory factory = mock(IndexImporterFactory.class);
        CursorContextFactory contextFactory = new CursorContextFactory(NULL, EMPTY_CONTEXT_SUPPLIER);
        try (BatchingNeoStores stores = batchingNeoStoresWithExternalPageCache(
                fileSystem,
                pageCache,
                NULL,
                CONTEXT_FACTORY,
                databaseLayout,
                DEFAULT,
                getInstance(),
                DefaultAdditionalIds.EMPTY,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                defaults(),
                INSTANCE)) {
            // when
            DataStatistics.RelationshipTypeCount[] relationshipTypeCounts = new DataStatistics.RelationshipTypeCount[] {
                new DataStatistics.RelationshipTypeCount(0, 33), new DataStatistics.RelationshipTypeCount(1, 66)
            };
            DataStatistics dataStatistics = new DataStatistics(100123, 100456, relationshipTypeCounts);
            try (ImportLogic logic = new ImportLogic(
                    databaseLayout,
                    stores,
                    DEFAULT,
                    defaults(),
                    getInstance(),
                    monitor,
                    Collector.EMPTY,
                    NO_MONITOR,
                    contextFactory,
                    factory,
                    NULL,
                    EmptyMemoryTracker.INSTANCE)) {
                logic.putState(dataStatistics);
                logic.success();
            }

            // then
            verify(monitor).done(eq(true), anyLong(), contains(dataStatistics.toString()));
        }
    }
}
