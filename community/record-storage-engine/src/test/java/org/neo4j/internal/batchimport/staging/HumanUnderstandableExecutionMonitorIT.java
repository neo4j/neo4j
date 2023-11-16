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
package org.neo4j.internal.batchimport.staging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.input.DataGeneratorInput.bareboneNodeHeader;
import static org.neo4j.internal.batchimport.input.DataGeneratorInput.bareboneRelationshipHeader;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

import java.io.OutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.collection.Dependencies;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.DataStatistics;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.batchimport.Monitor;
import org.neo4j.internal.batchimport.NodeDegreeCountStage;
import org.neo4j.internal.batchimport.ParallelBatchImporter;
import org.neo4j.internal.batchimport.cache.PageCacheArrayFactoryMonitor;
import org.neo4j.internal.batchimport.cache.idmapping.IdMappers;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.DataGeneratorInput;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

@Neo4jLayoutExtension
@ExtendWith({RandomExtension.class, DefaultFileSystemExtension.class, TestDirectorySupportExtension.class})
class HumanUnderstandableExecutionMonitorIT {
    private static final long NODE_COUNT = 1_000;
    private static final long RELATIONSHIP_COUNT = 10_000;

    @Inject
    private RandomSupport random;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void shouldReportProgressOfNodeImport() throws Exception {
        // given
        CapturingMonitor progress = new CapturingMonitor();
        PrintStream nullStream = new PrintStream(OutputStream.nullOutputStream());
        HumanUnderstandableExecutionMonitor monitor =
                new HumanUnderstandableExecutionMonitor(progress, nullStream, nullStream);
        IdType idType = IdType.INTEGER;
        DataGeneratorInput.DataDistribution dataDistribution = DataGeneratorInput.data(NODE_COUNT, RELATIONSHIP_COUNT);
        Groups groups = new Groups();
        Group group = groups.getOrCreate(null);
        var extractors = new Extractors();
        Input input = new DataGeneratorInput(
                dataDistribution,
                idType,
                random.seed(),
                bareboneNodeHeader(idType, group, extractors),
                bareboneRelationshipHeader(idType, group, extractors),
                groups);

        // when
        try (JobScheduler jobScheduler = new ThreadPoolJobScheduler()) {
            new ParallelBatchImporter(
                            databaseLayout,
                            fileSystem,
                            NULL,
                            Configuration.DEFAULT,
                            NullLogService.getInstance(),
                            monitor,
                            EMPTY,
                            new EmptyLogTailMetadata(defaults()),
                            defaults(pagecache_memory, mebiBytes(8)),
                            Monitor.NO_MONITOR,
                            jobScheduler,
                            Collector.EMPTY,
                            LogFilesInitializer.NULL,
                            IndexImporterFactory.EMPTY,
                            EmptyMemoryTracker.INSTANCE,
                            NULL_CONTEXT_FACTORY)
                    .doImport(input);

            // then
            progress.assertAllProgressReachedEnd();
        }
    }

    @Test
    void shouldStartFromNonFirstStage() {
        // given
        PrintStream nullStream = new PrintStream(OutputStream.nullOutputStream());
        HumanUnderstandableExecutionMonitor monitor =
                new HumanUnderstandableExecutionMonitor(Monitor.NO_MONITOR, nullStream, nullStream);
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency(Input.knownEstimates(10, 10, 10, 10, 10, 10, 10));
        BatchingNeoStores neoStores = mock(BatchingNeoStores.class);
        NodeStore nodeStore = mock(NodeStore.class, RETURNS_MOCKS);
        RelationshipStore relationshipStore = mock(RelationshipStore.class, RETURNS_MOCKS);
        when(neoStores.getNodeStore()).thenReturn(nodeStore);
        when(neoStores.getRelationshipStore()).thenReturn(relationshipStore);
        dependencies.satisfyDependency(neoStores);
        dependencies.satisfyDependency(IdMappers.actual());
        dependencies.satisfyDependency(mock(PageCacheArrayFactoryMonitor.class));
        dependencies.satisfyDependency(new DataStatistics(10, 10, new DataStatistics.RelationshipTypeCount[0]));
        monitor.initialize(dependencies);

        // when/then
        StageExecution execution = mock(StageExecution.class);
        when(execution.getStageName()).thenReturn(NodeDegreeCountStage.NAME);
        assertThatCode(() -> monitor.start(execution)).doesNotThrowAnyException();
    }

    private static class CapturingMonitor implements Monitor {
        private volatile int previousPercentageCompleted;

        @Override
        public void percentageCompleted(int percentage) {
            if (percentage < 0 || percentage > 100) {
                fail("Expected percentage to be 0..100% but was " + percentage);
            }

            if (percentage < previousPercentageCompleted) {
                fail("Progress should go forwards only, but went from " + previousPercentageCompleted + " to "
                        + percentage);
            }
            previousPercentageCompleted = percentage;
        }

        void assertAllProgressReachedEnd() {
            assertThat(previousPercentageCompleted).isEqualTo(100);
        }
    }
}
