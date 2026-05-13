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
package org.neo4j.kernel.api.impl.index;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10Directory;
import org.neo4j.kernel.api.impl.index.lucene.v9.Lucene9Directory;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.AbstractTextIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.concurrent.Futures;

@TestDirectoryExtension
class DatabaseIndexIntegrationTest {
    private static final int THREAD_NUMBER = 5;
    private static ExecutorService workers;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private final CountDownLatch raceSignal = new CountDownLatch(1);
    private DirectoryFactory directoryFactory;
    private WritableTestDatabaseIndex luceneIndex;

    @BeforeAll
    static void initExecutors() {
        workers = Executors.newFixedThreadPool(THREAD_NUMBER);
    }

    @AfterAll
    static void shutDownExecutor() {
        workers.shutdownNow();
    }

    void setUp(LuceneContext luceneContext) throws IOException {
        directoryFactory = switch (luceneContext) {
            case LUCENE_9 -> new Lucene9SyncNotifierDirectoryFactory(raceSignal);
            case LUCENE_10 -> new Lucene10SyncNotifierDirectoryFactory(raceSignal);
        };
        luceneIndex = createTestLuceneIndex(directoryFactory, testDirectory.homePath());
    }

    @AfterEach
    void tearDown() throws Exception {
        directoryFactory.close();
    }

    @ParameterizedTest
    @EnumSource
    void testSaveCallCommitAndCloseFromMultipleThreads(LuceneContext luceneContext) throws IOException {
        setUp(luceneContext);

        assertTimeoutPreemptively(ofSeconds(60), () -> {
            generateInitialData();
            List<Future<?>> closeFutures = submitTasks(() -> createConcurrentCloseTask(raceSignal));

            Futures.getAll(closeFutures);
            assertThat(luceneIndex.isOpen()).isFalse();
        });
    }

    @ParameterizedTest
    @EnumSource
    void saveCallCloseAndDropFromMultipleThreads(LuceneContext luceneContext) throws IOException {
        setUp(luceneContext);

        assertTimeoutPreemptively(ofSeconds(60), () -> {
            generateInitialData();
            List<Future<?>> futures = submitTasks(() -> createConcurrentDropTask(raceSignal));

            Futures.getAll(futures);
            assertThat(luceneIndex.isOpen()).isFalse();
        });
    }

    private WritableTestDatabaseIndex createTestLuceneIndex(DirectoryFactory dirFactory, Path folder)
            throws IOException {
        PartitionedIndexStorage indexStorage =
                new PartitionedIndexStorage(LuceneContext.LUCENE_10, dirFactory, fileSystem, folder);
        WritableTestDatabaseIndex index = new WritableTestDatabaseIndex(indexStorage);
        index.create();
        index.open();
        return index;
    }

    private List<Future<?>> submitTasks(Supplier<Runnable> taskSupplier) {
        List<Future<?>> futures = new ArrayList<>(THREAD_NUMBER);
        futures.add(workers.submit(createMainCloseTask()));
        for (int i = 0; i < THREAD_NUMBER - 1; i++) {
            futures.add(workers.submit(taskSupplier.get()));
        }
        return futures;
    }

    private void generateInitialData() throws IOException {
        LuceneIndexWriter indexWriter = firstPartitionWriter();
        for (int i = 0; i < 10; i++) {
            indexWriter.addDocument(createTestDocument(indexWriter));
        }
    }

    private Runnable createConcurrentDropTask(CountDownLatch dropRaceSignal) {
        return () -> {
            try {
                dropRaceSignal.await();
                Thread.yield();
                luceneIndex.drop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Runnable createConcurrentCloseTask(CountDownLatch closeRaceSignal) {
        return () -> {
            try {
                closeRaceSignal.await();
                Thread.yield();
                luceneIndex.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Runnable createMainCloseTask() {
        return () -> {
            try {
                luceneIndex.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static LuceneDocument createTestDocument(LuceneIndexWriter indexWriter) {
        LuceneDocument document = indexWriter.newDocument();
        document.addTextField("text", "textValue", true);
        document.addNumericDocValuesField("long", 1);
        return document;
    }

    private LuceneIndexWriter firstPartitionWriter() {
        List<AbstractIndexPartition> partitions = luceneIndex.getPartitions();
        assertEquals(1, partitions.size());
        AbstractIndexPartition partition = partitions.getFirst();
        return partition.getIndexWriter();
    }

    private static class WritableTestDatabaseIndex
            extends WritableDatabaseIndex<TestLuceneIndex, AbstractTextIndexReader> {
        WritableTestDatabaseIndex(PartitionedIndexStorage indexStorage) {
            super(
                    new TestLuceneIndex(indexStorage, new WritableIndexPartitionFactory(() -> {
                        Config config = Config.defaults();
                        return new IndexWriterConfigBuilder(IndexWriterConfigMode.TEXT, config).build();
                    })),
                    writable(),
                    false);
        }
    }

    private static class TestLuceneIndex extends AbstractLuceneIndex<AbstractTextIndexReader> {

        TestLuceneIndex(PartitionedIndexStorage indexStorage, IndexPartitionFactory partitionFactory) {
            super(indexStorage, partitionFactory, null, Config.defaults(), NullLogProvider.getInstance());
        }

        @Override
        protected AbstractTextIndexReader createSimpleReader(
                List<AbstractIndexPartition> partitions, IndexUsageTracking usageTracker) {
            return null;
        }

        @Override
        protected AbstractTextIndexReader createPartitionedReader(
                List<AbstractIndexPartition> partitions, IndexUsageTracking usageTracker) {
            return null;
        }
    }

    private static class Lucene10SyncNotifierDirectoryFactory implements DirectoryFactory {
        private final CountDownLatch signal;

        Lucene10SyncNotifierDirectoryFactory(CountDownLatch signal) {
            this.signal = signal;
        }

        @Override
        public LuceneDirectory open(Path dir) throws IOException {
            Files.createDirectories(dir);
            return new Lucene10Directory(new Lucene10SyncNotifierLuceneDirectory(FSDirectory.open(dir), signal));
        }

        @Override
        public LuceneContext getContext() {
            return LuceneContext.LUCENE_10;
        }

        @Override
        public void close() {}

        private static class Lucene10SyncNotifierLuceneDirectory extends FilterDirectory {
            private final CountDownLatch signal;

            Lucene10SyncNotifierLuceneDirectory(Directory delegate, CountDownLatch signal) {
                super(delegate);
                this.signal = signal;
            }

            @Override
            public void sync(Collection<String> names) throws IOException {
                // where are waiting for a specific sync during index commit process inside lucene
                // as soon as we will reach it - we will fail into sleep to give chance for concurrent close calls
                if (names.stream().noneMatch(name -> name.startsWith(IndexFileNames.PENDING_SEGMENTS))) {
                    try {
                        signal.countDown();
                        Thread.sleep(500);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                super.sync(names);
            }
        }
    }

    private static class Lucene9SyncNotifierDirectoryFactory implements DirectoryFactory {
        private final CountDownLatch signal;

        Lucene9SyncNotifierDirectoryFactory(CountDownLatch signal) {
            this.signal = signal;
        }

        @Override
        public LuceneDirectory open(Path dir) throws IOException {
            Files.createDirectories(dir);
            return new Lucene9Directory(new Lucene9SyncNotifierLuceneDirectory(
                    org.neo4j.shaded.lucene9.store.FSDirectory.open(dir), signal));
        }

        @Override
        public LuceneContext getContext() {
            return LuceneContext.LUCENE_9;
        }

        @Override
        public void close() {}

        private static class Lucene9SyncNotifierLuceneDirectory extends org.neo4j.shaded.lucene9.store.FilterDirectory {
            private final CountDownLatch signal;

            Lucene9SyncNotifierLuceneDirectory(
                    org.neo4j.shaded.lucene9.store.Directory delegate, CountDownLatch signal) {
                super(delegate);
                this.signal = signal;
            }

            @Override
            public void sync(Collection<String> names) throws IOException {
                // where are waiting for a specific sync during index commit process inside lucene
                // as soon as we will reach it - we will fail into sleep to give chance for concurrent close calls
                if (names.stream().noneMatch(name -> name.startsWith(IndexFileNames.PENDING_SEGMENTS))) {
                    try {
                        signal.countDown();
                        Thread.sleep(500);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                super.sync(names);
            }
        }
    }
}
