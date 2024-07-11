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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.AbstractTextIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
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
    private SyncNotifierDirectoryFactory directoryFactory;
    private WritableTestDatabaseIndex luceneIndex;

    @BeforeAll
    static void initExecutors() {
        workers = Executors.newFixedThreadPool(THREAD_NUMBER);
    }

    @AfterAll
    static void shutDownExecutor() {
        workers.shutdownNow();
    }

    @BeforeEach
    void setUp() throws IOException {
        directoryFactory = new SyncNotifierDirectoryFactory(raceSignal);
        luceneIndex = createTestLuceneIndex(directoryFactory, testDirectory.homePath());
    }

    @AfterEach
    void tearDown() {
        directoryFactory.close();
    }

    @RepeatedTest(2)
    void testSaveCallCommitAndCloseFromMultipleThreads() {
        assertTimeoutPreemptively(ofSeconds(60), () -> {
            generateInitialData();
            Supplier<Runnable> closeTaskSupplier = () -> createConcurrentCloseTask(raceSignal);
            List<Future<?>> closeFutures = submitTasks(closeTaskSupplier);

            Futures.getAll(closeFutures);

            assertFalse(luceneIndex.isOpen());
        });
    }

    @RepeatedTest(2)
    void saveCallCloseAndDropFromMultipleThreads() {
        assertTimeoutPreemptively(ofSeconds(60), () -> {
            generateInitialData();
            Supplier<Runnable> dropTaskSupplier = () -> createConcurrentDropTask(raceSignal);
            List<Future<?>> futures = submitTasks(dropTaskSupplier);

            Futures.getAll(futures);

            assertFalse(luceneIndex.isOpen());
        });
    }

    private WritableTestDatabaseIndex createTestLuceneIndex(DirectoryFactory dirFactory, Path folder)
            throws IOException {
        PartitionedIndexStorage indexStorage = new PartitionedIndexStorage(dirFactory, fileSystem, folder);
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
        IndexWriter indexWriter = firstPartitionWriter();
        for (int i = 0; i < 10; i++) {
            indexWriter.addDocument(createTestDocument());
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

    private static Document createTestDocument() {
        Document document = new Document();
        document.add(new TextField("text", "textValue", Field.Store.YES));
        document.add(new LongPoint("long", 1));
        return document;
    }

    private IndexWriter firstPartitionWriter() {
        List<AbstractIndexPartition> partitions = luceneIndex.getPartitions();
        assertEquals(1, partitions.size());
        AbstractIndexPartition partition = partitions.get(0);
        return partition.getIndexWriter();
    }

    private static class WritableTestDatabaseIndex
            extends WritableDatabaseIndex<TestLuceneIndex, AbstractTextIndexReader> {
        WritableTestDatabaseIndex(PartitionedIndexStorage indexStorage) {
            super(
                    new TestLuceneIndex(indexStorage, new WritableIndexPartitionFactory(() -> {
                        Config config = Config.defaults();
                        return new IndexWriterConfigBuilder(TestIndexWriterModes.STANDARD, config).build();
                    })),
                    writable(),
                    false);
        }
    }

    private static class TestLuceneIndex extends AbstractLuceneIndex<AbstractTextIndexReader> {

        TestLuceneIndex(PartitionedIndexStorage indexStorage, IndexPartitionFactory partitionFactory) {
            super(indexStorage, partitionFactory, null, Config.defaults());
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

    private static class SyncNotifierDirectoryFactory implements DirectoryFactory {
        final CountDownLatch signal;

        SyncNotifierDirectoryFactory(CountDownLatch signal) {
            this.signal = signal;
        }

        @Override
        public Directory open(Path dir) throws IOException {
            Files.createDirectories(dir);
            FSDirectory fsDir = FSDirectory.open(dir);
            return new SyncNotifierDirectory(fsDir, signal);
        }

        @Override
        public void close() {}

        private static class SyncNotifierDirectory extends Directory {
            private final Directory delegate;
            private final CountDownLatch signal;

            SyncNotifierDirectory(Directory delegate, CountDownLatch signal) {
                this.delegate = delegate;
                this.signal = signal;
            }

            @Override
            public String[] listAll() throws IOException {
                return delegate.listAll();
            }

            @Override
            public void deleteFile(String name) throws IOException {
                delegate.deleteFile(name);
            }

            @Override
            public long fileLength(String name) throws IOException {
                return delegate.fileLength(name);
            }

            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                return delegate.createOutput(name, context);
            }

            @Override
            public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
                return delegate.createTempOutput(prefix, suffix, context);
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

                delegate.sync(names);
            }

            @Override
            public void syncMetaData() throws IOException {
                delegate.syncMetaData();
            }

            @Override
            public void rename(String source, String dest) throws IOException {
                delegate.rename(source, dest);
            }

            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                return delegate.openInput(name, context);
            }

            @Override
            public Lock obtainLock(String name) throws IOException {
                return delegate.obtainLock(name);
            }

            @Override
            public void close() throws IOException {
                delegate.close();
            }

            @Override
            public Set<String> getPendingDeletions() throws IOException {
                return delegate.getPendingDeletions();
            }
        }
    }
}
