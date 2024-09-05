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

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceIndexProviderFactory;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.IndexingTestUtil;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.index.schema.CollectingIndexUpdater;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.kernel.recovery.RecoveryMode;
import org.neo4j.kernel.recovery.RecoveryMonitor;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Values;

@EphemeralNeo4jLayoutExtension
class IndexRecoveryIT {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    private GraphDatabaseAPI db;
    private final IndexProvider mockedIndexProvider = mock(IndexProvider.class);
    private final ExtensionFactory<?> mockedIndexProviderFactory =
            singleInstanceIndexProviderFactory(PROVIDER_DESCRIPTOR.getKey(), mockedIndexProvider);
    private final String key = "number_of_bananas_owned";
    private final Label myLabel = label("MyLabel");
    private final Monitors monitors = new Monitors();
    private DatabaseManagementService managementService;
    private ExecutorService executor;
    private final Object lock = new Object();

    @BeforeEach
    void setUp() {
        executor = newSingleThreadExecutor();
        when(mockedIndexProvider.getProviderDescriptor()).thenReturn(PROVIDER_DESCRIPTOR);
        when(mockedIndexProvider.getMinimumRequiredVersion()).thenReturn(KernelVersion.EARLIEST);
        when(mockedIndexProvider.storeMigrationParticipant(
                        any(FileSystemAbstraction.class), any(PageCache.class), any(), any(), any()))
                .thenReturn(StoreMigrationParticipant.NOT_PARTICIPATING);
        when(mockedIndexProvider.completeConfiguration(any(IndexDescriptor.class), any()))
                .then(inv -> inv.getArgument(0));
        when(mockedIndexProvider.getIndexType()).thenReturn(LOOKUP);
        when(mockedIndexProvider.validatePrototype(any(IndexPrototype.class))).thenAnswer(i -> i.getArguments()[0]);
    }

    @AfterEach
    void after() {
        executor.shutdown();
        if (db != null) {
            managementService.shutdown();
        }
    }

    @Test
    void shouldBeAbleToRecoverInTheMiddleOfPopulatingAnIndexWhereLogHasRotated() throws Exception {
        // Given
        startDb();

        Semaphore populationSemaphore = new Semaphore(0);
        Future<Void> killFuture;
        try {
            when(mockedIndexProvider.getPopulator(
                            any(IndexDescriptor.class),
                            any(IndexSamplingConfig.class),
                            any(),
                            any(),
                            any(TokenNameLookup.class),
                            any(),
                            any()))
                    .thenReturn(indexPopulatorWithControlledCompletionTiming(populationSemaphore));
            createSomeData();
            createIndex();

            // And Given
            killFuture = killDbInSeparateThread();
            rotateLogsAndCheckPoint();
        } finally {
            populationSemaphore.release();
        }

        killFuture.get();

        when(mockedIndexProvider.getInitialState(any(IndexDescriptor.class), any(CursorContext.class), any()))
                .thenReturn(InternalIndexState.POPULATING);
        Semaphore recoverySemaphore = new Semaphore(0);
        try {
            when(mockedIndexProvider.getPopulator(
                            any(IndexDescriptor.class),
                            any(IndexSamplingConfig.class),
                            any(),
                            any(),
                            any(TokenNameLookup.class),
                            any(),
                            any()))
                    .thenReturn(indexPopulatorWithControlledCompletionTiming(recoverySemaphore));
            var minimalIndexAccessor = mock(MinimalIndexAccessor.class);
            when(mockedIndexProvider.getMinimalIndexAccessor(any(), anyBoolean()))
                    .thenReturn(minimalIndexAccessor);
            boolean recoveryRequired = Recovery.isRecoveryRequired(fs, databaseLayout, defaults(), INSTANCE);
            monitors.addMonitorListener(new MyRecoveryMonitor(recoverySemaphore));
            // When
            startDb();

            try (Transaction transaction = db.beginTx()) {
                assertThat(transaction.schema().getIndexes(myLabel)).hasSize(1);
                assertThat(transaction.schema().getIndexes(myLabel))
                        .extracting(i -> transaction.schema().getIndexState(i))
                        .containsOnly(Schema.IndexState.POPULATING);
            }
            // in case if kill was not that fast and killed db after flush there will be no need to do recovery and
            // we will not gonna need to get index populators during recovery index service start
            verify(mockedIndexProvider, times(recoveryRequired ? 3 : 2))
                    .getPopulator(
                            any(IndexDescriptor.class),
                            any(IndexSamplingConfig.class),
                            any(),
                            any(),
                            any(TokenNameLookup.class),
                            any(),
                            any());
            verify(mockedIndexProvider, never())
                    .getOnlineAccessor(
                            any(IndexDescriptor.class),
                            any(IndexSamplingConfig.class),
                            any(TokenNameLookup.class),
                            any(),
                            any());
            verify(mockedIndexProvider, times(recoveryRequired ? 2 : 1)).getMinimalIndexAccessor(any(), anyBoolean());
            verify(minimalIndexAccessor, times(recoveryRequired ? 2 : 1)).drop();
        } finally {
            recoverySemaphore.release();
        }
    }

    @Test
    void shouldBeAbleToRecoverInTheMiddleOfPopulatingAnIndex() throws Exception {
        // Given
        final Semaphore populationSemaphore = new Semaphore(0);
        try {
            startDb();
            when(mockedIndexProvider.getPopulator(
                            any(IndexDescriptor.class),
                            any(IndexSamplingConfig.class),
                            any(),
                            any(),
                            any(TokenNameLookup.class),
                            any(),
                            any()))
                    .thenReturn(indexPopulatorWithControlledCompletionTiming(populationSemaphore));
            createSomeData();
            createIndex();
            killDb(populationSemaphore);
        } finally {
            populationSemaphore.release();
        }

        // When
        doReturn(InternalIndexState.POPULATING)
                .when(mockedIndexProvider)
                .getInitialState(any(IndexDescriptor.class), any(CursorContext.class), any());
        // Start on one permit to let recovery through
        final Semaphore recoverySemaphore = new Semaphore(1);
        try {
            doReturn(indexPopulatorWithControlledCompletionTiming(recoverySemaphore))
                    .when(mockedIndexProvider)
                    .getPopulator(
                            any(IndexDescriptor.class),
                            any(IndexSamplingConfig.class),
                            any(),
                            any(),
                            any(TokenNameLookup.class),
                            any(),
                            any());
            var minimalIndexAccessor = mock(MinimalIndexAccessor.class);
            doReturn(minimalIndexAccessor).when(mockedIndexProvider).getMinimalIndexAccessor(any(), anyBoolean());
            startDb();

            try (Transaction transaction = db.beginTx()) {
                assertThat(transaction.schema().getIndexes(myLabel)).hasSize(1);
                assertThat(transaction.schema().getIndexes(myLabel))
                        .extracting(i -> transaction.schema().getIndexState(i))
                        .containsOnly(Schema.IndexState.POPULATING);
            }
            verify(mockedIndexProvider, times(3))
                    .getPopulator(
                            any(IndexDescriptor.class),
                            any(IndexSamplingConfig.class),
                            any(),
                            any(),
                            any(TokenNameLookup.class),
                            any(),
                            any());
            verify(mockedIndexProvider, never())
                    .getOnlineAccessor(
                            any(IndexDescriptor.class),
                            any(IndexSamplingConfig.class),
                            any(TokenNameLookup.class),
                            any(),
                            any());
            // once during recovery and once during startup
            verify(mockedIndexProvider, times(2)).getMinimalIndexAccessor(any(), anyBoolean());
            verify(minimalIndexAccessor, times(2)).drop();
        } finally {
            recoverySemaphore.release();
        }
    }

    @Test
    void shouldBeAbleToRecoverAndUpdateOnlineIndex() throws Exception {
        // Given
        startDb();

        IndexPopulator populator = mock(IndexPopulator.class);
        when(mockedIndexProvider.getPopulator(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(),
                        any(),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(populator);
        when(populator.sample(any(CursorContext.class))).thenReturn(new IndexSample());
        IndexAccessor mockedAccessor = mock(IndexAccessor.class);
        when(mockedAccessor.newUpdater(any(IndexUpdateMode.class), any(CursorContext.class), anyBoolean()))
                .thenReturn(SwallowingIndexUpdater.INSTANCE);
        when(mockedIndexProvider.getOnlineAccessor(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(mockedAccessor);
        createIndexAndAwaitPopulation();
        // rotate logs
        rotateLogsAndCheckPoint();
        // make updates
        Set<IndexEntryUpdate<?>> expectedUpdates = createSomeBananas(myLabel);

        // And Given
        killDb();
        when(mockedIndexProvider.getInitialState(any(IndexDescriptor.class), any(CursorContext.class), any()))
                .thenReturn(ONLINE);
        GatheringIndexWriter writer = new GatheringIndexWriter();
        when(mockedIndexProvider.getOnlineAccessor(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(writer);

        // When
        startDb();

        // Then
        try (Transaction transaction = db.beginTx()) {
            assertThat(transaction.schema().getIndexes(myLabel)).hasSize(1);
            assertThat(transaction.schema().getIndexes(myLabel))
                    .extracting(i -> transaction.schema().getIndexState(i))
                    .containsOnly(Schema.IndexState.ONLINE);
        }
        verify(mockedIndexProvider)
                .getPopulator(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(),
                        any(),
                        any(TokenNameLookup.class),
                        any(),
                        any());
        int onlineAccessorInvocationCount = 3; // once when we create the index, and once when we restart the db
        verify(mockedIndexProvider, times(onlineAccessorInvocationCount))
                .getOnlineAccessor(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(TokenNameLookup.class),
                        any(),
                        any());
        assertEquals(expectedUpdates, writer.batchedUpdates);
    }

    @Test
    void shouldKeepFailedIndexesAsFailedAfterRestart() throws Exception {
        // Given
        IndexPopulator indexPopulator = mock(IndexPopulator.class);
        when(mockedIndexProvider.getPopulator(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(),
                        any(),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(indexPopulator);
        IndexAccessor indexAccessor = mock(IndexAccessor.class);
        when(mockedIndexProvider.getOnlineAccessor(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(indexAccessor);
        startDb();
        createIndex();
        rotateLogsAndCheckPoint();

        // And Given
        killDb();
        when(mockedIndexProvider.getInitialState(any(IndexDescriptor.class), any(CursorContext.class), any()))
                .thenReturn(InternalIndexState.FAILED);

        // When
        startDb();

        // Then
        try (Transaction transaction = db.beginTx()) {
            assertThat(transaction.schema().getIndexes(myLabel)).hasSize(1);
            assertThat(transaction.schema().getIndexes(myLabel))
                    .extracting(i -> transaction.schema().getIndexState(i))
                    .containsOnly(Schema.IndexState.FAILED);
        }
        verify(mockedIndexProvider)
                .getPopulator(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(),
                        any(),
                        any(TokenNameLookup.class),
                        any(),
                        any());
        verify(mockedIndexProvider).getMinimalIndexAccessor(any(IndexDescriptor.class), anyBoolean());
    }

    private void startDb() throws IOException {
        if (db != null) {
            managementService.shutdown();
        }

        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setFileSystem(new UncloseableDelegatingFileSystemAbstraction(fs))
                .addExtension(mockedIndexProviderFactory)
                .noOpSystemGraphInitializer()
                .setMonitors(monitors)
                .setConfig(GraphDatabaseSettings.check_point_interval_time, Duration.ofDays(1))
                .setConfig(GraphDatabaseSettings.check_point_interval_tx, Integer.MAX_VALUE)
                .build();

        db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
    }

    private void killDb(Semaphore populationSemaphore) {
        if (db != null) {
            Path snapshotDir = testDirectory.directory("snapshot");
            snapshotFs(snapshotDir);
            // The index population is waiting for this semaphore, and shutting down the database will
            // wait for the index population, so release the semaphore here.
            populationSemaphore.release();
            managementService.shutdown();
            restoreSnapshot(snapshotDir);
        }
    }

    private void killDb() {
        if (db != null) {
            Path snapshotDir = testDirectory.directory("snapshot");
            synchronized (lock) {
                snapshotFs(snapshotDir);
            }
            managementService.shutdown();
            restoreSnapshot(snapshotDir);
        }
    }

    private void snapshotFs(Path snapshotDir) {
        try {
            DatabaseLayout layout = databaseLayout;
            fs.copyRecursively(layout.databaseDirectory(), snapshotDir.resolve("data"));
            fs.copyRecursively(layout.getTransactionLogsDirectory(), snapshotDir.resolve("transactions"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void restoreSnapshot(Path snapshotDir) {
        try {
            DatabaseLayout layout = databaseLayout;
            fs.deleteRecursively(layout.databaseDirectory());
            fs.deleteRecursively(layout.getTransactionLogsDirectory());
            fs.copyRecursively(snapshotDir.resolve("data"), layout.databaseDirectory());
            fs.copyRecursively(snapshotDir.resolve("transactions"), layout.getTransactionLogsDirectory());
            fs.deleteRecursively(snapshotDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Future<Void> killDbInSeparateThread() {
        return executor.submit(() -> {
            killDb();
            return null;
        });
    }

    private void rotateLogsAndCheckPoint() throws IOException {
        try {
            synchronized (lock) {
                db.getDependencyResolver()
                        .resolveDependency(LogFiles.class)
                        .getLogFile()
                        .getLogRotation()
                        .rotateLogFile(LogAppendEvent.NULL);
                db.getDependencyResolver()
                        .resolveDependency(CheckPointer.class)
                        .forceCheckPoint(new SimpleTriggerInfo("test"));
            }
        } catch (Exception e) {
            if (db.isAvailable()) {
                throw new RuntimeException(e);
            }
        }
    }

    private void createIndexAndAwaitPopulation() throws KernelException {
        IndexDescriptor index = createIndex();
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(index.getName(), 1, MINUTES);
            tx.commit();
        }
    }

    private IndexDescriptor createIndex() throws KernelException {
        try (TransactionImpl tx = (TransactionImpl) db.beginTx()) {
            IndexDescriptor index =
                    IndexingTestUtil.createNodePropIndexWithSpecifiedProvider(tx, PROVIDER_DESCRIPTOR, myLabel, key);
            tx.commit();
            return index;
        }
    }

    private Set<IndexEntryUpdate<?>> createSomeBananas(Label label) {
        Set<IndexEntryUpdate<?>> updates = new HashSet<>();
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

            int labelId = ktx.tokenRead().nodeLabel(label.name());
            int propertyKeyId = ktx.tokenRead().propertyKey(key);
            var schemaDescriptor = SchemaDescriptors.forLabel(labelId, propertyKeyId);
            for (int number : new int[] {4, 10}) {
                Node node = tx.createNode(label);
                node.setProperty(key, number);
                updates.add(IndexEntryUpdate.add(node.getId(), () -> schemaDescriptor, Values.of(number)));
            }
            tx.commit();
            return updates;
        }
    }

    public static class GatheringIndexWriter extends IndexAccessor.Adapter {
        private final Set<IndexEntryUpdate<?>> regularUpdates = new HashSet<>();
        private final Set<IndexEntryUpdate<?>> batchedUpdates = new HashSet<>();

        @Override
        public IndexUpdater newUpdater(final IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
            return new CollectingIndexUpdater(updates -> {
                switch (mode) {
                    case ONLINE -> regularUpdates.addAll(updates);
                    case RECOVERY -> batchedUpdates.addAll(updates);
                    default -> throw new UnsupportedOperationException();
                }
            });
        }
    }

    private static IndexPopulator indexPopulatorWithControlledCompletionTiming(Semaphore semaphore) {
        return new IndexPopulator.Adapter() {
            @Override
            public void create() {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    // fall through and return early
                }
                throw new RuntimeException("this is expected");
            }
        };
    }

    private record MyRecoveryMonitor(Semaphore recoverySemaphore) implements RecoveryMonitor {
        @Override
        public void recoveryCompleted(long recoveryTimeInMilliseconds, RecoveryMode mode) {
            recoverySemaphore.release();
        }
    }

    private void createSomeData() {
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
    }
}
