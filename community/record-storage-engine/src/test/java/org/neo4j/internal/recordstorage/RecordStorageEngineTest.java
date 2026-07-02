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
package org.neo4j.internal.recordstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.lock.LockType.EXCLUSIVE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseFile;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.lock.Lock;
import org.neo4j.lock.LockService;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.StorageFileSelection;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.storage.RecordStorageEngineSupport;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class RecordStorageEngineTest {
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private PageCache pageCache;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    private final DatabaseHealth databaseHealth = mock(DatabaseHealth.class);
    private final RecordStorageEngineSupport storageEngineRule = new RecordStorageEngineSupport();

    @BeforeEach
    void before() throws Throwable {
        storageEngineRule.before();
    }

    @AfterEach
    void after() throws Throwable {
        storageEngineRule.after(true);
    }

    @Test
    @Timeout(30)
    void shutdownRecordStorageEngineAfterFailedTransaction() throws Exception {
        RecordStorageEngine engine = buildRecordStorageEngine();
        Exception applicationError = executeFailingTransaction(engine);
        assertNotNull(applicationError);
    }

    @Test
    void panicOnExceptionDuringCommandsApply() {
        IllegalStateException failure = new IllegalStateException("Too many open files");
        RecordStorageEngine engine = storageEngineRule
                .getWith(fs, pageCache, databaseLayout)
                .databaseHealth(databaseHealth)
                .transactionApplierTransformer(facade -> transactionApplierFacadeTransformer(facade, failure))
                .build();
        StorageEngineTransaction storageEngineTransaction = mock(StorageEngineTransaction.class);
        CommandBatch commandBatch = mock(CommandBatch.class);
        when(commandBatch.commandCount()).thenReturn(1);
        when(storageEngineTransaction.commandBatch()).thenReturn(commandBatch);

        assertThatThrownBy(() -> engine.apply(storageEngineTransaction, TransactionApplicationMode.INTERNAL))
                .rootCause()
                .isEqualTo(failure);

        verify(databaseHealth).panic(any(Throwable.class));
    }

    private static TransactionAppliersDispatcherFactory transactionApplierFacadeTransformer(
            TransactionAppliersDispatcherFactory facade, Exception failure) {
        return new CapturingTransactionAppliersDispatcherFactory(value -> {
                    throw new RuntimeException(failure);
                })
                .wrapAroundActualApplier(facade);
    }

    @Test
    void databasePanicIsRaisedWhenTxApplicationFails() throws Throwable {
        RecordStorageEngine engine = buildRecordStorageEngine();
        Exception applicationError = executeFailingTransaction(engine);
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(databaseHealth).panic(captor.capture());
        Throwable exception = captor.getValue();
        if (exception instanceof KernelException) {
            assertThat(((KernelException) exception).status()).isEqualTo(Status.General.UnknownError);
            exception = exception.getCause();
        }
        assertThat(exception).isEqualTo(applicationError);
    }

    @Test
    void shouldListAllStoreFilesWithEnabledRelaxedLocking() {
        RecordStorageEngine engine = recordStorageEngineBuilder().build();

        // when
        Collection<Path> allFiles = engine.listStorageFiles(new StorageFileSelection(true, true, false));
        Set<Path> currentFiles = new HashSet<>(allFiles);

        // then
        Set<Path> allPossibleFiles =
                new HashSet<>(engine.listStorageFiles(new StorageFileSelection(true, true, false)));
        allPossibleFiles.removeAll(databaseLayout.indexStatisticsStore().allSegments(fs));

        assertEquals(allPossibleFiles, currentFiles);
        Collection<Path> atomicFiles = engine.listStorageFiles(new StorageFileSelection(true, false, false));
        var expectedAtomicFiles = new ArrayList<>(databaseLayout.countStore().allSegments(fs));
        expectedAtomicFiles.addAll(
                databaseLayout.relationshipGroupDegreesStore().allSegments(fs));
        assertThat(atomicFiles).containsOnlyOnceElementsOf(expectedAtomicFiles);
    }

    @Test
    void shouldCloseLockGroupAfterAppliers() throws Exception {
        // given
        long nodeId = 5;
        LockService lockService = mock(LockService.class);
        Lock nodeLock = mock(Lock.class);
        when(lockService.acquireNodeLock(nodeId, EXCLUSIVE)).thenReturn(nodeLock);
        Consumer<Boolean> applierCloseCall =
                mock(Consumer.class); // <-- simply so that we can use InOrder mockito construct
        CapturingTransactionAppliersDispatcherFactory applier =
                new CapturingTransactionAppliersDispatcherFactory(applierCloseCall);
        RecordStorageEngine engine = recordStorageEngineBuilder()
                .lockService(lockService)
                .transactionApplierTransformer(applier::wrapAroundActualApplier)
                .build();
        try (StoreCursors storageCursors = engine.createStorageCursors(NULL_CONTEXT)) {
            StorageEngineTransaction storageEngineTransaction = mock(StorageEngineTransaction.class);
            when(storageEngineTransaction.cursorContext()).thenReturn(NULL_CONTEXT);
            when(storageEngineTransaction.storeCursors()).thenReturn(storageCursors);
            var commandBatch = mock(CommandBatch.class);
            when(commandBatch.commandCount()).thenReturn(1);
            when(storageEngineTransaction.commandBatch()).thenReturn(commandBatch);
            when(commandBatch.accept(any())).thenAnswer(invocationOnMock -> {
                // Visit one node command
                Visitor<StorageCommand, IOException> visitor = invocationOnMock.getArgument(0);
                NodeRecord after = new NodeRecord(nodeId);
                after.setInUse(true);
                visitor.visit(new Command.NodeCommand(
                        RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION),
                        new NodeRecord(nodeId),
                        after));
                return null;
            });
            // when
            engine.apply(storageEngineTransaction, TransactionApplicationMode.INTERNAL);

            // then
            InOrder inOrder = inOrder(lockService, applierCloseCall, nodeLock);
            inOrder.verify(lockService).acquireNodeLock(nodeId, EXCLUSIVE);
            inOrder.verify(applierCloseCall).accept(true);
            inOrder.verify(nodeLock).release();
            inOrder.verifyNoMoreInteractions();
        }
    }

    @Test
    void listStorageFilesContainsUniqueEntries() throws IOException {
        // Populate the layout
        var engine = buildRecordStorageEngine();
        engine.shutdown();

        RecordStorageEngineFactory engineFactory = new RecordStorageEngineFactory();
        var files = engineFactory.listStorageFiles(fs, databaseLayout);
        assertThat(files).isEqualTo(files.stream().distinct().toList());
    }

    @Test
    void listStorageFilesContainsExistsMarker() throws IOException {
        // Populate the layout
        var engine = buildRecordStorageEngine();
        engine.shutdown();

        RecordStorageEngineFactory engineFactory = new RecordStorageEngineFactory();
        var files = engineFactory.listStorageFiles(fs, databaseLayout);

        Function<Path, String> stringify =
                (pth) -> databaseLayout.databaseDirectory().relativize(pth).toString();

        assertThat(files.stream().map(stringify)).contains(RecordDatabaseFile.EXISTS_MARKER.getName());
    }

    private RecordStorageEngine buildRecordStorageEngine() {
        return recordStorageEngineBuilder().build();
    }

    private RecordStorageEngineSupport.Builder recordStorageEngineBuilder() {
        return storageEngineRule.getWith(fs, pageCache, databaseLayout).databaseHealth(databaseHealth);
    }

    private static Exception executeFailingTransaction(RecordStorageEngine engine) throws IOException {
        Exception applicationError = new UnderlyingStorageException("No space left on device");
        StorageEngineTransaction txToApply = newTransactionThatFailsWith(applicationError);
        assertThatThrownBy(() -> engine.apply(txToApply, TransactionApplicationMode.INTERNAL))
                .rootCause()
                .isSameAs(applicationError);
        return applicationError;
    }

    private static StorageEngineTransaction newTransactionThatFailsWith(Exception error) throws IOException {
        var transaction = mock(StorageEngineTransaction.class);
        var commandBatch = mock(CommandBatch.class);
        when(transaction.commandBatch()).thenReturn(commandBatch);
        when(commandBatch.commandCount()).thenReturn(1);
        doThrow(error).when(commandBatch).accept(any());
        long txId = ThreadLocalRandom.current().nextLong(0, 1000);
        when(transaction.transactionId()).thenReturn(txId);
        return transaction;
    }

    private static class CapturingTransactionAppliersDispatcherFactory extends TransactionAppliersDispatcherFactory {
        private final Consumer<Boolean> applierCloseCall;
        private TransactionAppliersDispatcherFactory actual;

        CapturingTransactionAppliersDispatcherFactory(Consumer<Boolean> applierCloseCall) {
            super((workSync, cursorContext) -> workSync.newBatch(cursorContext, false));
            this.applierCloseCall = applierCloseCall;
        }

        CapturingTransactionAppliersDispatcherFactory wrapAroundActualApplier(
                TransactionAppliersDispatcherFactory actual) {
            this.actual = actual;
            return this;
        }

        @Override
        public TransactionAppliersDispatcher startTx(StorageEngineTransaction transaction, BatchContext batchContext)
                throws IOException {
            final var transactionApplier = actual.startTx(transaction, batchContext);
            return new TransactionAppliersDispatcher() {
                @Override
                public boolean visit(StorageCommand element) throws IOException {
                    return transactionApplier.visit(element);
                }

                @Override
                public void close() throws Exception {
                    applierCloseCall.accept(true);
                    transactionApplier.close();
                }
            };
        }
    }
}
