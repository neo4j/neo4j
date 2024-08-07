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
import java.util.stream.Collectors;
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
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;
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

        assertThatThrownBy(() -> engine.apply(storageEngineTransaction, TransactionApplicationMode.INTERNAL))
                .rootCause()
                .isEqualTo(failure);

        verify(databaseHealth).panic(any(Throwable.class));
    }

    private static TransactionApplierFactoryChain transactionApplierFacadeTransformer(
            TransactionApplierFactoryChain facade, Exception failure) {
        return new CapturingTransactionApplierFactoryChain(value -> {
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
        Collection<StoreFileMetadata> atomicFiles = new ArrayList<>();
        Collection<StoreFileMetadata> replayableFiles = new ArrayList<>();
        engine.listStorageFiles(atomicFiles, replayableFiles);
        Collection<StoreFileMetadata> allFiles = new ArrayList<>();
        allFiles.addAll(atomicFiles);
        allFiles.addAll(replayableFiles);
        Set<Path> currentFiles = allFiles.stream().map(StoreFileMetadata::path).collect(Collectors.toSet());

        // then
        Set<Path> allPossibleFiles = new HashSet<>(databaseLayout.storeFiles());
        allPossibleFiles.remove(databaseLayout.indexStatisticsStore());

        assertEquals(allPossibleFiles, currentFiles);
        assertThat(atomicFiles.stream().map(StoreFileMetadata::path).collect(Collectors.toSet()))
                .isEqualTo(Set.of(databaseLayout.countStore(), databaseLayout.relationshipGroupDegreesStore()));
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
        CapturingTransactionApplierFactoryChain applier = new CapturingTransactionApplierFactoryChain(applierCloseCall);
        RecordStorageEngine engine = recordStorageEngineBuilder()
                .lockService(lockService)
                .transactionApplierTransformer(applier::wrapAroundActualApplier)
                .build();
        try (StoreCursors storageCursors = engine.createStorageCursors(NULL_CONTEXT)) {
            StorageEngineTransaction storageEngineTransaction = mock(StorageEngineTransaction.class);
            when(storageEngineTransaction.cursorContext()).thenReturn(NULL_CONTEXT);
            when(storageEngineTransaction.storeCursors()).thenReturn(storageCursors);
            when(storageEngineTransaction.commandBatch()).thenReturn(mock(CommandBatch.class));
            when(storageEngineTransaction.accept(any())).thenAnswer(invocationOnMock -> {
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
        StorageEngineTransaction transaction = mock(StorageEngineTransaction.class);
        doThrow(error).when(transaction).accept(any());
        long txId = ThreadLocalRandom.current().nextLong(0, 1000);
        when(transaction.transactionId()).thenReturn(txId);
        return transaction;
    }

    private static class CapturingTransactionApplierFactoryChain extends TransactionApplierFactoryChain {
        private final Consumer<Boolean> applierCloseCall;
        private TransactionApplierFactoryChain actual;

        CapturingTransactionApplierFactoryChain(Consumer<Boolean> applierCloseCall) {
            super(IdGeneratorUpdatesWorkSync::newBatch);
            this.applierCloseCall = applierCloseCall;
        }

        CapturingTransactionApplierFactoryChain wrapAroundActualApplier(TransactionApplierFactoryChain actual) {
            this.actual = actual;
            return this;
        }

        @Override
        public TransactionApplier startTx(StorageEngineTransaction transaction, BatchContext batchContext)
                throws IOException {
            final TransactionApplier transactionApplier = actual.startTx(transaction, batchContext);
            return new TransactionApplier() {
                @Override
                public boolean visit(StorageCommand element) throws IOException {
                    return transactionApplier.visit(element);
                }

                @Override
                public boolean visitNodeCommand(Command.NodeCommand command) throws IOException {
                    return transactionApplier.visitNodeCommand(command);
                }

                @Override
                public boolean visitRelationshipCommand(Command.RelationshipCommand command) throws IOException {
                    return transactionApplier.visitRelationshipCommand(command);
                }

                @Override
                public boolean visitPropertyCommand(Command.PropertyCommand command) throws IOException {
                    return transactionApplier.visitPropertyCommand(command);
                }

                @Override
                public boolean visitRelationshipGroupCommand(Command.RelationshipGroupCommand command)
                        throws IOException {
                    return transactionApplier.visitRelationshipGroupCommand(command);
                }

                @Override
                public boolean visitRelationshipTypeTokenCommand(Command.RelationshipTypeTokenCommand command)
                        throws IOException {
                    return transactionApplier.visitRelationshipTypeTokenCommand(command);
                }

                @Override
                public boolean visitLabelTokenCommand(Command.LabelTokenCommand command) throws IOException {
                    return transactionApplier.visitLabelTokenCommand(command);
                }

                @Override
                public boolean visitPropertyKeyTokenCommand(Command.PropertyKeyTokenCommand command)
                        throws IOException {
                    return transactionApplier.visitPropertyKeyTokenCommand(command);
                }

                @Override
                public boolean visitSchemaRuleCommand(Command.SchemaRuleCommand command) throws IOException {
                    return transactionApplier.visitSchemaRuleCommand(command);
                }

                @Override
                public boolean visitNodeCountsCommand(Command.NodeCountsCommand command) throws IOException {
                    return transactionApplier.visitNodeCountsCommand(command);
                }

                @Override
                public boolean visitRelationshipCountsCommand(Command.RelationshipCountsCommand command)
                        throws IOException {
                    return transactionApplier.visitRelationshipCountsCommand(command);
                }

                @Override
                public boolean visitMetaDataCommand(Command.MetaDataCommand command) throws IOException {
                    return transactionApplier.visitMetaDataCommand(command);
                }

                @Override
                public boolean visitGroupDegreeCommand(Command.GroupDegreeCommand command) throws IOException {
                    return transactionApplier.visitGroupDegreeCommand(command);
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
