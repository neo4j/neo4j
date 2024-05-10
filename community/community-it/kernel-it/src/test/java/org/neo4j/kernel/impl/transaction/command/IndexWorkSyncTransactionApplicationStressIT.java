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
package org.neo4j.kernel.impl.transaction.command;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.TimeUtil.parseTimeMillis;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.index.schema.RangeIndexProvider.DESCRIPTOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.NO_DECORATION;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.batchimport.cache.idmapping.string.Workers;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Commands;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.CompleteTransaction;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.storageengine.api.CommandBatchToApply;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.storage.RecordStorageEngineSupport;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@PageCacheExtension
class IndexWorkSyncTransactionApplicationStressIT {
    private static final LabelSchemaDescriptor descriptor = SchemaDescriptors.forLabel(0, 0);

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    private final RecordStorageEngineSupport storageEngineRule = new RecordStorageEngineSupport();
    private TransactionCommitmentFactory commitmentFactory;
    private IdStoreTransactionIdGenerator transactionIdGenerator;

    @BeforeEach
    void setUp() throws Throwable {
        storageEngineRule.before();
        SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        commitmentFactory = new TransactionCommitmentFactory(transactionIdStore);
        transactionIdGenerator = new IdStoreTransactionIdGenerator(transactionIdStore);
    }

    @AfterEach
    void tearDown() throws Throwable {
        storageEngineRule.after(false);
    }

    @Test
    void shouldApplyIndexUpdatesInWorkSyncedBatches() throws Exception {
        // GIVEN
        long duration = parseTimeMillis.apply(System.getProperty(getClass().getName() + ".duration", "2s"));
        int numThreads = Integer.getInteger(
                getClass().getName() + ".numThreads", Runtime.getRuntime().availableProcessors());
        CollectingIndexUpdateListener index = new CollectingIndexUpdateListener();
        RecordStorageEngine storageEngine = storageEngineRule
                .getWith(fileSystem, pageCache, RecordDatabaseLayout.ofFlat(directory.directory(DEFAULT_DATABASE_NAME)))
                .indexUpdateListener(index)
                .build();
        try (var storageCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
            storageEngine.apply(
                    tx(
                            singletonList(Commands.createIndexRule(DESCRIPTOR, 1, descriptor)),
                            storageCursors,
                            commitmentFactory,
                            transactionIdGenerator),
                    EXTERNAL);
        }

        // WHEN
        Workers<Worker> workers = new Workers<>(getClass().getSimpleName());
        final AtomicBoolean end = new AtomicBoolean();
        for (int i = 0; i < numThreads; i++) {
            workers.start(new Worker(i, end, storageEngine, 10, index, commitmentFactory, transactionIdGenerator));
        }

        // let the threads hammer the storage engine for some time
        Thread.sleep(duration);
        end.set(true);

        // THEN (assertions as part of the workers applying transactions)
        workers.awaitAndThrowOnError();
    }

    private static Value propertyValue(int id, int progress) {
        return Values.of(id + "_" + progress);
    }

    private static TransactionToApply tx(
            List<StorageCommand> commands,
            StoreCursors storeCursors,
            TransactionCommitmentFactory commitmentFactory,
            TransactionIdGenerator transactionIdGenerator) {
        CompleteTransaction txRepresentation = new CompleteTransaction(
                commands, UNKNOWN_CONSENSUS_INDEX, -1, -1, -1, -1, LatestVersions.LATEST_KERNEL_VERSION, ANONYMOUS);
        TransactionToApply tx = new TransactionToApply(
                txRepresentation,
                NULL_CONTEXT,
                storeCursors,
                commitmentFactory.newCommitment(),
                transactionIdGenerator);
        tx.batchAppended(2, new LogPosition(1, 2), new LogPosition(3, 4), 1);
        return tx;
    }

    private static class Worker implements Runnable {
        private final int id;
        private final AtomicBoolean end;
        private final RecordStorageEngine storageEngine;
        private final NodeStore nodeIds;
        private final int batchSize;
        private final CollectingIndexUpdateListener index;
        private final TransactionCommitmentFactory commitmentFactory;
        private final TransactionIdGenerator transactionIdGenerator;
        private int i;
        private int base;

        Worker(
                int id,
                AtomicBoolean end,
                RecordStorageEngine storageEngine,
                int batchSize,
                CollectingIndexUpdateListener index,
                TransactionCommitmentFactory commitmentFactory,
                TransactionIdGenerator transactionIdGenerator) {
            this.id = id;
            this.end = end;
            this.storageEngine = storageEngine;
            this.batchSize = batchSize;
            this.index = index;
            this.commitmentFactory = commitmentFactory;
            this.transactionIdGenerator = transactionIdGenerator;
            NeoStores neoStores = this.storageEngine.testAccessNeoStores();
            this.nodeIds = neoStores.getNodeStore();
        }

        @Override
        public void run() {
            try (StorageReader reader = storageEngine.newReader();
                    CommandCreationContext creationContext = storageEngine.newCommandCreationContext(false);
                    var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
                creationContext.initialize(
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        NULL_CONTEXT,
                        storeCursors,
                        CommandCreationContext.NO_STARTTIME_OF_OLDEST_TRANSACTION,
                        ResourceLocker.IGNORE,
                        () -> LockTracer.NONE);
                TransactionQueue queue = new TransactionQueue(batchSize, tx -> {
                    // Apply
                    storageEngine.apply(tx, EXTERNAL);

                    // And verify that all nodes are in the index
                    verifyIndex(tx);
                    base += batchSize;
                });
                for (; !end.get(); i++) {
                    queue.queue(createNodeAndProperty(
                            i, reader, creationContext, storeCursors, commitmentFactory, transactionIdGenerator));
                }
                queue.applyTransactions();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private TransactionToApply createNodeAndProperty(
                int progress,
                StorageReader reader,
                CommandCreationContext creationContext,
                StoreCursors storeCursors,
                TransactionCommitmentFactory commitmentFactory,
                TransactionIdGenerator transactionIdGenerator)
                throws Exception {
            TransactionState txState = new TxState();
            long nodeId = nodeIds.getIdGenerator().nextId(NULL_CONTEXT);
            txState.nodeDoCreate(nodeId);
            txState.nodeDoAddLabel(descriptor.getLabelId(), nodeId);
            txState.nodeDoAddProperty(nodeId, descriptor.getPropertyId(), propertyValue(id, progress));
            List<StorageCommand> commands = storageEngine.createCommands(
                    txState,
                    reader,
                    creationContext,
                    LockTracer.NONE,
                    NO_DECORATION,
                    NULL_CONTEXT,
                    storeCursors,
                    INSTANCE);
            return tx(commands, storeCursors, commitmentFactory, transactionIdGenerator);
        }

        private void verifyIndex(CommandBatchToApply tx) throws Exception {
            NodeVisitor visitor = new NodeVisitor();
            for (int i = 0; tx != null; i++) {
                tx.commandBatch().accept(visitor.clear());

                Value propertyValue = propertyValue(id, base + i);
                index.assertHasIndexEntry(propertyValue, visitor.nodeId);
                tx = tx.next();
            }
        }
    }

    private static class NodeVisitor implements Visitor<StorageCommand, IOException> {
        long nodeId;

        @Override
        public boolean visit(StorageCommand element) {
            if (element instanceof NodeCommand) {
                nodeId = ((NodeCommand) element).getKey();
            }
            return false;
        }

        public NodeVisitor clear() {
            nodeId = -1;
            return this;
        }
    }

    private static class CollectingIndexUpdateListener extends IndexUpdateListener.Adapter {
        // Only one index assumed
        private final ConcurrentMap<Value, Set<Long>> index = new ConcurrentHashMap<>();

        @Override
        public void applyUpdates(
                Iterable<IndexEntryUpdate<IndexDescriptor>> updates, CursorContext cursorContext, boolean parallel) {
            updates.forEach(rawUpdate -> {
                // Only additions assumed
                assert rawUpdate.updateMode() == UpdateMode.ADDED;
                ValueIndexEntryUpdate<?> update = (ValueIndexEntryUpdate<?>) rawUpdate;
                index.computeIfAbsent(update.values()[0], value -> ConcurrentHashMap.newKeySet())
                        .add(update.getEntityId());
            });
        }

        void assertHasIndexEntry(Value value, long entityId) {
            assertTrue(index.getOrDefault(value, emptySet()).contains(entityId));
        }
    }
}
