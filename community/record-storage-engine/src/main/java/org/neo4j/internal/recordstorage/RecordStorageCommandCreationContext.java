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

import static java.lang.Math.toIntExact;

import java.util.function.Supplier;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.recordstorage.RecordAccess.LoadMonitor;
import org.neo4j.internal.recordstorage.id.BatchedTransactionIdSequenceProvider;
import org.neo4j.internal.recordstorage.id.IdSequenceProvider;
import org.neo4j.internal.recordstorage.id.TransactionIdSequenceProvider;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StandardDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Holds commit data structures for creating records in a {@link NeoStores}.
 */
class RecordStorageCommandCreationContext implements CommandCreationContext {
    private final NeoStores neoStores;
    private final Config config;
    private final boolean multiVersioned;
    private final TokenNameLookup tokenNameLookup;
    private final InternalLogProvider logProvider;
    private final int denseNodeThreshold;

    private KernelVersionProvider kernelVersionProvider;
    private PropertyCreator propertyCreator;
    private PropertyDeleter propertyDeleter;
    private RelationshipGroupGetter relationshipGroupGetter;
    private Loaders loaders;
    private CursorContext cursorContext;
    private StoreCursors storeCursors;
    private ResourceLocker locks;
    private final DynamicAllocatorProvider dynamicAllocatorProvider;
    private final IdSequenceProvider transactionSequenceProvider;

    RecordStorageCommandCreationContext(
            NeoStores neoStores,
            TokenNameLookup tokenNameLookup,
            InternalLogProvider logProvider,
            int denseNodeThreshold,
            Config config,
            boolean multiVersioned) {
        this.tokenNameLookup = tokenNameLookup;
        this.logProvider = logProvider;
        this.denseNodeThreshold = denseNodeThreshold;
        this.neoStores = neoStores;
        this.config = config;
        this.multiVersioned = multiVersioned;
        this.transactionSequenceProvider = createIdSequenceProvider(neoStores, multiVersioned);
        this.dynamicAllocatorProvider = new TransactionDynamicAllocatorProvider(neoStores, transactionSequenceProvider);
    }

    @Override
    public void initialize(
            KernelVersionProvider kernelVersionProvider,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            Supplier<Long> startTimeOfOldestActiveTransaction,
            ResourceLocker locks,
            Supplier<LockTracer> lockTracer) {
        this.kernelVersionProvider = kernelVersionProvider;
        this.cursorContext = cursorContext;
        this.loaders = new Loaders(neoStores, storeCursors);
        this.storeCursors = storeCursors;
        this.locks = locks;
        this.relationshipGroupGetter =
                new RelationshipGroupGetter(ignored -> nextId(StoreType.RELATIONSHIP_GROUP), cursorContext);
        PropertyTraverser propertyTraverser = new PropertyTraverser();
        this.propertyDeleter = new PropertyDeleter(
                propertyTraverser, neoStores, tokenNameLookup, logProvider, config, cursorContext, storeCursors);
        this.propertyCreator = new PropertyCreator(
                dynamicAllocatorProvider.allocator(StoreType.PROPERTY_STRING),
                dynamicAllocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                propertyTraverser,
                transactionSequenceProvider,
                cursorContext);
    }

    private long nextId(StoreType storeType) {
        return transactionSequenceProvider.getIdSequence(storeType).nextId(cursorContext);
    }

    ResourceLocker getLocks() {
        return locks;
    }

    @Override
    public long reserveNode() {
        return nextId(StoreType.NODE);
    }

    @Override
    public long reserveRelationship(
            long sourceNode,
            long targetNode,
            int relationshipType,
            boolean sourceNodeAddedInTx,
            boolean targetNodeAddedInTx) {
        return nextId(StoreType.RELATIONSHIP);
    }

    @Override
    public long reserveSchema() {
        return nextId(StoreType.SCHEMA);
    }

    @Override
    public int reserveRelationshipTypeTokenId() {
        return toIntExact(nextId(StoreType.RELATIONSHIP_TYPE_TOKEN));
    }

    @Override
    public int reservePropertyKeyTokenId() {
        return toIntExact(nextId(StoreType.PROPERTY_KEY_TOKEN));
    }

    @Override
    public int reserveLabelTokenId() {
        return toIntExact(nextId(StoreType.LABEL_TOKEN));
    }

    @Override
    public void close() {
        transactionSequenceProvider.release(cursorContext);
    }

    TransactionRecordState createTransactionRecordState(
            ResourceLocker locks,
            LockTracer lockTracer,
            LogCommandSerialization commandSerialization,
            MemoryTracker memoryTracker,
            LoadMonitor monitor) {
        RecordChangeSet recordChangeSet = new RecordChangeSet(loaders, memoryTracker, monitor, storeCursors);
        var relationshipLocker =
                multiVersioned ? new MultiversionResourceLocker(locks, neoStores.getRelationshipStore()) : locks;
        RelationshipModifier relationshipModifier = new RelationshipModifier(
                relationshipGroupGetter,
                propertyDeleter,
                denseNodeThreshold,
                relationshipLocker,
                lockTracer,
                cursorContext,
                memoryTracker);
        return new TransactionRecordState(
                kernelVersionProvider,
                recordChangeSet,
                neoStores,
                locks,
                lockTracer,
                relationshipModifier,
                propertyCreator,
                propertyDeleter,
                cursorContext,
                storeCursors,
                memoryTracker,
                commandSerialization,
                dynamicAllocatorProvider,
                transactionSequenceProvider);
    }

    @Override
    public KernelVersion kernelVersion() {
        return kernelVersionProvider.kernelVersion();
    }

    private static class TransactionDynamicAllocatorProvider implements DynamicAllocatorProvider {
        private final StandardDynamicRecordAllocator[] dynamicAllocators =
                new StandardDynamicRecordAllocator[StoreType.STORE_TYPES.length];
        private final IdSequenceProvider transactionSequenceProvider;
        private final NeoStores neoStores;

        public TransactionDynamicAllocatorProvider(
                NeoStores neoStores, IdSequenceProvider transactionSequenceProvider) {
            this.neoStores = neoStores;
            this.transactionSequenceProvider = transactionSequenceProvider;
        }

        @Override
        public DynamicRecordAllocator allocator(StoreType type) {
            StandardDynamicRecordAllocator allocator = dynamicAllocators[type.ordinal()];
            if (allocator != null) {
                return allocator;
            }

            var newAllocator = new StandardDynamicRecordAllocator(
                    cursorContext ->
                            transactionSequenceProvider.getIdSequence(type).nextId(cursorContext),
                    neoStores.getRecordStore(type).getRecordDataSize());
            dynamicAllocators[type.ordinal()] = newAllocator;
            return newAllocator;
        }
    }

    private static IdSequenceProvider createIdSequenceProvider(NeoStores neoStores, boolean multiVersioned) {
        return multiVersioned
                ? new BatchedTransactionIdSequenceProvider(neoStores)
                : new TransactionIdSequenceProvider(neoStores);
    }
}
