/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage;

import static java.lang.Math.toIntExact;

import java.util.function.BooleanSupplier;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
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
    private final MemoryTracker memoryTracker;
    private final PropertyStore propertyStore;
    private final TokenNameLookup tokenNameLookup;
    private final InternalLogProvider logProvider;
    private final int denseNodeThreshold;
    // The setting for relaxed dense node locking is a supplier since the command creation context instances are created
    // once per
    // kernel transaction object and so will be reused between transactions. The relaxed locking feature may change from
    // tx to tx
    // and so it will need to be queried per tx commit.
    private final BooleanSupplier relaxedLockingForDenseNodes;

    private PropertyCreator propertyCreator;
    private PropertyDeleter propertyDeleter;
    private RelationshipGroupGetter relationshipGroupGetter;
    private Loaders loaders;
    private CursorContext cursorContext;
    private StoreCursors storeCursors;

    RecordStorageCommandCreationContext(
            NeoStores neoStores,
            TokenNameLookup tokenNameLookup,
            InternalLogProvider logProvider,
            int denseNodeThreshold,
            BooleanSupplier relaxedLockingForDenseNodes,
            Config config,
            MemoryTracker memoryTracker) {
        this.tokenNameLookup = tokenNameLookup;
        this.logProvider = logProvider;
        this.denseNodeThreshold = denseNodeThreshold;
        this.relaxedLockingForDenseNodes = relaxedLockingForDenseNodes;
        this.neoStores = neoStores;
        this.config = config;
        this.memoryTracker = memoryTracker;
        this.propertyStore = neoStores.getPropertyStore();
    }

    @Override
    public void initialize(CursorContext cursorContext, StoreCursors storeCursors) {
        this.cursorContext = cursorContext;
        this.loaders = new Loaders(neoStores, storeCursors);
        this.storeCursors = storeCursors;
        this.relationshipGroupGetter =
                new RelationshipGroupGetter(neoStores.getRelationshipGroupStore(), cursorContext);
        PropertyTraverser propertyTraverser = new PropertyTraverser();
        this.propertyDeleter = new PropertyDeleter(
                propertyTraverser,
                neoStores,
                tokenNameLookup,
                logProvider,
                config,
                cursorContext,
                memoryTracker,
                storeCursors);
        this.propertyCreator = new PropertyCreator(
                new StandardDynamicRecordAllocator(
                        propertyStore.getStringStore(),
                        propertyStore.getStringStore().getRecordDataSize()),
                new StandardDynamicRecordAllocator(
                        propertyStore.getArrayStore(),
                        propertyStore.getArrayStore().getRecordDataSize()),
                propertyStore,
                propertyTraverser,
                cursorContext,
                memoryTracker);
    }

    private long nextId(StoreType storeType) {
        return neoStores.getRecordStore(storeType).nextId(cursorContext);
    }

    @Override
    public long reserveNode() {
        return nextId(StoreType.NODE);
    }

    @Override
    public long reserveRelationship(long sourceNode) {
        return nextId(StoreType.RELATIONSHIP);
    }

    @Override
    public long reserveSchema() {
        return nextId(StoreType.SCHEMA);
    }

    @Override
    public int reserveRelationshipTypeTokenId() {
        return toIntExact(neoStores.getRelationshipTypeTokenStore().nextId(cursorContext));
    }

    @Override
    public int reservePropertyKeyTokenId() {
        return toIntExact(neoStores.getPropertyKeyTokenStore().nextId(cursorContext));
    }

    @Override
    public int reserveLabelTokenId() {
        return toIntExact(neoStores.getLabelTokenStore().nextId(cursorContext));
    }

    @Override
    public void close() {}

    TransactionRecordState createTransactionRecordState(
            IntegrityValidator integrityValidator,
            long lastTransactionIdWhenStarted,
            ResourceLocker locks,
            LockTracer lockTracer,
            LogCommandSerialization commandSerialization,
            RecordAccess.LoadMonitor monitor) {
        RecordChangeSet recordChangeSet = new RecordChangeSet(loaders, memoryTracker, monitor, storeCursors);
        RelationshipModifier relationshipModifier = new RelationshipModifier(
                relationshipGroupGetter,
                propertyDeleter,
                denseNodeThreshold,
                relaxedLockingForDenseNodes.getAsBoolean(),
                cursorContext,
                memoryTracker);
        return new TransactionRecordState(
                neoStores,
                integrityValidator,
                recordChangeSet,
                lastTransactionIdWhenStarted,
                locks,
                lockTracer,
                relationshipModifier,
                propertyCreator,
                propertyDeleter,
                cursorContext,
                storeCursors,
                memoryTracker,
                commandSerialization);
    }
}
