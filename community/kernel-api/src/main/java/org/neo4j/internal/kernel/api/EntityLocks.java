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
package org.neo4j.internal.kernel.api;

import java.util.function.Supplier;
import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.storageengine.api.StorageLocks;

public class EntityLocks implements Locks {

    private final StorageLocks storageLocks;
    private final Supplier<LockTracer> lockTracer;
    private final LockManager.Client lockClient;
    private final AssertOpen assertOpen;

    public EntityLocks(
            StorageLocks storageLocks,
            Supplier<LockTracer> lockTracer,
            LockManager.Client lockClient,
            AssertOpen assertOpen) {
        this.storageLocks = storageLocks;
        this.lockTracer = lockTracer;
        this.lockClient = lockClient;
        this.assertOpen = assertOpen;
    }

    private void performCheckBeforeOperation() {
        assertOpen.assertOpen();
    }

    @Override
    public void acquireExclusiveNodeLock(long... ids) {
        performCheckBeforeOperation();
        storageLocks.acquireExclusiveNodeLock(lockTracer.get(), ids);
    }

    @Override
    public void acquireExclusiveRelationshipLock(long... ids) {
        performCheckBeforeOperation();
        storageLocks.acquireExclusiveRelationshipLock(lockTracer.get(), ids);
    }

    @Override
    public void releaseExclusiveNodeLock(long... ids) {
        performCheckBeforeOperation();
        storageLocks.releaseExclusiveNodeLock(ids);
    }

    @Override
    public void releaseExclusiveRelationshipLock(long... ids) {
        performCheckBeforeOperation();
        storageLocks.releaseExclusiveRelationshipLock(ids);
    }

    @Override
    public void acquireSharedNodeLock(long... ids) {
        performCheckBeforeOperation();
        storageLocks.acquireSharedNodeLock(lockTracer.get(), ids);
    }

    @Override
    public void acquireSharedRelationshipLock(long... ids) {
        performCheckBeforeOperation();
        storageLocks.acquireSharedRelationshipLock(lockTracer.get(), ids);
    }

    @Override
    public void acquireSharedRelationshipTypeLock(long... ids) {
        performCheckBeforeOperation();
        lockClient.acquireShared(lockTracer.get(), ResourceType.RELATIONSHIP_TYPE, ids);
    }

    @Override
    public void acquireSharedLabelLock(long... ids) {
        performCheckBeforeOperation();
        lockClient.acquireShared(lockTracer.get(), ResourceType.LABEL, ids);
    }

    @Override
    public void releaseSharedNodeLock(long... ids) {
        performCheckBeforeOperation();
        storageLocks.releaseSharedNodeLock(ids);
    }

    @Override
    public void releaseSharedRelationshipLock(long... ids) {
        performCheckBeforeOperation();
        storageLocks.releaseSharedRelationshipLock(ids);
    }

    @Override
    public void releaseSharedLabelLock(long... ids) {
        performCheckBeforeOperation();
        lockClient.releaseShared(ResourceType.LABEL, ids);
    }

    @Override
    public void releaseSharedRelationshipTypeLock(long... ids) {
        performCheckBeforeOperation();
        lockClient.releaseShared(ResourceType.RELATIONSHIP_TYPE, ids);
    }

    @Override
    public void acquireSharedLookupLock(EntityType entityType) {
        performCheckBeforeOperation();
        acquireSharedSchemaLock(() -> SchemaDescriptors.forAnyEntityTokens(entityType));
    }

    @Override
    public void releaseSharedLookupLock(EntityType entityType) {
        performCheckBeforeOperation();
        releaseSharedSchemaLock(() -> SchemaDescriptors.forAnyEntityTokens(entityType));
    }

    // bellow schema and index entry methods don't do performCheckBeforeOperation on purpose
    // callers usually "batch" them and can decide to do single check
    @Override
    public void acquireSharedSchemaLock(SchemaDescriptorSupplier schemaLike) {
        SchemaDescriptor schema = schemaLike.schema();
        lockClient.acquireShared(lockTracer.get(), schema.keyType(), schema.lockingKeys());
    }

    @Override
    public void releaseSharedSchemaLock(SchemaDescriptorSupplier schemaLike) {
        SchemaDescriptor schema = schemaLike.schema();
        long[] lockingKeys = schema.lockingKeys();
        lockClient.releaseShared(schema.keyType(), lockingKeys);
    }

    @Override
    public void acquireSharedIndexEntryLock(long... indexEntries) {
        lockClient.acquireShared(lockTracer.get(), ResourceType.INDEX_ENTRY, indexEntries);
    }

    @Override
    public void releaseSharedIndexEntryLock(long... indexEntries) {
        lockClient.releaseShared(ResourceType.INDEX_ENTRY, indexEntries);
    }

    @Override
    public void acquireExclusiveIndexEntryLock(long... indexEntries) {
        lockClient.acquireExclusive(lockTracer.get(), ResourceType.INDEX_ENTRY, indexEntries);
    }

    @Override
    public void releaseExclusiveIndexEntryLock(long... indexEntries) {
        lockClient.releaseExclusive(ResourceType.INDEX_ENTRY, indexEntries);
    }
}
