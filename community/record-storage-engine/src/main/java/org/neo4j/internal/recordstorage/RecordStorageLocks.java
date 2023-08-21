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

import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceType;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

/**
 * Record storage specific locking. (See also  {@link ResourceType}
 *
 * To avoid deadlocks, they should be taken in the following order
 * <dl>
 *     <dt>{@link ResourceType#LABEL} or {@link ResourceType#RELATIONSHIP_TYPE} - Token id</dt>
 *     <dd>Schema locks, will lock indexes and constraints on the particular label or relationship type.</dd>
 *
 *     <dt>{@link ResourceType#SCHEMA_NAME} - Schema name (XXH64 hashed)</dt>
 *     <dd>
 *         Lock a schema name to avoid duplicates. Note, collisions are possible since we hash the string, but this only affect concurrency and not correctness.
 *     </dd>
 *
 *     <dt>{@link ResourceType#NODE_RELATIONSHIP_GROUP_DELETE} - Node id</dt>
 *     <dd>
 *         Lock taken on a node during the transaction creation phase to prevent deletion of said node and/or relationship group.
 *         This is different from the {@link ResourceType#NODE} to allow concurrent label and property changes together with relationship modifications.
 *     </dd>
 *
 *     <dt>{@link ResourceType#NODE} - Node id</dt>
 *     <dd>
 *         Lock on a node, used to prevent concurrent updates to the node records, i.e. add/remove label, set property, add/remove relationship.
 *         Note that changing relationships will only require a lock on the node if the head of the relationship chain/relationship group chain
 *         must be updated, since that is the only data part of the node record.
 *     </dd>
 *
 *     <dt>{@link ResourceType#DEGREES} - Node id</dt>
 *     <dd>
 *         Used to lock nodes to avoid concurrent label changes with relationship addition/deletion. This would otherwise lead to inconsistent count store.
 *     </dd>
 *
 *     <dt>{@link ResourceType#RELATIONSHIP_DELETE} - Relationship id</dt>
 *     <dd>Lock a relationship for exclusive access during deletion.</dd>
 *
 *     <dt>{@link ResourceType#RELATIONSHIP_GROUP} - Node id</dt>
 *     <dd>
 *         Lock the full relationship group chain for a given node(dense). This will not lock the node in contrast to
 *         {@link ResourceType#NODE_RELATIONSHIP_GROUP_DELETE}.
 *     </dd>
 *
 *     <dt>{@link ResourceType#RELATIONSHIP} - Relationship id</dt>
 *     <dd>Lock on a relationship, or more specifically a relationship record, to prevent concurrent updates.</dd>
 * </dl>
 */
class RecordStorageLocks implements StorageLocks {
    private final ResourceLocker locker;

    RecordStorageLocks(ResourceLocker locker) {
        this.locker = locker;
    }

    @Override
    public void acquireExclusiveNodeLock(LockTracer lockTracer, long... ids) {
        locker.acquireExclusive(lockTracer, ResourceType.NODE_RELATIONSHIP_GROUP_DELETE, ids);
        locker.acquireExclusive(lockTracer, ResourceType.NODE, ids);
    }

    @Override
    public void releaseExclusiveNodeLock(long... ids) {
        locker.releaseExclusive(ResourceType.NODE, ids);
        locker.releaseExclusive(ResourceType.NODE_RELATIONSHIP_GROUP_DELETE, ids);
    }

    @Override
    public void acquireSharedNodeLock(LockTracer lockTracer, long... ids) {
        locker.acquireShared(lockTracer, ResourceType.NODE_RELATIONSHIP_GROUP_DELETE, ids);
        locker.acquireShared(lockTracer, ResourceType.NODE, ids);
    }

    @Override
    public void releaseSharedNodeLock(long... ids) {
        locker.releaseShared(ResourceType.NODE, ids);
        locker.releaseShared(ResourceType.NODE_RELATIONSHIP_GROUP_DELETE, ids);
    }

    @Override
    public void acquireExclusiveRelationshipLock(LockTracer lockTracer, long... ids) {
        locker.acquireExclusive(lockTracer, ResourceType.RELATIONSHIP_DELETE, ids);
        locker.acquireExclusive(lockTracer, ResourceType.RELATIONSHIP, ids);
    }

    @Override
    public void releaseExclusiveRelationshipLock(long... ids) {
        locker.releaseExclusive(ResourceType.RELATIONSHIP, ids);
        locker.releaseExclusive(ResourceType.RELATIONSHIP_DELETE, ids);
    }

    @Override
    public void acquireSharedRelationshipLock(LockTracer lockTracer, long... ids) {
        locker.acquireShared(lockTracer, ResourceType.RELATIONSHIP_DELETE, ids);
        locker.acquireShared(lockTracer, ResourceType.RELATIONSHIP, ids);
    }

    @Override
    public void releaseSharedRelationshipLock(long... ids) {
        locker.releaseShared(ResourceType.RELATIONSHIP, ids);
        locker.releaseShared(ResourceType.RELATIONSHIP_DELETE, ids);
    }

    @Override
    public void acquireRelationshipCreationLock(
            LockTracer lockTracer,
            long sourceNode,
            long targetNode,
            boolean sourceNodeAddedInTx,
            boolean targetNodeAddedInTx) {
        lockGroupAndDegrees(locker, lockTracer, sourceNode, targetNode, sourceNodeAddedInTx, targetNodeAddedInTx);
    }

    @Override
    public void acquireRelationshipDeletionLock(
            LockTracer lockTracer,
            long sourceNode,
            long targetNode,
            long relationship,
            boolean relationshipAddedInTx,
            boolean sourceNodeAddedInTx,
            boolean targetNodeAddedInTx) {
        lockGroupAndDegrees(locker, lockTracer, sourceNode, targetNode, sourceNodeAddedInTx, targetNodeAddedInTx);
        locker.acquireExclusive(lockTracer, ResourceType.RELATIONSHIP_DELETE, relationship);
    }

    @Override
    public void acquireNodeDeletionLock(ReadableTransactionState txState, LockTracer lockTracer, long node) {
        if (!txState.nodeIsAddedInThisBatch(node)) {
            locker.acquireExclusive(lockTracer, ResourceType.NODE_RELATIONSHIP_GROUP_DELETE, node);
            locker.acquireExclusive(lockTracer, ResourceType.NODE, node);
            locker.acquireExclusive(lockTracer, ResourceType.DEGREES, node);
            // Note. We also take RELATIONSHIP_GROUP lock in TransactionRecordState.nodeDelete if we need to clean up
            // any left over empty groups.
        }
    }

    @Override
    public void acquireNodeLabelChangeLock(LockTracer lockTracer, long node, int labelId) {
        // This is done for e.g. NODE ADD/REMOVE label where we need a stable degree to tell the counts store. Acquiring
        // this lock
        // will prevent any RELATIONSHIP CREATE/DELETE to happen on this node until this transaction is committed.
        // What would happen w/o this lock (simplest scenario):
        // T1: Start TX, add label L on node N, which currently has got 5 relationships
        // T2: Start TX, create another relationship on N
        // T1: Go into commit where commands are created and for the added label the relationship degrees are read and
        // placed in a counts command
        // T2: Go into commit and fully complete commit, i.e. before T1. N now has 6 relationships
        // T1: Complete the commit
        // --> N has 6 relationships, but counts store says that it has 5
        locker.acquireExclusive(lockTracer, ResourceType.DEGREES, node);
    }

    private static void lockGroupAndDegrees(
            ResourceLocker locker,
            LockTracer lockTracer,
            long sourceNode,
            long targetNode,
            boolean sourceNodeAddedInTx,
            boolean targetNodeAddedInTx) {
        var minNode = Math.min(sourceNode, targetNode);
        lockGroupAndDegrees(
                locker, lockTracer, minNode, minNode == sourceNode ? sourceNodeAddedInTx : targetNodeAddedInTx);
        if (sourceNode != targetNode) {
            var maxNode = Math.max(sourceNode, targetNode);
            lockGroupAndDegrees(
                    locker, lockTracer, maxNode, maxNode == sourceNode ? sourceNodeAddedInTx : targetNodeAddedInTx);
        }
    }

    private static void lockGroupAndDegrees(
            ResourceLocker locker, LockTracer lockTracer, long node, boolean nodeAddedInTx) {
        if (!nodeAddedInTx) {
            locker.acquireShared(lockTracer, ResourceType.NODE_RELATIONSHIP_GROUP_DELETE, node);
            locker.acquireShared(lockTracer, ResourceType.DEGREES, node);
        }
    }
}
