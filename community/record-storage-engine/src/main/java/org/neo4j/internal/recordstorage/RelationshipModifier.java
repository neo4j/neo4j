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

import static org.neo4j.collection.trackable.HeapTrackingCollections.newLongObjectMap;
import static org.neo4j.internal.recordstorage.RelationshipCreator.NodeDataLookup.DIR_IN;
import static org.neo4j.internal.recordstorage.RelationshipCreator.NodeDataLookup.DIR_LOOP;
import static org.neo4j.internal.recordstorage.RelationshipCreator.NodeDataLookup.DIR_OUT;
import static org.neo4j.internal.recordstorage.RelationshipCreator.relCount;
import static org.neo4j.internal.recordstorage.RelationshipLockHelper.findAndLockInsertionPoint;
import static org.neo4j.internal.recordstorage.RelationshipLockHelper.lockRelationshipsInOrder;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.kernel.impl.store.record.Record.isNull;
import static org.neo4j.kernel.impl.store.record.RecordLoad.ALWAYS;
import static org.neo4j.lock.ResourceType.NODE;
import static org.neo4j.lock.ResourceType.NODE_RELATIONSHIP_GROUP_DELETE;
import static org.neo4j.lock.ResourceType.RELATIONSHIP;
import static org.neo4j.lock.ResourceType.RELATIONSHIP_GROUP;

import java.util.function.Predicate;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.internal.counts.DegreeUpdater;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.RelationshipModifications.RelationshipBatch;

/**
 * Manages locking and creation/delete of relationships. Will call on {@link RelationshipCreator} and {@link RelationshipDeleter} for actual
 * record changes in the end.
 */
public class RelationshipModifier {
    public static final int DEFAULT_EXTERNAL_DEGREES_THRESHOLD_SWITCH = 10;

    private final RelationshipGroupGetter relGroupGetter;
    private final int denseNodeThreshold;
    private final ResourceLocker locks;
    private final LockTracer lockTracer;
    private final MemoryTracker memoryTracker;
    private final RelationshipCreator creator;
    private final RelationshipDeleter deleter;

    public RelationshipModifier(
            RelationshipGroupGetter relGroupGetter,
            PropertyDeleter propertyChainDeleter,
            int denseNodeThreshold,
            ResourceLocker locks,
            LockTracer lockTracer,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        this.relGroupGetter = relGroupGetter;
        this.denseNodeThreshold = denseNodeThreshold;
        this.locks = locks;
        this.lockTracer = lockTracer;
        this.memoryTracker = memoryTracker;

        this.creator =
                new RelationshipCreator(denseNodeThreshold, DEFAULT_EXTERNAL_DEGREES_THRESHOLD_SWITCH, cursorContext);
        this.deleter = new RelationshipDeleter(
                relGroupGetter, propertyChainDeleter, DEFAULT_EXTERNAL_DEGREES_THRESHOLD_SWITCH);
    }

    /**
     * Handle locking and modifications of Node/RelationshipGroup/Relationship-records for creating and deleting relationships.
     * @param modifications The modifications to process. Assumed to be sorted by id & type
     */
    public void modifyRelationships(
            RelationshipModifications modifications, RecordAccessSet recordChanges, DegreeUpdater groupDegreesUpdater) {
        /*
         * The general idea here is to do granular locking (only lock the resources/records that we need to change) in a way that minimizes deadlocks
         * First we figure out all the locks we need by looking at the changes we need to do in combination with that is already stored
         * We take all the locks we need in a defined order (sorted by type and id)
         * Then we can make all changes without locking logic to keep it as simple as possible
         */
        try (HeapTrackingLongObjectHashMap<NodeContext> contexts = newLongObjectMap(memoryTracker)) {
            MappedNodeDataLookup nodeDataLookup =
                    new MappedNodeDataLookup(contexts, relGroupGetter, recordChanges, memoryTracker);
            // Acquire most locks
            // First we take Node and Group locks (sorted by node id)
            acquireMostOfTheNodeAndGroupsLocks(modifications, recordChanges, contexts, nodeDataLookup);
            // Then we take all the Relationship locks (sorted by relationship id, per relationship chain)
            acquireRelationshipLocksAndSomeOthers(modifications, recordChanges, contexts);

            // Do the modifications
            creator.relationshipCreate(modifications.creations(), recordChanges, groupDegreesUpdater, nodeDataLookup);
            deleter.relationshipDelete(
                    modifications.deletions(),
                    recordChanges,
                    groupDegreesUpdater,
                    nodeDataLookup,
                    memoryTracker,
                    locks /*no lock tracing because no blocking acquisitions*/);
        }
    }

    private void acquireMostOfTheNodeAndGroupsLocks(
            RelationshipModifications modifications,
            RecordAccessSet recordChanges,
            MutableLongObjectMap<NodeContext> contexts,
            MappedNodeDataLookup nodeDataLookup) {
        /* Here we're going to figure out if we need to make changes to any node and/or relationship group records and lock them if we do. */
        // We check modifications for each node, it might need locking. The iteration here is always sorted by node id
        modifications.forEachSplit(byNode -> {
            long nodeId = byNode.nodeId();
            RecordProxy<NodeRecord, Void> nodeProxy =
                    recordChanges.getNodeRecords().getOrLoad(nodeId, null);
            NodeRecord node = nodeProxy.forReadingLinkage(); // optimistic (unlocked) read
            boolean nodeIsAddedInTx = node.isCreated();
            if (!node.isDense()) // we can not trust this as the node is not locked
            {
                if (!nodeIsAddedInTx) // to avoid locking unnecessarily
                {
                    locks.acquireExclusive(lockTracer, NODE, nodeId); // lock and re-read, now we can trust it
                    nodeProxy = recordChanges.getNodeRecords().getOrLoad(nodeId, null);
                    node = nodeProxy.forReadingLinkage();
                    if (node.isDense()) {
                        // another transaction just turned this node dense, unlock and let it be handled below
                        locks.releaseExclusive(NODE, nodeId);
                    } else if (byNode.hasCreations()) {
                        // Sparse node with added relationships. We might turn this node dense, at which point the group
                        // lock will be needed, so lock it
                        locks.acquireExclusive(lockTracer, RELATIONSHIP_GROUP, nodeId);
                    }
                }
            }

            if (node.isDense()) // the node is not locked but the dense node is a one-way transform so we can trust it
            {
                // Stabilize first in chains, in case they are deleted or needed for chain degrees.
                // We are preventing any changes to the group which in turn blocks any other relationship becomming the
                // first in chain
                locks.acquireShared(lockTracer, RELATIONSHIP_GROUP, nodeId);
                // Creations
                NodeContext nodeContext = NodeContext.createNodeContext(nodeProxy, memoryTracker);
                contexts.put(nodeId, nodeContext);
                if (byNode.hasCreations()) {
                    // We have some creations on a dense node. If the group exists we can use that, otherwise we create
                    // it
                    byNode.forEachCreationSplit(byType -> {
                        RelationshipGroupGetter.RelationshipGroupPosition groupPosition =
                                findRelationshipGroup(recordChanges, nodeContext, byType);
                        nodeContext.setCurrentGroup(
                                groupPosition.group() != null
                                        ? groupPosition.group()
                                        : groupPosition.closestPrevious());
                        RecordProxy<RelationshipGroupRecord, Integer> groupProxy = groupPosition.group();
                        if (groupProxy == null) {
                            // The group did not exist
                            if (!nodeContext.hasExclusiveGroupLock()) {
                                // And we did not already have the lock, so we need to upgrade to exclusive create it
                                locks.releaseShared(RELATIONSHIP_GROUP, nodeId);
                                // Note the small window here where we dont hold any group lock, things might change so
                                // we can not trust previous group reads
                                locks.acquireExclusive(lockTracer, NODE, nodeId);
                                locks.acquireExclusive(lockTracer, RELATIONSHIP_GROUP, nodeId);
                            }
                            nodeContext.setNode(recordChanges.getNodeRecords().getOrLoad(nodeId, null));
                            long groupStartingId =
                                    nodeContext.node().forReadingLinkage().getNextRel();
                            long groupStartingPrevId = NULL_REFERENCE.longValue();
                            if (groupPosition.closestPrevious() != null) {
                                groupStartingId =
                                        groupPosition.closestPrevious().getKey();
                                groupStartingPrevId = groupPosition
                                        .closestPrevious()
                                        .forReadingLinkage()
                                        .getPrev();
                            }
                            // At this point the group is locked so we can create it
                            groupProxy = relGroupGetter.getOrCreateRelationshipGroup(
                                    nodeContext.node(),
                                    byType.type(),
                                    recordChanges.getRelGroupRecords(),
                                    groupStartingPrevId,
                                    groupStartingId);
                            // another transaction might beat us at this point, so we are not guaranteed to be the
                            // creator but we can trust it to exist
                            if (!nodeContext.hasExclusiveGroupLock()) {
                                nodeContext.markExclusiveGroupLock();
                            } else if (groupProxy.isCreated()) {
                                nodeContext
                                        .clearDenseContext(); // When a new group is created we can no longer trust the
                                // pointers of the cache
                            }
                        }
                        nodeContext.denseContext(byType.type()).setGroup(groupProxy);
                    });

                    if (!nodeContext.hasExclusiveGroupLock()) {
                        // No other path has given us the exclusive lock yet
                        byNode.forEachCreationSplitInterruptible(byType -> {
                            // But if we are creating relationships to a chain that does not exist on the group
                            // or we might need to flip the external degrees flag
                            RelationshipGroupRecord group = nodeContext
                                    .denseContext(byType.type())
                                    .group()
                                    .forReadingLinkage();
                            if (byType.hasOut() && (!group.hasExternalDegreesOut() || isNull(group.getFirstOut()))
                                    || byType.hasIn() && (!group.hasExternalDegreesIn() || isNull(group.getFirstIn()))
                                    || byType.hasLoop()
                                            && (!group.hasExternalDegreesLoop() || isNull(group.getFirstLoop()))) {
                                // Then we need the exclusive lock to change it
                                locks.releaseShared(RELATIONSHIP_GROUP, nodeId);
                                // Note the small window here where we dont hold any group lock, things might change so
                                // we can not trust previous group reads
                                locks.acquireExclusive(lockTracer, RELATIONSHIP_GROUP, nodeId);
                                nodeContext.markExclusiveGroupLock();
                                return true; // And we can abort the iteration as the group lock is protecting all
                                // relationship group records of the node
                            }
                            return false;
                        });
                    }
                }

                // Deletions
                if (byNode.hasDeletions()) {
                    if (!nodeContext
                            .hasExclusiveGroupLock()) // no need to do anything if it is already locked by additions
                    {
                        byNode.forEachDeletionSplitInterruptible(byType -> {
                            NodeContext.DenseContext denseContext = nodeContext.denseContext(byType.type());
                            RelationshipGroupRecord group = denseContext.getOrLoadGroup(
                                    relGroupGetter,
                                    nodeContext.node().forReadingLinkage(),
                                    byType.type(),
                                    recordChanges.getRelGroupRecords());
                            // here we have the shared lock, so we can trust the read
                            if (byType.hasOut() && !group.hasExternalDegreesOut()
                                    || byType.hasIn() && !group.hasExternalDegreesIn()
                                    || byType.hasLoop() && !group.hasExternalDegreesLoop()) {
                                // We have deletions but without external degrees, we might need to flip that so we lock
                                // it
                                locks.releaseShared(RELATIONSHIP_GROUP, nodeId);
                                // Note the small window here where we dont hold any group lock, things might change so
                                // we can not trust previous group reads
                                locks.acquireExclusive(lockTracer, RELATIONSHIP_GROUP, nodeId);
                                nodeContext.markExclusiveGroupLock();
                                return true;
                            } else {
                                // We have deletions and only external degrees
                                boolean hasAnyFirst = batchContains(byType.out(), group.getFirstOut())
                                        || batchContains(byType.in(), group.getFirstIn())
                                        || batchContains(byType.loop(), group.getFirstLoop());
                                if (hasAnyFirst) {
                                    // But we're deleting the first in the chain so the group needs to be updated
                                    locks.releaseShared(RELATIONSHIP_GROUP, nodeId);
                                    // Note the small window here where we dont hold any group lock, things might change
                                    // so we can not trust previous group reads
                                    locks.acquireExclusive(lockTracer, RELATIONSHIP_GROUP, nodeId);
                                    nodeContext.markExclusiveGroupLock();
                                    return true;
                                }
                            }
                            return false;
                        });
                    }
                }

                // Look for an opportunity to delete empty groups that we noticed while looking for groups above
                if (nodeContext.hasExclusiveGroupLock() && nodeContext.hasAnyEmptyGroup()) {
                    // There may be one or more empty groups that we can delete
                    if (locks.tryExclusiveLock(NODE_RELATIONSHIP_GROUP_DELETE, nodeId)) {
                        // We got the EXCLUSIVE group lock so we can go ahead and try to remove any potentially empty
                        // groups
                        if (!nodeContext.hasEmptyFirstGroup() || locks.tryExclusiveLock(NODE, nodeId)) {
                            if (nodeContext.hasEmptyFirstGroup()) {
                                // It's possible that we need to delete the first group, i.e. we just now locked the
                                // node and therefore need to re-read it
                                nodeContext.setNode(
                                        recordChanges.getNodeRecords().getOrLoad(nodeId, null));
                            }
                            Predicate<RelationshipGroupRecord> canDeleteGroup =
                                    group -> !byNode.hasCreations(group.getType());
                            if (RelationshipGroupGetter.deleteEmptyGroups(
                                    nodeContext.node(), canDeleteGroup, nodeDataLookup)) {
                                nodeContext.clearDenseContext();
                            }
                        }
                    }
                }
            }
        });
    }

    /**
     * Traverses a relationship group chain and while doing that it will notice empty groups and mark that in the context.
     * This information can later be used to attempt to delete empty groups at a place where they will be locked.
     */
    private RelationshipGroupGetter.RelationshipGroupPosition findRelationshipGroup(
            RecordAccessSet recordChanges,
            NodeContext nodeContext,
            RelationshipModifications.NodeRelationshipTypeIds byType) {
        return relGroupGetter.getRelationshipGroup(
                nodeContext.groupStartingPrevId(),
                nodeContext.groupStartingId(),
                byType.type(),
                recordChanges.getRelGroupRecords(),
                group -> {
                    if (group.getType() != byType.type()) {
                        nodeContext.checkEmptyGroup(group);
                    }
                });
    }

    private void acquireRelationshipLocksAndSomeOthers(
            RelationshipModifications modifications,
            RecordAccessSet recordChanges,
            MutableLongObjectMap<NodeContext> contexts) {
        /*
         * Here we're mostly going to figure out which relationships to lock.
         * When deleting we need to lock surrounding relationships to keep the integrity of the chain
         *      We take all these locks in sorted order
         * When creating we need to consecutive locked relationships to insert between
         *      If we have deletions on the chain we use that as insertion point, as it will already be locked
         *      If we dont have deletions, the locks will be taken in chain-order
         *          Not sorted, but still in a consistent order with other transactions doing the same thing
         *          Finding the insertion point is done by try-lock so it is not causing deadlocks with concurrent deletions
         */

        // Here we have already taken the Node/Group locks we need, and we have at least a shared group lock
        RecordAccess<RelationshipRecord, Void> relRecords = recordChanges.getRelRecords();
        modifications.forEachSplit(byNode -> {
            long nodeId = byNode.nodeId();
            NodeRecord node =
                    recordChanges.getNodeRecords().getOrLoad(nodeId, null).forReadingLinkage();
            if (!node.isDense()) {
                // Since it is a sparse node we know that it is exclusively locked
                if (!checkAndLockRelationshipsIfNodeIsGoingToBeDense(node, byNode, relRecords)) {
                    // We're not turning this node into dense
                    if (byNode.hasDeletions()) {
                        // Lock all relationships we're deleting, including the first in chain to update degrees
                        lockRelationshipsInOrder(
                                byNode.deletions(), node.getNextRel(), relRecords, locks, memoryTracker);
                    } else if (byNode.hasCreations()) {
                        // We only have creations but we still need the first in chain to update degrees (if it exists)
                        long firstRel = node.getNextRel();
                        if (!isNull(firstRel)) {
                            locks.acquireExclusive(lockTracer, RELATIONSHIP, firstRel);
                        }
                    }
                }
            } else {
                // We dont know if this node is locked so we can not really trust the read, but dense is one-way so its
                // safe
                NodeContext nodeContext = contexts.get(nodeId);
                if (byNode.hasDeletions()) {
                    byNode.forEachDeletionSplit(byType -> {
                        // We have some deletions on this group. The group is minimum shared locked so the first in
                        // chains (for non-external degrees) are stable
                        NodeContext.DenseContext context = nodeContext.denseContext(byType.type());
                        RelationshipGroupRecord group = context.getOrLoadGroup(
                                relGroupGetter, node, byType.type(), recordChanges.getRelGroupRecords());
                        long outFirstInChainForDegrees =
                                group.hasExternalDegreesOut() ? NULL_REFERENCE.longValue() : group.getFirstOut();
                        long inFirstInChainForDegrees =
                                group.hasExternalDegreesIn() ? NULL_REFERENCE.longValue() : group.getFirstIn();
                        long loopFirstInChainForDegrees =
                                group.hasExternalDegreesLoop() ? NULL_REFERENCE.longValue() : group.getFirstLoop();
                        // Lock each chain individually. It may cause deadlocks in some extremely unlikely scenarios but
                        // heavily reduce the number of iterations
                        // needed to get a stable lock on all the relationships when there a lot of contention on these
                        // particular chains
                        lockRelationshipsInOrder(
                                byType.out(), outFirstInChainForDegrees, relRecords, locks, memoryTracker);
                        lockRelationshipsInOrder(
                                byType.in(), inFirstInChainForDegrees, relRecords, locks, memoryTracker);
                        lockRelationshipsInOrder(
                                byType.loop(), loopFirstInChainForDegrees, relRecords, locks, memoryTracker);

                        // If we've locked some relationships for deletion, then we can use that as an insertion point
                        // for any creations we might have
                        // It will save us some time finding and locking an additional point, also reduce the likelihood
                        // of deadlocks
                        context.setInsertionPoint(DIR_OUT, insertionPointFromDeletion(byType.out(), relRecords));
                        context.setInsertionPoint(DIR_IN, insertionPointFromDeletion(byType.in(), relRecords));
                        context.setInsertionPoint(DIR_LOOP, insertionPointFromDeletion(byType.loop(), relRecords));
                    });
                }

                if (byNode.hasCreations()) {
                    byNode.forEachCreationSplit(byType -> {
                        // Now handle the creations by finding a suitable place to insert at
                        NodeContext.DenseContext context = nodeContext.denseContext(byType.type());
                        RelationshipGroupRecord group = context.getOrLoadGroup(
                                relGroupGetter, node, byType.type(), recordChanges.getRelGroupRecords());
                        context.setInsertionPoint(
                                DIR_OUT,
                                findAndLockInsertionPointForDense(
                                        byType.out(),
                                        context.insertionPoint(DIR_OUT),
                                        relRecords,
                                        group,
                                        DirectionWrapper.OUTGOING,
                                        nodeId));
                        context.setInsertionPoint(
                                DIR_IN,
                                findAndLockInsertionPointForDense(
                                        byType.in(),
                                        context.insertionPoint(DIR_IN),
                                        relRecords,
                                        group,
                                        DirectionWrapper.INCOMING,
                                        nodeId));
                        context.setInsertionPoint(
                                DIR_LOOP,
                                findAndLockInsertionPointForDense(
                                        byType.loop(),
                                        context.insertionPoint(DIR_LOOP),
                                        relRecords,
                                        group,
                                        DirectionWrapper.LOOP,
                                        nodeId));
                        context.markInsertionPointsAsChanged();
                    });
                }

                if (!nodeContext.hasExclusiveGroupLock()) {
                    // we no longer need the read lock, as all we either locked the first in chains, or have exclusive
                    // lock
                    locks.releaseShared(RELATIONSHIP_GROUP, byNode.nodeId());
                }
            }
        });
    }

    private boolean checkAndLockRelationshipsIfNodeIsGoingToBeDense(
            NodeRecord node,
            RelationshipModifications.NodeRelationshipIds byNode,
            RecordAccess<RelationshipRecord, Void> relRecords) {
        // We have an exclusively locked sparse not that may turn dense
        long nextRel = node.getNextRel();
        if (!isNull(nextRel)) {
            RelationshipRecord rel = relRecords.getOrLoad(nextRel, null).forReadingData();
            long nodeId = node.getId();
            if (!rel.isFirstInChain(nodeId)) {
                throw new IllegalStateException("Expected node " + rel + " to be first in chain for node " + nodeId);
            }
            int currentDegree = relCount(nodeId, rel);
            if (currentDegree + byNode.creations().size() >= denseNodeThreshold) {
                // The current length plus our additions in this transaction is above threshold, it will be converted so
                // we need to lock all the relationships
                // Since it is sparse and locked we can trust this chain read to be stable
                // find all id's and lock them as we will create new chains based on type and direction
                MutableLongList ids = LongLists.mutable.withInitialCapacity(currentDegree);
                do {
                    ids.add(nextRel);
                    nextRel =
                            relRecords.getOrLoad(nextRel, null).forReadingData().getNextRel(nodeId);
                } while (!isNull(nextRel));

                locks.acquireExclusive(lockTracer, RELATIONSHIP, ids.toSortedArray());
                return true;
            }
        }
        return false;
    }

    private RecordProxy<RelationshipRecord, Void> insertionPointFromDeletion(
            RelationshipBatch deletions, RecordAccess<RelationshipRecord, Void> relRecords) {
        return deletions.isEmpty() ? null : relRecords.getOrLoad(deletions.first(), null, ALWAYS);
    }

    private RecordProxy<RelationshipRecord, Void> findAndLockInsertionPointForDense(
            RelationshipBatch creations,
            RecordProxy<RelationshipRecord, Void> potentialInsertionPointFromDeletion,
            RecordAccess<RelationshipRecord, Void> relRecords,
            RelationshipGroupRecord group,
            DirectionWrapper direction,
            long nodeId) {
        if (!creations.isEmpty()) {
            // We have creations so we need to find a suitable point to insert it at
            if (potentialInsertionPointFromDeletion != null) {
                return potentialInsertionPointFromDeletion; // This is already locked for deletion, just take it!
            }
            // If we get here then there are no deletions on this chain and we have the RELATIONSHIP_GROUP SHARED Lock
            long firstInChain = direction.getNextRel(group);
            if (!isNull(firstInChain)) {
                // The chain exists
                if (!direction.hasExternalDegrees(group)) {
                    // And we don't have external degrees, so we need the first in chain for degrees update
                    locks.acquireExclusive(lockTracer, RELATIONSHIP, firstInChain);
                }
                // and a good insertion point by walking the chain with try-locks
                return findAndLockInsertionPoint(firstInChain, nodeId, relRecords, locks, lockTracer);
            }
        }
        return null;
    }

    private static boolean batchContains(RelationshipBatch batch, long id) {
        return !isNull(id) && batch.contains(id);
    }
}
