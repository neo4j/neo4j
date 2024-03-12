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

import static org.neo4j.internal.recordstorage.RelationshipConnection.END_NEXT;
import static org.neo4j.internal.recordstorage.RelationshipConnection.END_PREV;
import static org.neo4j.internal.recordstorage.RelationshipConnection.START_NEXT;
import static org.neo4j.internal.recordstorage.RelationshipConnection.START_PREV;
import static org.neo4j.internal.recordstorage.RelationshipCreator.relCount;
import static org.neo4j.internal.recordstorage.RelationshipGroupGetter.RelationshipGroupMonitor.EMPTY;
import static org.neo4j.internal.recordstorage.RelationshipGroupGetter.deleteGroup;
import static org.neo4j.internal.recordstorage.RelationshipGroupGetter.groupIsEmpty;
import static org.neo4j.kernel.impl.store.record.Record.isNull;
import static org.neo4j.lock.ResourceType.NODE;
import static org.neo4j.lock.ResourceType.NODE_RELATIONSHIP_GROUP_DELETE;
import static org.neo4j.lock.ResourceType.RELATIONSHIP_GROUP;

import org.neo4j.internal.counts.DegreeUpdater;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.RelationshipModifications.RelationshipBatch;

/**
 * A utility class to handle all record changes necessary for deleting relationships
 * It is assumed that all required locks are taken before the use of this class but it may try take some additional locks to clean up empty records
 */
class RelationshipDeleter {
    private final RelationshipGroupGetter relGroupGetter;
    private final PropertyDeleter propertyChainDeleter;
    private final long externalDegreesThreshold;
    private final boolean multiVersion;

    RelationshipDeleter(
            RelationshipGroupGetter relGroupGetter,
            PropertyDeleter propertyChainDeleter,
            long externalDegreesThreshold,
            boolean multiVersion) {
        this.relGroupGetter = relGroupGetter;
        this.propertyChainDeleter = propertyChainDeleter;
        this.externalDegreesThreshold = externalDegreesThreshold;
        this.multiVersion = multiVersion;
    }

    /**
     * Deletes relationships found in {@code ids}.
     * Here we assume that all required locks are taken do do the changes needed
     *
     * @param deletions The ids of all relationships to delete in this transaction.
     * @param recordChanges {@link RecordAccessSet} to coordinate and keep changes to records.
     * @param groupDegreesUpdater for recording updates to degrees for the degrees store.
     * @param nodeDataLookup additional lookup for groups.
     * @param locks {@link ResourceLocker} for optimistic locking for deleting groups.
     */
    void relationshipDelete(
            RelationshipBatch deletions,
            RecordAccessSet recordChanges,
            DegreeUpdater groupDegreesUpdater,
            MappedNodeDataLookup nodeDataLookup,
            MemoryTracker memoryTracker,
            ResourceLocker locks) {
        deletions.forEach((id, type, startNode, endNode, noProperties) -> {
            RelationshipRecord record =
                    recordChanges.getRelRecords().getOrLoad(id, null).forChangingLinkage();
            propertyChainDeleter.deletePropertyChain(record, recordChanges.getPropertyRecords(), memoryTracker);
            disconnectRelationship(record, recordChanges.getRelRecords());
            updateNodesForDeletedRelationship(record, recordChanges, groupDegreesUpdater, nodeDataLookup, locks);
            record.setInUse(false);
            record.setType(-1);
        });
    }

    private void disconnectRelationship(RelationshipRecord rel, RecordAccess<RelationshipRecord, Void> relChanges) {
        disconnect(rel, START_NEXT, relChanges);
        disconnect(rel, START_PREV, relChanges);
        disconnect(rel, END_NEXT, relChanges);
        disconnect(rel, END_PREV, relChanges);
    }

    private void disconnect(
            RelationshipRecord rel, RelationshipConnection pointer, RecordAccess<RelationshipRecord, Void> relChanges) {
        long otherRelId = pointer.otherSide().get(rel);
        if (isNull(otherRelId)) {
            return;
        }

        RelationshipRecord otherRel = relChanges.getOrLoad(otherRelId, null).forChangingLinkage();
        boolean changed = false;
        long newId = pointer.get(rel);
        boolean newIsFirst = pointer.isFirstInChain(rel);
        if (otherRel.getFirstNode() == pointer.compareNode(rel)) {
            pointer.start().set(otherRel, newId, newIsFirst);
            changed = true;
        }
        if (otherRel.getSecondNode() == pointer.compareNode(rel)) {
            pointer.end().set(otherRel, newId, newIsFirst);
            changed = true;
        }
        if (!changed) {
            throw new InvalidRecordException(otherRel + " don't match " + rel);
        }
    }

    private void updateNodesForDeletedRelationship(
            RelationshipRecord rel,
            RecordAccessSet recordChanges,
            DegreeUpdater groupDegreesUpdater,
            MappedNodeDataLookup nodeDataLookup,
            ResourceLocker locks) {
        boolean loop = rel.getFirstNode() == rel.getSecondNode();
        updateNodeForDeletedRelationship(
                rel, recordChanges, groupDegreesUpdater, rel.getFirstNode(), true, nodeDataLookup, locks);
        updateNodeForDeletedRelationship(
                rel, recordChanges, groupDegreesUpdater, rel.getSecondNode(), !loop, nodeDataLookup, locks);
    }

    /**
     * Disconnects the deleted relationship R from the relevant surrounding records. There are three cases:
     * <pre>
     * - This is the last relationship in the chain
     *     [RelationshipGroup] -X-> [R]
     *     Also if the relationship group becomes completely empty after this it is attempted to be deleted, if locks can be acquired,
     *     otherwise another operation later will again notice this fact and attempt to delete the group.
     * - This relationships sits In between two other relationships
     *     [prev neighbour Relationship] -X-> [R] -X-> [next neighbour Relationship]
     * - At the end of the chain
     *     [prev neighbour Relationship] -X-> [R]
     * </pre>
     *
     * Degree of the modified chain is also decremented.
     */
    private void updateNodeForDeletedRelationship(
            RelationshipRecord rel,
            RecordAccessSet recordChanges,
            DegreeUpdater groupDegreesUpdater,
            long nodeId,
            boolean updateDegree,
            MappedNodeDataLookup nodeDataLookup,
            ResourceLocker locks) {
        // When we reach here, all required locks (node/relationships/groups) should be taken for the required changes
        // The relationship is already disconnected, so we just need to fix the degrees and potentially update some
        // pointers if we deleted the first
        RecordProxy<NodeRecord, Void> nodeProxy = recordChanges.getNodeRecords().getOrLoad(nodeId, null);
        NodeRecord node = nodeProxy.forReadingLinkage();
        if (!node.isDense()) {
            // Sparse node
            if (rel.isFirstInChain(nodeId)) {
                // If we're deleting the first-in-chain, update that
                node = nodeProxy.forChangingLinkage();
                node.setNextRel(rel.getNextRel(nodeId));
            }
            // Then the degrees
            if (updateDegree) {
                decrementTotalRelationshipCount(nodeId, rel, node.getNextRel(), recordChanges.getRelRecords());
            }
        } else {
            // Dense node
            DirectionWrapper direction = DirectionWrapper.wrapDirection(rel, node);
            RecordProxy<RelationshipGroupRecord, Integer> groupProxy =
                    nodeDataLookup.group(nodeId, rel.getType(), false);
            if (rel.isFirstInChain(nodeId)) {
                // If we're deleting the first
                RelationshipGroupRecord group = groupProxy.forChangingData();
                // update the first-in-chain
                direction.setNextRel(group, rel.getNextRel(nodeId));
                if (group.inUse() && groupIsEmpty(group)) {
                    // We're deleting the last relationship in this group, lets try to clean it up
                    // Since we don't have the required locks we can only do best-effort here with try-lock or risk
                    // deadlocks
                    boolean nodeRelationshipsLocked = locks.tryExclusiveLock(NODE_RELATIONSHIP_GROUP_DELETE, nodeId);
                    boolean nodeLocked = nodeRelationshipsLocked && locks.tryExclusiveLock(NODE, nodeId);
                    if (multiVersion || (nodeLocked && locks.tryExclusiveLock(RELATIONSHIP_GROUP, nodeId))) {
                        // We got all the locks, delete it!
                        nodeProxy = recordChanges.getNodeRecords().getOrLoad(nodeId, null);

                        if (isNull(group.getPrev())) {
                            // Since the prev-pointer is not a stored state, we need to traverse to it again to get the
                            // correct prev
                            long realPrev = relGroupGetter
                                    .getRelationshipGroup(
                                            nodeProxy.forReadingLinkage(),
                                            group.getType(),
                                            recordChanges.getRelGroupRecords(),
                                            EMPTY)
                                    .group()
                                    .forReadingLinkage()
                                    .getPrev();
                            group.setPrev(realPrev);
                        }
                        deleteGroup(nodeProxy, group, nodeDataLookup);
                    } else {
                        // We failed to get the all the locks. Cleanup will be done when we find empty groups in later
                        // transactions
                        // We need to unlock the locks we got
                        if (nodeLocked) {
                            locks.releaseExclusive(NODE, nodeId);
                        }
                        if (nodeRelationshipsLocked) {
                            locks.releaseExclusive(NODE_RELATIONSHIP_GROUP_DELETE, nodeId);
                        }
                    }
                }
            }

            if (updateDegree) {
                RelationshipGroupRecord group = groupProxy.forReadingData();
                if (direction.hasExternalDegrees(group)) // Optimistic reading is fine, as this is a one-way switch
                {
                    // Either a simple lockfree external degrees update
                    groupDegreesUpdater.increment(group.getId(), direction.direction(), -1);
                } else {
                    // Of we need to update the degree stored in the first back-pointer
                    // or potentially flip to external degrees (this is a rare case, most likely seen when the store was
                    // created before the introduction of
                    // external degrees, with nodes having long relationship chains
                    RecordProxy<RelationshipRecord, Void> firstRelProxy = null;
                    long prevCount;
                    if (rel.isFirstInChain(nodeId)) {
                        prevCount = rel.getPrevRel(nodeId);
                    } else {
                        firstRelProxy = recordChanges.getRelRecords().getOrLoad(direction.getNextRel(group), null);
                        prevCount = firstRelProxy.forReadingLinkage().getPrevRel(nodeId);
                    }
                    long count = prevCount - 1;

                    if (count > externalDegreesThreshold) {
                        direction.setHasExternalDegrees(groupProxy.forChangingData());
                        groupDegreesUpdater.increment(groupProxy.getKey(), direction.direction(), count);
                    } else if (count > 0) {
                        if (firstRelProxy == null) {
                            firstRelProxy = recordChanges.getRelRecords().getOrLoad(direction.getNextRel(group), null);
                        }
                        firstRelProxy.forChangingLinkage().setPrevRel(count, nodeId);
                    }
                }
            }
        }
    }

    private void decrementTotalRelationshipCount(
            long nodeId, RelationshipRecord rel, long firstRelId, RecordAccess<RelationshipRecord, Void> relRecords) {
        if (isNull(firstRelId)) {
            return;
        }
        boolean deletingFirstInChain = rel.isFirstInChain(nodeId);
        RelationshipRecord firstRel = relRecords.getOrLoad(firstRelId, null).forChangingLinkage();
        if (nodeId == firstRel.getFirstNode()) {
            firstRel.setFirstPrevRel(deletingFirstInChain ? relCount(nodeId, rel) - 1 : firstRel.getFirstPrevRel() - 1);
            assert firstRel.getFirstPrevRel() >= 0;
            firstRel.setFirstInFirstChain(true);
        }
        if (nodeId == firstRel.getSecondNode()) {
            firstRel.setSecondPrevRel(
                    deletingFirstInChain ? relCount(nodeId, rel) - 1 : firstRel.getSecondPrevRel() - 1);
            assert firstRel.getSecondPrevRel() >= 0;
            firstRel.setFirstInSecondChain(true);
        }
    }
}
