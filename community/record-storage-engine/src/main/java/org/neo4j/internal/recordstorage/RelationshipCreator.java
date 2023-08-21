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

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.Record.isNull;

import org.neo4j.internal.counts.DegreeUpdater;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.RelationshipModifications.RelationshipBatch;

/**
 * A utility class to handle all record changes necessary for creating relationships
 * It is assumed that all required locks are taken before the use of this class
 */
public class RelationshipCreator {
    public static final ConnectToDenseMonitor NO_CONNECT_TO_DENSE_MONITOR =
            (createdRelationship, group, direction) -> {};

    private final int denseNodeThreshold;
    private final long externalDegreesThreshold;
    private final CursorContext cursorContext;

    public RelationshipCreator(int denseNodeThreshold, long externalDegreesThreshold, CursorContext cursorContext) {
        this.denseNodeThreshold = denseNodeThreshold;
        this.externalDegreesThreshold = externalDegreesThreshold;
        this.cursorContext = cursorContext;
    }

    public interface NodeDataLookup extends RelationshipGroupGetter.GroupLookup {
        int DIR_OUT = 0;
        int DIR_IN = 1;
        int DIR_LOOP = 2;

        RecordProxy<RelationshipRecord, Void> insertionPoint(long nodeId, int type, int direction);

        RecordProxy<RelationshipGroupRecord, Integer> group(long nodeId, int type, boolean create);
    }

    public static class InsertFirst extends RelationshipGroupGetter.DirectGroupLookup implements NodeDataLookup {
        private final RelationshipGroupGetter relationshipGroupGetter;

        public InsertFirst(
                RelationshipGroupGetter relationshipGroupGetter,
                RecordAccessSet recordChanges,
                CursorContext cursorContext) {
            super(recordChanges, cursorContext);
            this.relationshipGroupGetter = relationshipGroupGetter;
        }

        @Override
        public RecordProxy<RelationshipRecord, Void> insertionPoint(long nodeId, int type, int direction) {
            return null;
        }

        @Override
        public RecordProxy<RelationshipGroupRecord, Integer> group(long nodeId, int type, boolean create) {
            RecordProxy<NodeRecord, Void> nodeChange =
                    recordChanges.getNodeRecords().getOrLoad(nodeId, null);
            return relationshipGroupGetter.getOrCreateRelationshipGroup(
                    nodeChange, type, recordChanges.getRelGroupRecords());
        }
    }

    /**
     * Creates all relationships in {@code creations}
     * @param creations {@link RelationshipModifications} with all relationship creations in this transaction.
     * @param recordChanges all record changes.
     * @param groupDegreesUpdater updater of external group degrees for dense nodes.
     * @param nodeDataLookup for looking up where to insert relationships.
     */
    public void relationshipCreate(
            RelationshipBatch creations,
            RecordAccessSet recordChanges,
            DegreeUpdater groupDegreesUpdater,
            NodeDataLookup nodeDataLookup) {
        creations.forEach((id, type, firstNodeId, secondNodeId, addedProperties) -> relationshipCreate(
                id, type, firstNodeId, secondNodeId, recordChanges, groupDegreesUpdater, nodeDataLookup));
    }

    /**
     * Creates a relationship with the given id, from the nodes identified by id and of type typeId
     * Here we assume that all required locks are taken do do the changes needed
     *
     * @param id The id of the relationship to create.
     * @param type The id of the relationship type this relationship will have.
     * @param firstNodeId The id of the start node.
     * @param secondNodeId The id of the end node.
     */
    public void relationshipCreate(
            long id,
            int type,
            long firstNodeId,
            long secondNodeId,
            RecordAccessSet recordChanges,
            DegreeUpdater groupDegreesUpdater,
            NodeDataLookup nodeDataLookup) {
        RecordProxy<NodeRecord, Void> firstNode = recordChanges.getNodeRecords().getOrLoad(firstNodeId, null);
        RecordProxy<NodeRecord, Void> secondNode = firstNodeId == secondNodeId
                ? firstNode
                : recordChanges.getNodeRecords().getOrLoad(secondNodeId, null);
        RecordAccess<RelationshipRecord, Void> relRecords = recordChanges.getRelRecords();
        convertNodeToDenseIfNecessary(firstNode, relRecords, groupDegreesUpdater, nodeDataLookup);
        convertNodeToDenseIfNecessary(secondNode, relRecords, groupDegreesUpdater, nodeDataLookup);
        RelationshipRecord record = relRecords.create(id, null, cursorContext).forChangingLinkage();
        record.setLinks(firstNodeId, secondNodeId, type);
        record.setInUse(true);
        record.setCreated();
        connectRelationship(firstNode, secondNode, record, relRecords, groupDegreesUpdater, nodeDataLookup);
    }

    static int relCount(long nodeId, RelationshipRecord rel) {
        // Degrees are stored in the backward pointer of the first in chain!
        return (int) rel.getPrevRel(nodeId);
    }

    private void convertNodeToDenseIfNecessary(
            RecordProxy<NodeRecord, Void> nodeChange,
            RecordAccess<RelationshipRecord, Void> relRecords,
            DegreeUpdater groupDegreesUpdater,
            NodeDataLookup nodeDataLookup) {
        NodeRecord node = nodeChange.forReadingLinkage();
        if (node.isDense()) {
            return;
        }
        long relId = node.getNextRel();
        if (!isNull(relId)) {
            RecordProxy<RelationshipRecord, Void> relProxy = relRecords.getOrLoad(relId, null);
            if (relCount(node.getId(), relProxy.forReadingData()) >= denseNodeThreshold) {
                convertNodeToDenseNode(
                        nodeChange,
                        relProxy.forChangingLinkage(),
                        relRecords,
                        groupDegreesUpdater,
                        nodeDataLookup,
                        NO_CONNECT_TO_DENSE_MONITOR);
            }
        }
    }

    private void connectRelationship(
            RecordProxy<NodeRecord, Void> firstNodeChange,
            RecordProxy<NodeRecord, Void> secondNodeChange,
            RelationshipRecord rel,
            RecordAccess<RelationshipRecord, Void> relRecords,
            DegreeUpdater groupDegreesUpdater,
            NodeDataLookup nodeDataLookup) {
        // Assertion interpreted: if node is a normal node and we're trying to create a
        // relationship that we already have as first rel for that node --> error
        NodeRecord firstNode = firstNodeChange.forReadingLinkage();
        NodeRecord secondNode = secondNodeChange.forReadingLinkage();
        assert firstNode.getNextRel() != rel.getId() || firstNode.isDense();
        assert secondNode.getNextRel() != rel.getId() || secondNode.isDense();

        if (!firstNode.isDense()) {
            rel.setFirstNextRel(firstNode.getNextRel());
        }
        if (!secondNode.isDense()) {
            rel.setSecondNextRel(secondNode.getNextRel());
        }

        boolean loop = firstNode.getId() == secondNode.getId();
        if (!firstNode.isDense()) {
            connectSparse(firstNode.getId(), firstNode.getNextRel(), rel, relRecords);
        } else {
            int index = loop ? NodeDataLookup.DIR_LOOP : NodeDataLookup.DIR_OUT;
            connectRelationshipToDenseNode(
                    firstNodeChange,
                    rel,
                    relRecords,
                    groupDegreesUpdater,
                    nodeDataLookup.insertionPoint(firstNode.getId(), rel.getType(), index),
                    nodeDataLookup,
                    NO_CONNECT_TO_DENSE_MONITOR);
        }

        if (!secondNode.isDense()) {
            if (!loop) {
                connectSparse(secondNode.getId(), secondNode.getNextRel(), rel, relRecords);
            } else {
                rel.setFirstInFirstChain(true);
                rel.setSecondPrevRel(rel.getFirstPrevRel());
            }
        } else if (!loop) {
            connectRelationshipToDenseNode(
                    secondNodeChange,
                    rel,
                    relRecords,
                    groupDegreesUpdater,
                    nodeDataLookup.insertionPoint(secondNode.getId(), rel.getType(), NodeDataLookup.DIR_IN),
                    nodeDataLookup,
                    NO_CONNECT_TO_DENSE_MONITOR);
        }

        if (!firstNode.isDense()) {
            firstNodeChange.forChangingLinkage();
            firstNode.setNextRel(rel.getId());
        }
        if (!secondNode.isDense()) {
            secondNodeChange.forChangingLinkage();
            secondNode.setNextRel(rel.getId());
        }
    }

    private void connectRelationshipToDenseNode(
            RecordProxy<NodeRecord, Void> nodeChange,
            RelationshipRecord createdRelationship,
            RecordAccess<RelationshipRecord, Void> relRecords,
            DegreeUpdater groupDegreesUpdater,
            RecordProxy<RelationshipRecord, Void> insertionPoint,
            NodeDataLookup nodeDataLookup,
            ConnectToDenseMonitor connectToDenseMonitor) {
        NodeRecord node = nodeChange.forReadingLinkage();
        DirectionWrapper dir = DirectionWrapper.wrapDirection(createdRelationship, node);
        connectDense(
                node,
                nodeDataLookup.group(nodeChange.getKey(), createdRelationship.getType(), true),
                dir,
                createdRelationship,
                relRecords,
                groupDegreesUpdater,
                insertionPoint,
                connectToDenseMonitor);
    }

    public void convertNodeToDenseNode(
            RecordProxy<NodeRecord, Void> nodeChange,
            RelationshipRecord firstRel,
            RecordAccess<RelationshipRecord, Void> relRecords,
            DegreeUpdater groupDegreesUpdater,
            NodeDataLookup nodeDataLookup,
            ConnectToDenseMonitor connectToDenseMonitor) {
        NodeRecord node = nodeChange.forChangingLinkage();
        node.setDense(true);
        node.setNextRel(NO_NEXT_RELATIONSHIP.intValue());
        long relId = firstRel.getId();
        RelationshipRecord relRecord = firstRel;
        while (!isNull(relId)) {
            // Get the next relationship id before connecting it (where linkage is overwritten)
            relId = relRecord.getNextRel(node.getId());
            relRecord.setPrevRel(NO_NEXT_RELATIONSHIP.longValue(), node.getId());
            relRecord.setNextRel(NO_NEXT_RELATIONSHIP.longValue(), node.getId());
            connectRelationshipToDenseNode(
                    nodeChange,
                    relRecord,
                    relRecords,
                    groupDegreesUpdater,
                    null,
                    nodeDataLookup,
                    connectToDenseMonitor);
            if (!isNull(relId)) { // Lock and load the next relationship in the chain
                relRecord = relRecords.getOrLoad(relId, null).forChangingLinkage();
            }
        }
    }

    /**
     * Connects the created relationship R to the relevant surrounding records. There are three cases:
     * <pre>
     * - First in the chain
     *     [RelationshipGroup] --> [R]
     * - In between two other relationships
     *     [prev neighbour Relationship] --> [R] --> [next neighbour Relationship]
     * - At the end of the chain
     *     [prev neighbour Relationship] --> [R]
     * </pre>
     *
     * Degree of the modified chain is also incremented.
     */
    private void connectDense(
            NodeRecord node,
            RecordProxy<RelationshipGroupRecord, Integer> groupProxy,
            DirectionWrapper direction,
            RelationshipRecord createdRelationship,
            RecordAccess<RelationshipRecord, Void> relRecords,
            DegreeUpdater groupDegreesUpdater,
            RecordProxy<RelationshipRecord, Void> insertionPoint,
            ConnectToDenseMonitor monitor) {
        long nodeId = node.getId();
        RelationshipGroupRecord group = groupProxy.forReadingLinkage();
        long firstRelId = direction.getNextRel(group);
        RecordProxy<RelationshipRecord, Void> relationshipBefore = null;
        RecordProxy<RelationshipRecord, Void> relationshipAfter = null;
        // We need a place to insert the relationship. We have three cases
        // - First in chain (between group and potentially the previously first relationship)
        // - Between two relationships somewhere in the chain
        // - After the last in the chain

        if (insertionPoint != null) {
            // If we provided an insertion point we use that, otherwise we insert first
            relationshipBefore = insertionPoint;
            long next = insertionPoint.forReadingLinkage().getNextRel(nodeId);
            if (!isNull(next)) {
                relationshipAfter = relRecords.getOrLoad(next, null);
            }
        }

        // Here everything is known and locked, do the insertion
        if (relationshipBefore == null) {
            // first, i.e. there's no relationship at all for this type and direction or we failed to get a lock in the
            // chain
            if (!isNull(firstRelId)) {
                RelationshipRecord firstRel =
                        relRecords.getOrLoad(firstRelId, null).forChangingLinkage();
                if (!firstRel.isFirstInChain(nodeId)) {
                    throw new IllegalStateException("Expected " + firstRel + " to be first in chain for " + nodeId);
                }
                firstRel.setFirstInChain(false, nodeId);
                createdRelationship.setNextRel(firstRelId, nodeId);
                createdRelationship.setPrevRel(firstRel.getPrevRel(nodeId), nodeId);
                firstRel.setPrevRel(createdRelationship.getId(), nodeId);
            } else {
                // This is the first and only relationship in this chain. Set degree to 0 since the degree is
                // incremented below.
                createdRelationship.setPrevRel(0, nodeId);
                monitor.firstRelationshipInChainInserted(createdRelationship, group, direction);
            }

            group = groupProxy.forChangingData();
            direction.setNextRel(group, createdRelationship.getId());
            createdRelationship.setFirstInChain(true, nodeId);
            firstRelId = createdRelationship.getId();
        } else if (relationshipAfter != null) {
            // between two relationships somewhere in the chain
            createdRelationship.setFirstInChain(false, nodeId);
            // Link before <-> created
            RelationshipRecord before = relationshipBefore.forChangingLinkage();
            before.setNextRel(createdRelationship.getId(), nodeId);
            createdRelationship.setPrevRel(before.getId(), nodeId);

            // Link created <-> after
            RelationshipRecord after = relationshipAfter.forChangingLinkage();
            createdRelationship.setNextRel(after.getId(), nodeId);
            after.setPrevRel(createdRelationship.getId(), nodeId);
        } else {
            // last in the chain
            createdRelationship.setFirstInChain(false, nodeId);
            RelationshipRecord lastRelationship = relationshipBefore.forChangingLinkage();
            lastRelationship.setNextRel(createdRelationship.getId(), nodeId);
            createdRelationship.setPrevRel(lastRelationship.getId(), nodeId);
        }

        // With the chain connected we need to update the degrees
        if (direction.hasExternalDegrees(group)) // Optimistic reading is fine, as this is a one-way switch
        {
            // we don't need any locks to update the external degrees as that is safe
            groupDegreesUpdater.increment(group.getId(), direction.direction(), 1);
        } else {
            RecordProxy<RelationshipRecord, Void> firstRelProxy = relRecords.getOrLoad(firstRelId, null);
            long prevCount = firstRelProxy.forReadingLinkage().getPrevRel(nodeId);
            long count = prevCount + 1;
            // If we can we switch to external degrees for better concurrency in future updates
            if (count > externalDegreesThreshold) {
                group = groupProxy.forChangingData();
                direction.setHasExternalDegrees(group);
                groupDegreesUpdater.increment(group.getId(), direction.direction(), count);
            } else {
                // Or update the degrees stored in the back-pointer of the first-in-chain
                firstRelProxy.forChangingLinkage().setPrevRel(count, nodeId);
            }
        }
    }

    private void connectSparse(
            long nodeId,
            long firstRelId,
            RelationshipRecord createdRelationship,
            RecordAccess<RelationshipRecord, Void> relRecords) {
        long newCount = 1;
        if (!isNull(firstRelId)) {
            RelationshipRecord firstRel = relRecords.getOrLoad(firstRelId, null).forChangingLinkage();
            boolean changed = false;
            if (firstRel.getFirstNode() == nodeId) {
                newCount = firstRel.getFirstPrevRel() + 1;
                firstRel.setFirstPrevRel(createdRelationship.getId());
                firstRel.setFirstInFirstChain(false);
                changed = true;
            }
            if (firstRel.getSecondNode() == nodeId) {
                newCount = firstRel.getSecondPrevRel() + 1;
                firstRel.setSecondPrevRel(createdRelationship.getId());
                firstRel.setFirstInSecondChain(false);
                changed = true;
            }
            if (!changed) {
                throw new InvalidRecordException(nodeId + " doesn't match " + firstRel);
            }
        }

        // Set the relationship count
        if (createdRelationship.getFirstNode() == nodeId) {
            createdRelationship.setFirstPrevRel(newCount);
            createdRelationship.setFirstInFirstChain(true);
        }
        if (createdRelationship.getSecondNode() == nodeId) {
            createdRelationship.setSecondPrevRel(newCount);
            createdRelationship.setFirstInSecondChain(true);
        }
    }

    public interface ConnectToDenseMonitor {
        void firstRelationshipInChainInserted(
                RelationshipRecord relationship, RelationshipGroupRecord group, DirectionWrapper direction);
    }
}
