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

import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.kernel.impl.store.record.Record.isNull;

import java.util.function.Predicate;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

public class RelationshipGroupGetter {
    private final IdSequence idGenerator;
    private final CursorContext cursorContext;

    interface RelationshipGroupMonitor {
        void visit(RelationshipGroupRecord group);

        RelationshipGroupMonitor EMPTY = g -> {};
    }

    public RelationshipGroupGetter(IdSequence idGenerator, CursorContext cursorContext) {
        this.idGenerator = idGenerator;
        this.cursorContext = cursorContext;
    }

    public RelationshipGroupPosition getRelationshipGroup(
            NodeRecord node,
            int type,
            RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords,
            RelationshipGroupMonitor monitor) {
        return getRelationshipGroup(NULL_REFERENCE.longValue(), node.getNextRel(), type, relGroupRecords, monitor);
    }

    /**
     * @param prevGroupId supplied here because {@link RelationshipGroupRecord#getPrev()} isn't persisted in the record.
     * @param startingGroupId which group id to start iterating from.
     */
    public RelationshipGroupPosition getRelationshipGroup(
            long prevGroupId,
            long startingGroupId,
            int type,
            RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords,
            RelationshipGroupMonitor monitor) {
        long groupId = startingGroupId;
        long previousGroupId = prevGroupId;
        RecordProxy<RelationshipGroupRecord, Integer> previous = null;
        RecordProxy<RelationshipGroupRecord, Integer> current;
        while (!isNull(groupId)) {
            current = relGroupRecords.getOrLoad(groupId, null);
            RelationshipGroupRecord record = current.forReadingData();
            monitor.visit(record);
            record.setPrev(previousGroupId); // not persistent so not a "change"
            if (record.getType() == type) {
                return new RelationshipGroupPosition(previous, current);
            } else if (record.getType()
                    > type) { // The groups are sorted in the chain, so if we come too far we can return
                // empty handed right away
                return new RelationshipGroupPosition(previous, null);
            }
            previousGroupId = groupId;
            groupId = record.getNext();
            previous = current;
        }
        return new RelationshipGroupPosition(previous, null);
    }

    public RecordProxy<RelationshipGroupRecord, Integer> getOrCreateRelationshipGroup(
            RecordProxy<NodeRecord, Void> nodeChange,
            int type,
            RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords) {
        return getOrCreateRelationshipGroup(
                nodeChange,
                type,
                relGroupRecords,
                NULL_REFERENCE.longValue(),
                nodeChange.forReadingLinkage().getNextRel());
    }

    public RecordProxy<RelationshipGroupRecord, Integer> getOrCreateRelationshipGroup(
            RecordProxy<NodeRecord, Void> nodeChange,
            int type,
            RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords,
            long prevGroupId,
            long startingGroupId) {
        RelationshipGroupPosition existingGroup = getRelationshipGroup(
                prevGroupId, startingGroupId, type, relGroupRecords, RelationshipGroupMonitor.EMPTY);
        RecordProxy<RelationshipGroupRecord, Integer> change = existingGroup.group();
        if (change == null) {
            NodeRecord node = nodeChange.forReadingLinkage();
            assert node.isDense() : "Node " + node + " should have been dense at this point";
            long id = idGenerator.nextId(cursorContext);
            change = relGroupRecords.create(id, type, cursorContext);
            RelationshipGroupRecord record = change.forChangingData();
            record.setInUse(true);
            record.setCreated();
            record.setOwningNode(node.getId());

            // Attach it...
            RecordProxy<RelationshipGroupRecord, Integer> closestPreviousChange = existingGroup.closestPrevious();
            if (closestPreviousChange != null) { // ...after the closest previous one
                RelationshipGroupRecord closestPrevious = closestPreviousChange.forChangingLinkage();

                // if there's' a group after the found insertion point we need to set its prev to this new group
                if (!isNull(closestPrevious.getNext())) {
                    relGroupRecords
                            .getOrLoad(closestPrevious.getNext(), null)
                            .forChangingLinkage()
                            .setPrev(id);
                }

                record.setNext(closestPrevious.getNext());
                record.setPrev(closestPrevious.getId());
                closestPrevious.setNext(id);
            } else { // ...first in the chain
                node = nodeChange.forChangingLinkage();
                long firstGroupId = node.getNextRel();
                if (!isNull(firstGroupId)) { // There are others, make way for this new group
                    RelationshipGroupRecord previousFirstRecord =
                            relGroupRecords.getOrLoad(firstGroupId, type).forReadingData();
                    record.setNext(previousFirstRecord.getId());
                    previousFirstRecord.setPrev(id);
                }
                node.setNextRel(id);
            }
        }
        return change;
    }

    static void deleteGroup(
            RecordProxy<NodeRecord, Void> nodeChange, RelationshipGroupRecord group, GroupLookup groupLookup) {
        long previous = group.getPrev();
        long next = group.getNext();
        if (isNull(previous)) { // This is the first one, just point the node to the next group
            nodeChange.forChangingLinkage().setNextRel(next);
        } else { // There are others before it, point the previous to the next group
            groupLookup.group(previous).forChangingLinkage().setNext(next);
        }

        if (!isNull(
                next)) { // There are groups after this one, point that next group to the previous of the group to be
            // deleted
            groupLookup
                    .group(next)
                    .forReadingLinkage()
                    .setPrev(previous); // This is only for updating cache, thus reading not changing.
        }
        group.setInUse(false);
    }

    static boolean groupIsEmpty(RelationshipGroupRecord group) {
        return isNull(group.getFirstOut()) && isNull(group.getFirstIn()) && isNull(group.getFirstLoop());
    }

    public static boolean deleteEmptyGroups(
            RecordProxy<NodeRecord, Void> nodeProxy,
            Predicate<RelationshipGroupRecord> canDeleteGroup,
            GroupLookup groupLookup) {
        long groupId = nodeProxy.forReadingLinkage().getNextRel();
        long previousGroupId = Record.NULL_REFERENCE.longValue();
        RecordProxy<RelationshipGroupRecord, Integer> current;
        boolean anyDeleted = false;
        while (!isNull(groupId)) {
            current = groupLookup.group(groupId);
            RelationshipGroupRecord group = current.forReadingData();
            group.setPrev(previousGroupId);
            if (group.inUse() && groupIsEmpty(group) && canDeleteGroup.test(group)) {
                group = current.forChangingData();
                deleteGroup(nodeProxy, group, groupLookup);
                anyDeleted = true;
            } else {
                previousGroupId = groupId;
            }
            groupId = group.getNext();
        }
        return anyDeleted;
    }

    public record RelationshipGroupPosition(
            RecordProxy<RelationshipGroupRecord, Integer> closestPrevious,
            RecordProxy<RelationshipGroupRecord, Integer> group) {}

    interface GroupLookup {
        RecordProxy<RelationshipGroupRecord, Integer> group(long groupId);
    }

    static class DirectGroupLookup implements GroupLookup {
        final RecordAccessSet recordChanges;
        final CursorContext cursorContext;

        DirectGroupLookup(RecordAccessSet recordChanges, CursorContext cursorContext) {
            this.recordChanges = recordChanges;
            this.cursorContext = cursorContext;
        }

        @Override
        public RecordProxy<RelationshipGroupRecord, Integer> group(long groupId) {
            return recordChanges.getRelGroupRecords().getOrLoad(groupId, null);
        }
    }
}
