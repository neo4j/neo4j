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
package org.neo4j.consistency.checker;

import static org.neo4j.consistency.checker.RecordLoading.checkValidToken;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.recordstorage.RecordRelationshipScanCursor;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Checks relationship groups vs the relationships and node refer to.
 */
class RelationshipGroupChecker implements Checker {
    private static final String RELATIONSHIP_GROUPS_CHECKER_TAG = "relationshipGroupsChecker";
    private final NeoStores neoStores;
    private final ConsistencyReport.Reporter reporter;
    private final CheckerContext context;
    private final ProgressListener progress;

    RelationshipGroupChecker(CheckerContext context) {
        this.neoStores = context.neoStores;
        this.reporter = context.reporter;
        this.context = context;
        this.progress = context.progressReporter(
                this,
                "Relationship groups",
                neoStores.getRelationshipGroupStore().getIdGenerator().getHighId());
    }

    @Override
    public void check(LongRange nodeIdRange, boolean firstRange, boolean lastRange, MemoryTracker memoryTracker)
            throws Exception {
        ParallelExecution execution = context.execution;
        checkToOwner(nodeIdRange, context.contextFactory);
        if (firstRange) {
            execution.run(
                    getClass().getSimpleName(),
                    execution.partition(
                            neoStores.getRelationshipGroupStore(),
                            (from, to, last) -> () -> checkToRelationship(from, to, context.contextFactory)));
        }
    }

    @Override
    public boolean shouldBeChecked(ConsistencyFlags flags) {
        return flags.checkGraph();
    }

    /**
     * Check relationship group to owner node
     */
    private void checkToOwner(LongRange nodeIdRange, CursorContextFactory contextFactory) {
        RelationshipGroupStore groupStore = neoStores.getRelationshipGroupStore();
        CacheAccess.Client client = context.cacheAccess.client();
        final long highId = groupStore.getIdGenerator().getHighId();

        try (var cursorContext = contextFactory.create(RELATIONSHIP_GROUPS_CHECKER_TAG);
                var storeCursors = new CachedStoreCursors(neoStores, cursorContext);
                RecordReader<RelationshipGroupRecord> groupReader = new RecordReader<>(
                        neoStores.getRelationshipGroupStore(), true, cursorContext, context.memoryTracker);
                var localProgress = progress.threadLocalReporter()) {
            for (long id = groupStore.getNumberOfReservedLowIds(); id < highId && !context.isCancelled(); id++) {
                localProgress.add(1);
                RelationshipGroupRecord record = groupReader.read(id);
                if (!record.inUse()) {
                    continue;
                }

                long owningNode = record.getOwningNode();
                if (nodeIdRange.isWithinRangeExclusiveTo(owningNode)) {
                    long cachedOwnerNextRel = client.getFromCache(owningNode, CacheSlots.NodeLink.SLOT_RELATIONSHIP_ID);
                    boolean nodeIsInUse = client.getBooleanFromCache(owningNode, CacheSlots.NodeLink.SLOT_IN_USE);
                    if (!nodeIsInUse) {
                        reporter.forRelationshipGroup(record).ownerNotInUse();
                    } else if (cachedOwnerNextRel == id) {
                        // The old checker only verified that the relationship group that node.nextGroup pointed to had
                        // this node as its owner
                        client.putToCacheSingle(owningNode, CacheSlots.NodeLink.SLOT_CHECK_MARK, 0);
                    }

                    if (NULL_REFERENCE.is(record.getNext())) {
                        // This is the last group in the chain for this node. Verify that there's only one such last
                        // group.
                        boolean hasAlreadySeenLastGroup =
                                client.getBooleanFromCache(owningNode, CacheSlots.NodeLink.SLOT_HAS_LAST_GROUP);
                        if (hasAlreadySeenLastGroup) {
                            reporter.forRelationshipGroup(record)
                                    .multipleLastGroups(
                                            context.recordLoader.node(owningNode, storeCursors, context.memoryTracker));
                        }
                        client.putToCacheSingle(owningNode, CacheSlots.NodeLink.SLOT_HAS_LAST_GROUP, 1);
                    }
                }
            }
        }
    }

    /**
     * Check relationship groups to first in chain relationship. Run only on first node-range
     */
    private void checkToRelationship(long fromGroupId, long toGroupId, CursorContextFactory contextFactory) {
        try (var cursorContext = contextFactory.create(RELATIONSHIP_GROUPS_CHECKER_TAG);
                var storeCursors = new CachedStoreCursors(neoStores, cursorContext);
                RecordReader<RelationshipGroupRecord> groupReader = new RecordReader<>(
                        neoStores.getRelationshipGroupStore(), true, cursorContext, context.memoryTracker);
                RecordReader<RelationshipGroupRecord> comparativeReader = new RecordReader<>(
                        neoStores.getRelationshipGroupStore(), false, cursorContext, context.memoryTracker);
                RecordStorageReader reader = new RecordStorageReader(neoStores);
                RecordRelationshipScanCursor relationshipCursor =
                        reader.allocateRelationshipScanCursor(cursorContext, storeCursors, context.memoryTracker)) {
            for (long id = fromGroupId; id < toGroupId && !context.isCancelled(); id++) {
                RelationshipGroupRecord record = groupReader.read(id);
                if (!record.inUse()) {
                    continue;
                }

                long owningNode = record.getOwningNode();

                if (owningNode < 0) {
                    reporter.forRelationshipGroup(record).illegalOwner();
                }
                checkValidToken(
                        record,
                        record.getType(),
                        context.tokenHolders.relationshipTypeTokens(),
                        neoStores.getRelationshipTypeTokenStore(),
                        (group, token) -> reporter.forRelationshipGroup(group).illegalRelationshipType(),
                        (group, token) -> reporter.forRelationshipGroup(group).relationshipTypeNotInUse(token),
                        storeCursors,
                        context.memoryTracker);

                if (!NULL_REFERENCE.is(record.getNext())) {
                    RelationshipGroupRecord comparativeRecord = comparativeReader.read(record.getNext());
                    if (!comparativeRecord.inUse()) {
                        reporter.forRelationshipGroup(record).nextGroupNotInUse();
                    } else {
                        if (record.getType() >= comparativeRecord.getType()) {
                            reporter.forRelationshipGroup(record).invalidTypeSortOrder();
                        }
                        if (owningNode != comparativeRecord.getOwningNode()) {
                            reporter.forRelationshipGroup(record).nextHasOtherOwner(comparativeRecord);
                        }
                    }
                }

                checkRelationshipGroupRelationshipLink(
                        relationshipCursor,
                        record,
                        record.getFirstOut(),
                        RelationshipGroupLink.OUT,
                        group -> reporter.forRelationshipGroup(group).firstOutgoingRelationshipNotInUse(),
                        group -> reporter.forRelationshipGroup(group).firstOutgoingRelationshipNotFirstInChain(),
                        group -> reporter.forRelationshipGroup(group).firstOutgoingRelationshipOfOtherType(),
                        (group, rel) -> reporter.forRelationshipGroup(group)
                                .firstOutgoingRelationshipDoesNotShareNodeWithGroup(rel),
                        storeCursors);
                checkRelationshipGroupRelationshipLink(
                        relationshipCursor,
                        record,
                        record.getFirstIn(),
                        RelationshipGroupLink.IN,
                        group -> reporter.forRelationshipGroup(group).firstIncomingRelationshipNotInUse(),
                        group -> reporter.forRelationshipGroup(group).firstIncomingRelationshipNotFirstInChain(),
                        group -> reporter.forRelationshipGroup(group).firstIncomingRelationshipOfOtherType(),
                        (group, rel) -> reporter.forRelationshipGroup(group)
                                .firstIncomingRelationshipDoesNotShareNodeWithGroup(rel),
                        storeCursors);
                checkRelationshipGroupRelationshipLink(
                        relationshipCursor,
                        record,
                        record.getFirstLoop(),
                        RelationshipGroupLink.LOOP,
                        group -> reporter.forRelationshipGroup(group).firstLoopRelationshipNotInUse(),
                        group -> reporter.forRelationshipGroup(group).firstLoopRelationshipNotFirstInChain(),
                        group -> reporter.forRelationshipGroup(group).firstLoopRelationshipOfOtherType(),
                        (group, rel) -> reporter.forRelationshipGroup(group)
                                .firstLoopRelationshipDoesNotShareNodeWithGroup(rel),
                        storeCursors);
            }
        }
    }

    private void checkRelationshipGroupRelationshipLink(
            RecordRelationshipScanCursor relationshipCursor,
            RelationshipGroupRecord record,
            long relationshipId,
            RelationshipGroupLink relationshipGroupLink,
            Consumer<RelationshipGroupRecord> reportRelationshipNotInUse,
            Consumer<RelationshipGroupRecord> reportRelationshipNotFirstInChain,
            Consumer<RelationshipGroupRecord> reportRelationshipOfOtherType,
            BiConsumer<RelationshipGroupRecord, RelationshipRecord> reportNodeNotSharedWithGroup,
            StoreCursors storeCursors) {
        if (!NULL_REFERENCE.is(relationshipId)) {
            relationshipCursor.single(relationshipId);
            if (!relationshipCursor.next()) {
                reportRelationshipNotInUse.accept(record);
            } else {
                if (!relationshipGroupLink.isFirstInChain(relationshipCursor)) {
                    reportRelationshipNotFirstInChain.accept(record);
                }
                if (relationshipCursor.getType() != record.getType()) {
                    reportRelationshipOfOtherType.accept(record);
                }

                boolean hasCorrectNode = relationshipCursor.getFirstNode() == record.getOwningNode()
                        || relationshipCursor.getSecondNode() == record.getOwningNode();
                if (!hasCorrectNode) {
                    reportNodeNotSharedWithGroup.accept(
                            record,
                            context.recordLoader.relationship(
                                    relationshipCursor.getId(), storeCursors, context.memoryTracker));
                }
            }
        }
    }

    @Override
    public String toString() {
        return String.format(
                "%s[highId:%d]",
                getClass().getSimpleName(),
                neoStores.getRelationshipGroupStore().getIdGenerator().getHighId());
    }
}
