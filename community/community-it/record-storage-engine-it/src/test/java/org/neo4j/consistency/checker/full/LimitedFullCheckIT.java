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
package org.neo4j.consistency.checker.full;

import static org.neo4j.consistency.checking.cache.CacheSlots.CACHE_LINE_SIZE_BYTES;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.common.EntityType;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checker.EntityBasedMemoryLimiter;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheckIntegrationTest;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.IndexEntryUpdate;

class LimitedFullCheckIT extends FullCheckIntegrationTest {
    @Override
    protected EntityBasedMemoryLimiter.Factory memoryLimit() {
        // Make it so that it will have to do the checking in a couple of node id ranges
        return EntityBasedMemoryLimiter.defaultMemoryLimiter(
                fixture.neoStores().getNodeStore().getIdGenerator().getHighId() * CACHE_LINE_SIZE_BYTES / 3);
    }

    @Test
    void shouldFindDuplicatesInUniqueIndexEvenWhenInDifferentRanges()
            throws ConsistencyCheckIncompleteException, IndexEntryConflictException, IOException {
        // given

        // Create 2 extra nodes to guarantee that the node id of our duplicate is not in the same range as the original
        // entry.
        createOneNode();
        createOneNode();
        // Create a node so the duplicate in the index refers to a valid node
        // (IndexChecker only reports the duplicate if it refers to a node id lower than highId)
        long nodeId = createOneNode();
        for (IndexDescriptor indexDescriptor : getValueIndexDescriptors()) {
            if (indexDescriptor.schema().entityType() == EntityType.NODE) {
                // Don't close this accessor. It will be done when shutting down db.
                IndexAccessor accessor = fixture.indexAccessorLookup().apply(indexDescriptor);

                try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                    // There is already another node (created in generateInitialData()) that has this value
                    updater.process(IndexEntryUpdate.add(nodeId, indexDescriptor, values(indexDescriptor)));
                }
                accessor.force(FileFlushEvent.NULL, NULL_CONTEXT);
            }
        }

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on(stats)
                .verify(RecordType.NODE, 1) // the duplicate in the unique index
                .verify(RecordType.INDEX, 3) // the index entries pointing to node that should not be in index (3 RANGE)
                .andThatsAllFolks();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldFindIndexInconsistenciesWhenHaveDifferentNumberRangesForEntityTypes(boolean moreNodesThanRelationships)
            throws ConsistencyCheckIncompleteException, IOException, IndexEntryConflictException {
        long highEntityId = Math.max(
                fixture.neoStores().getNodeStore().getIdGenerator().getHighId(),
                fixture.neoStores().getRelationshipStore().getIdGenerator().getHighId());

        // Adds more indexed entities to get more ranges for the entity type we want to have the most of.
        // Then removes the last indexed entity to make sure we can find problems in the last range for both
        // nodes and relationships even when we have different number of ranges.
        Pair<Long, Long> lastAddedIds = addMoreIndexedEntries(highEntityId, moreNodesThanRelationships);
        removeFromIndex(lastAddedIds.first(), lastAddedIds.other());

        // Allow 3 entities in each range
        final EntityBasedMemoryLimiter.Factory factory =
                EntityBasedMemoryLimiter.defaultMemoryLimiter(CACHE_LINE_SIZE_BYTES * 3);
        ConsistencySummaryStatistics stats = check(factory);

        on(stats)
                .verify(RecordType.NODE, 2) // 2 node indexes with 1 entry removed
                .verify(RecordType.RELATIONSHIP, 2) // 2 relationship indexes with 1 entry removed
                .andThatsAllFolks();
    }

    private Pair<Long, Long> addMoreIndexedEntries(long highEntityId, boolean moreNodesThanRelationships) {
        AtomicLong lastAddedNode = new AtomicLong();
        AtomicLong lastAddedRel = new AtomicLong();
        fixture.apply(tx -> {
            long nbrEntities = highEntityId;
            Node node;
            Relationship relationship;
            do {
                node = tx.createNode(label("label3"));
                node.setProperty(PROP1, VALUE1);
                node.setProperty(PROP2, VALUE2);
            } while (moreNodesThanRelationships && nbrEntities-- > 0);

            do {
                relationship = node.createRelationshipTo(node, withName("C"));
                relationship.setProperty(PROP1, VALUE1);
                relationship.setProperty(PROP2, VALUE2);
            } while (!moreNodesThanRelationships && nbrEntities-- > 0);

            lastAddedNode.set(node.getId());
            lastAddedRel.set(relationship.getId());
        });

        return Pair.of(lastAddedNode.get(), lastAddedRel.get());
    }

    private void removeFromIndex(long nodeToRemoveFromIndex, long relToRemoveFromIndex)
            throws IOException, IndexEntryConflictException {
        for (IndexDescriptor indexDescriptor : getValueIndexDescriptors()) {
            // Don't close this accessor. It will be done when shutting down db.
            IndexAccessor accessor = fixture.indexAccessorLookup().apply(indexDescriptor);

            try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                long idToRemove = relToRemoveFromIndex;
                if (indexDescriptor.schema().entityType() == EntityType.NODE) {
                    idToRemove = nodeToRemoveFromIndex;
                }
                updater.process(IndexEntryUpdate.remove(idToRemove, indexDescriptor, values(indexDescriptor)));
            }
            accessor.force(FileFlushEvent.NULL, NULL_CONTEXT);
        }
    }
}
