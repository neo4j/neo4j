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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.storageengine.api.cursor.StoreCursors.NULL;

import org.eclipse.collections.api.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.EntityLocks;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.security.AccessMode.Static;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class DefaultNodeBasedRelationshipTypeIndexCursorTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldOnlyTraverseOutgoingRelationships() {
        // given
        var storageCursors = new StubStorageCursors();
        var internalCursors =
                new InternalCursorFactory(storageCursors, NULL, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE, false);
        var nodeCursor = new DefaultNodeCursor(
                c -> {}, storageCursors.allocateNodeCursor(NULL_CONTEXT, NULL), internalCursors, false);
        var relationshipCursor = new DefaultRelationshipTraversalCursor(
                c -> {},
                storageCursors.allocateRelationshipTraversalCursor(NULL_CONTEXT, NULL),
                internalCursors,
                false);
        var cursor = new DefaultNodeBasedRelationshipTypeIndexCursor(c -> {}, nodeCursor, relationshipCursor);
        KernelRead read = new KernelRead(
                mock(StorageReader.class),
                mock(TokenRead.class),
                mock(DefaultPooledCursors.class),
                mock(StoreCursors.class),
                mock(EntityLocks.class),
                mock(QueryContext.class),
                mock(TxStateHolder.class),
                mock(SchemaRead.class),
                mock(IndexingService.class),
                EmptyMemoryTracker.INSTANCE,
                false,
                mock(AssertOpen.class),
                () -> Static.FULL,
                false);

        cursor.setRead(read);
        int numNodes = 10;
        int numRelationships = 5;
        int type = 1;
        var nodesIds = new long[numNodes];
        for (int i = 0; i < numNodes; i++) {
            storageCursors.withNode(i);
            nodesIds[i] = i;
        }
        for (int i = 0; i < numRelationships; i++) {
            storageCursors.withRelationship(i, random.nextLong(numNodes), type, random.nextLong(numNodes));
        }

        // when
        var progressor = progressor(cursor, type, nodesIds);
        cursor.initialize(progressor, type, LongSets.immutable.empty().longIterator(), LongSets.immutable.empty());
        while (cursor.next()) {
            // then
            assertIsOutgoingRelationship(
                    storageCursors, cursor.sourceNodeReference(), cursor.relationshipReference(), cursor.type());
        }
    }

    private void assertIsOutgoingRelationship(
            StubStorageCursors storageCursors, long sourceNodeId, long relationshipId, int type) {
        try (var nodeCursor = storageCursors.allocateNodeCursor(NULL_CONTEXT, NULL);
                var relationshipScanCursor = storageCursors.allocateRelationshipScanCursor(NULL_CONTEXT, NULL);
                var relationshipTraversalCursor =
                        storageCursors.allocateRelationshipTraversalCursor(NULL_CONTEXT, NULL)) {
            // Find it using the scan cursor
            relationshipScanCursor.single(relationshipId);
            assertThat(relationshipScanCursor.next()).isTrue();
            assertThat(relationshipScanCursor.sourceNodeReference()).isEqualTo(sourceNodeId);

            // Find it using the traversal cursor
            nodeCursor.single(sourceNodeId);
            assertThat(nodeCursor.next()).isTrue();
            nodeCursor.relationships(
                    relationshipTraversalCursor, RelationshipSelection.selection(type, Direction.OUTGOING));
            boolean found = false;
            while (relationshipTraversalCursor.next() && !found) {
                if (relationshipTraversalCursor.entityReference() == relationshipId) {
                    found = true;
                }
            }
            assertThat(found).isTrue();
        }
    }

    private IndexProgressor progressor(DefaultNodeBasedRelationshipTypeIndexCursor cursor, int type, long... nodeIds) {
        return new IndexProgressor() {
            private int index = -1;

            @Override
            public boolean next() {
                if (index + 1 >= nodeIds.length) {
                    return false;
                }
                long nodeId = nodeIds[++index];
                boolean accepted = cursor.acceptEntity(nodeId, type);
                assertThat(accepted).isTrue();
                return true;
            }

            @Override
            public void close() {}
        };
    }
}
