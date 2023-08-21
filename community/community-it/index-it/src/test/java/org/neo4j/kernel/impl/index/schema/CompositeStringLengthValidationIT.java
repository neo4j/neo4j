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
package org.neo4j.kernel.impl.index.schema;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.index.internal.gbptree.DynamicSizeUtil;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
@DbmsExtension
class CompositeStringLengthValidationIT {
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String KEY = "key";
    private static final String KEY2 = "key2";

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private PageCache pageCache;

    @Inject
    private RandomSupport random;

    private int firstSlotLength;
    private int secondSlotLength;

    @BeforeEach
    void calculateSlotSizes() {
        // TODO mvcc: this test should verify smaller limit for mvcc record format when we can start db with that format
        int pageSize = pageCache.pageSize();
        int totalSpace = DynamicSizeUtil.keyValueSizeCapFromPageSize(pageSize) - NativeIndexKey.ENTITY_ID_SIZE;
        int perSlotOverhead = GenericKey.TYPE_ID_SIZE + Types.SIZE_STRING_LENGTH;
        int firstSlotSpace = totalSpace / 2;
        int secondSlotSpace = totalSpace - firstSlotSpace;
        this.firstSlotLength = firstSlotSpace - perSlotOverhead;
        this.secondSlotLength = secondSlotSpace - perSlotOverhead;
    }

    @Test
    void shouldHandleCompositeSizesCloseToTheLimit() throws KernelException {
        String firstSlot = random.nextAlphaNumericString(firstSlotLength, firstSlotLength);
        String secondSlot = random.nextAlphaNumericString(secondSlotLength, secondSlotLength);

        // given
        IndexDescriptor index = createIndex(KEY, KEY2);

        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode(LABEL);
            node.setProperty(KEY, firstSlot);
            node.setProperty(KEY2, secondSlot);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            int propertyKeyId1 = ktx.tokenRead().propertyKey(KEY);
            int propertyKeyId2 = ktx.tokenRead().propertyKey(KEY2);
            try (NodeValueIndexCursor cursor =
                    ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                IndexReadSession indexReadSession = ktx.dataRead().indexReadSession(index);
                ktx.dataRead()
                        .nodeIndexSeek(
                                ktx.queryContext(),
                                indexReadSession,
                                cursor,
                                unconstrained(),
                                PropertyIndexQuery.exact(propertyKeyId1, firstSlot),
                                PropertyIndexQuery.exact(propertyKeyId2, secondSlot));
                assertTrue(cursor.next());
                assertEquals(node.getId(), cursor.nodeReference());
                assertFalse(cursor.next());
            }
            tx.commit();
        }
    }

    @Test
    void shouldFailBeforeCommitOnCompositeSizesLargerThanLimit() {
        String firstSlot = random.nextAlphaNumericString(firstSlotLength + 1, firstSlotLength + 1);
        String secondSlot = random.nextAlphaNumericString(secondSlotLength, secondSlotLength);

        // given
        createIndex(KEY, KEY2);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            try (Transaction tx = db.beginTx()) {
                Node node = tx.createNode(LABEL);
                node.setProperty(KEY, firstSlot);
                node.setProperty(KEY2, secondSlot);
                tx.commit();
            }
        });
        assertThat(e.getMessage())
                .contains("Property value is too large to index, please see index documentation for limitations.");
    }

    private IndexDescriptor createIndex(String... keys) {
        IndexDefinition indexDefinition;
        try (Transaction tx = db.beginTx()) {
            IndexCreator indexCreator = tx.schema().indexFor(LABEL);
            for (String key : keys) {
                indexCreator = indexCreator.on(key);
            }
            indexDefinition = indexCreator.create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(30, SECONDS);
            tx.commit();
        }
        return ((IndexDefinitionImpl) indexDefinition).getIndexReference();
    }
}
