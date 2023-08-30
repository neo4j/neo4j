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
package org.neo4j.kernel.api;

import static org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat.DEFAULT_PAYLOAD_SIZE;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.format.RecordFormats;

/**
 * We are naively testing that the available space estimate
 * is what we expect based on the changes we make to the graph by making a lot of
 * assumptions about storage engine internals.
 * <p/>
 * For record format, we are currently testing freeing up space in:
 * <ul>
 *  <li>nodes</li>
 *  <li>relationships</li>
 *  <li>dense node relationship groups</li>
 *  <li>dynamic labels</li>
 *  <li>single-block properties</li>
 *  <li>dynamic string properties</li>
 * </ul>
 */
public abstract class RecordFormatDatabaseSizeServiceAvailableReservedSizeIT
        extends DatabaseSizeServiceAvailableReservedSizeITBase {
    private NeoStores stores;

    @BeforeEach
    void getNeoStore() {
        stores = get(RecordStorageEngine.class).testAccessNeoStores();
    }

    protected RecordFormatDatabaseSizeServiceAvailableReservedSizeIT(RecordFormats recordFormats) {
        super(recordFormats.name());
    }

    @Test
    void shouldAccountForDeletedNodes() throws IOException {
        final var nodeCount = 1000;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < nodeCount; ++i) {
                tx.createNode(NODE_LABEL);
            }
            tx.commit();
        }

        assertAvailableReservedSpaceChanged(tx -> {
            int deletedNodes = 0;
            for (var node : tx.getAllNodes()) {
                deletedNodes += countDeletions(0.1, node);
            }
            return deletedNodes * nodeRecordSize();
        });
    }

    @Test
    void shouldAccountForDeletedRelationships() throws IOException {
        final var relCount = 1000;
        try (Transaction tx = db.beginTx()) {
            final var a = tx.createNode(NODE_LABEL);
            final var b = tx.createNode(NODE_LABEL);
            for (int i = 0; i < relCount; ++i) {
                a.createRelationshipTo(b, REL_TYPE);
            }
            tx.commit();
        }

        assertAvailableReservedSpaceChanged(tx -> {
            final var node = tx.getNodeById(0);
            int deletedRels = 0;
            for (var rel : node.getRelationships()) {
                deletedRels += countDeletions(0.1, rel);
            }
            return deletedRels * relationshipRecordSize();
        });
    }

    @Test
    void shouldAccountForDeletedDenseNodeRelationships() throws IOException {
        final var type1 = RelationshipType.withName("TYPE1");
        final var type2 = RelationshipType.withName("TYPE2");

        final var relCount = 1000;
        try (Transaction tx = db.beginTx()) {
            final var a = tx.createNode(NODE_LABEL);
            final var b = tx.createNode(NODE_LABEL);
            for (int i = 0; i < relCount; ++i) {
                a.createRelationshipTo(b, i % 2 == 0 ? type1 : type2);
            }
            tx.commit();
        }

        assertAvailableReservedSpaceChanged(tx -> {
            final var node = tx.getNodeById(0);
            for (var rel : node.getRelationships()) {
                rel.delete();
            }

            // For dense nodes, each node has one relationship group per relationship type.
            final var relationshipTypes = 2;
            final var relationshipGroups = relationshipTypes * 2;
            return relCount * relationshipRecordSize() + relationshipGroups * relationshipGroupRecordSize();
        });
    }

    @Test
    void shouldAccountForDeletedLoopRelationships() throws IOException {
        final var relCount = 1000;
        try (Transaction tx = db.beginTx()) {
            final var a = tx.createNode(NODE_LABEL);
            for (int i = 0; i < relCount; ++i) {
                a.createRelationshipTo(a, REL_TYPE);
            }
            tx.commit();
        }

        assertAvailableReservedSpaceChanged(tx -> {
            final var node = tx.getNodeById(0);
            int deletedRels = 0;
            for (var rel : node.getRelationships()) {
                deletedRels += countDeletions(0.1, rel);
            }
            return deletedRels * relationshipRecordSize();
        });
    }

    @Test
    void shouldAccountForDeletedNodeIntProperties() throws IOException {
        shouldAccountForDeletedNodeProperties(1337, 0L);
    }

    @Test
    void shouldAccountForDeletedNodeStringProperties() throws IOException {
        final var string = random.nextAlphaNumericString(128, 256);
        final long dynamicBlocksPerString = requiredDynamicBlocksFor(string);
        shouldAccountForDeletedNodeProperties(string, dynamicBlocksPerString * dynamicStringRecordBlockSize());
    }

    private void shouldAccountForDeletedNodeProperties(Object propertyValue, long expectedFreedDynamicStoreBytes)
            throws IOException {
        final var propCount = 25 * blocksPerPropertyRecord() + 1;

        try (Transaction tx = db.beginTx()) {
            final var node = tx.createNode(NODE_LABEL);
            for (int i = 0; i < propCount; ++i) {
                node.setProperty(PROPERTY_KEY_PREFIX + i, propertyValue);
            }
            tx.commit();
        }

        assertAvailableReservedSpaceChanged(tx -> {
            final var node = tx.getNodeById(0);
            int deletedPropRecords = 0;
            int deletedProps = 0;
            final var it = node.getPropertyKeys().iterator();

            while (it.hasNext()) {
                // If we delete the whole record worth of properties,
                // it should be freed and be accounted for.
                // If some properties remain in the record, it should not.
                final boolean deleteWholeRecord = random.nextBoolean();

                boolean first = true;
                int i = 0;
                for (; i < blocksPerPropertyRecord() && it.hasNext(); ++i) {
                    final var key = it.next();

                    if (first || deleteWholeRecord) {
                        ++deletedProps;
                        node.removeProperty(key);
                        first = false;
                    }
                }

                // If we deleted the whole record, or we ended on deleting the only property
                // in the last record, we expect the record to be freed.
                final boolean lastRecordHadSingleProperty = i == 1;
                if (deleteWholeRecord || lastRecordHadSingleProperty) {
                    ++deletedPropRecords;
                }
            }

            final var freedPropRecordBytes = deletedPropRecords * propertyRecordSize();
            final var freedDynamicRecordBytes = deletedProps * expectedFreedDynamicStoreBytes;
            return freedPropRecordBytes + freedDynamicRecordBytes;
        });
    }

    @Test
    void shouldAccountForLabelsRemovedFromHeavyNode() throws IOException {
        final var labelCount = 165;

        try (Transaction tx = db.beginTx()) {
            final var node = tx.createNode();
            for (int i = 0; i < labelCount; ++i) {
                node.addLabel(Label.label("Label" + i));
            }
            tx.commit();
        }

        assertAvailableReservedSpaceChanged(tx -> {
            final var node = tx.getNodeById(0);

            // Dynamic label arrays contain the node ID as their first element
            final var labelIds = new long[labelCount + 1];
            labelIds[0] = node.getId();

            int i = 1;
            for (var label : node.getLabels()) {
                final var itx = (InternalTransaction) tx;
                labelIds[i++] = itx.kernelTransaction().tokenRead().nodeLabel(label.name());
                node.removeLabel(label);
            }

            return requiredDynamicBlocksFor(labelIds) * dynamicLabelRecordBlockSize();
        });
    }

    long requiredDynamicBlocksFor(String str) {
        final var encoded = PropertyStore.encodeString(str);
        return MathUtil.ceil(encoded.length, dynamicStringRecordDataSize());
    }

    long requiredDynamicBlocksFor(long[] labelIds) {
        final var requiredBytes = DynamicArrayStore.NUMBER_HEADER_SIZE
                + MathUtil.ceil(
                        ShortArray.LONG.calculateRequiredBitsForArray(labelIds, labelIds.length) * labelIds.length,
                        Byte.SIZE);

        return MathUtil.ceil(requiredBytes, dynamicLabelRecordDataSize());
    }

    long nodeRecordSize() {
        return stores.getNodeStore().getRecordSize();
    }

    long propertyRecordSize() {
        return stores.getPropertyStore().getRecordSize();
    }

    long blocksPerPropertyRecord() {
        return DEFAULT_PAYLOAD_SIZE / Long.BYTES;
    }

    long relationshipRecordSize() {
        return stores.getRelationshipStore().getRecordSize();
    }

    long relationshipGroupRecordSize() {
        return stores.getRelationshipGroupStore().getRecordSize();
    }

    long dynamicStringRecordBlockSize() {
        return stores.getPropertyStore().getStringStore().getRecordSize();
    }

    long dynamicStringRecordDataSize() {
        return stores.getPropertyStore().getStringStore().getRecordDataSize();
    }

    long dynamicLabelRecordBlockSize() {
        return stores.getNodeStore().getDynamicLabelStore().getRecordSize();
    }

    long dynamicLabelRecordDataSize() {
        return stores.getNodeStore().getDynamicLabelStore().getRecordDataSize();
    }

    int countDeletions(double probability, Entity entity) {
        return (random.withProbability(probability, entity::delete)) ? 1 : 0;
    }
}
