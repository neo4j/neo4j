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
package org.neo4j.kernel.impl.store;

import static org.eclipse.collections.impl.tuple.Tuples.pair;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.collections.api.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class TestPropertyBlocks extends AbstractNeo4jTestCase {
    @Test
    void simpleAddIntegers() {
        long inUseBefore = propertyRecordsInUse();
        Node node = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (int i = 0; i < PropertyType.getPayloadSizeLongs(); i++) {
                txNode.setProperty("prop" + i, i);
                assertEquals(i, txNode.getProperty("prop" + i));
            }
            transaction.commit();
        }
        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (int i = 0; i < PropertyType.getPayloadSizeLongs(); i++) {
                assertEquals(i, txNode.getProperty("prop" + i));
            }

            for (int i = 0; i < PropertyType.getPayloadSizeLongs(); i++) {
                assertEquals(i, txNode.removeProperty("prop" + i));
                assertFalse(txNode.hasProperty("prop" + i));
            }
            transaction.commit();
        }
        assertEquals(inUseBefore, propertyRecordsInUse());
    }

    @Test
    void simpleAddDoubles() {
        long inUseBefore = propertyRecordsInUse();
        Node node = createNode();

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (int i = 0; i < PropertyType.getPayloadSizeLongs() / 2; i++) {
                txNode.setProperty("prop" + i, i * -1.0);
                assertEquals(i * -1.0, txNode.getProperty("prop" + i));
            }
            transaction.commit();
        }

        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (int i = 0; i < PropertyType.getPayloadSizeLongs() / 2; i++) {
                assertEquals(i * -1.0, txNode.getProperty("prop" + i));
            }

            for (int i = 0; i < PropertyType.getPayloadSizeLongs() / 2; i++) {
                assertEquals(i * -1.0, txNode.removeProperty("prop" + i));
                assertFalse(txNode.hasProperty("prop" + i));
            }
            transaction.commit();
        }
        assertEquals(inUseBefore, propertyRecordsInUse());
    }

    @Test
    void deleteEverythingInMiddleRecord() {
        long inUseBefore = propertyRecordsInUse();
        Node node = createNode();

        Map<String, Object> properties = new HashMap<>();
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (int i = 0; i < 3 * PropertyType.getPayloadSizeLongs(); i++) {
                var key = "shortString" + i;
                var value = String.valueOf(i);
                txNode.setProperty(key, value);
                properties.put(key, value);
            }
            transaction.commit();
        }
        assertEquals(inUseBefore + 3, propertyRecordsInUse());

        final List<Pair<String, Object>> middleRecordProps = getPropertiesFromNode(node.getId(), 1);
        try (Transaction transaction = getGraphDb().beginTx()) {
            middleRecordProps.forEach(nameAndValue -> {
                final String name = nameAndValue.getOne();
                final Object value = nameAndValue.getTwo();
                assertEquals(value, transaction.getNodeById(node.getId()).removeProperty(name));
                properties.remove(name);
            });
            transaction.commit();
        }

        assertEquals(inUseBefore + 2, propertyRecordsInUse());
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (var key : properties.keySet()) {
                assertEquals(properties.get(key), txNode.removeProperty(key));
            }
            transaction.commit();
        }
    }

    private List<Pair<String, Object>> getPropertiesFromNode(long nodeId, int recordIndexInChain) {
        var nodeStore = nodeStore();
        var propertyStore = propertyStore();
        try (var cursor = nodeStore.openPageCursorForReading(0, NULL_CONTEXT);
                var propertyCursor = propertyStore.openPageCursorForReading(0, NULL_CONTEXT)) {
            var nodeRecord = nodeStore.newRecord();
            var propertyRecord = propertyStore.newRecord();
            nodeStore.getRecordByCursor(nodeId, nodeRecord, RecordLoad.NORMAL, cursor, EmptyMemoryTracker.INSTANCE);
            long prop = nodeRecord.getNextProp();
            for (int i = 0; i < recordIndexInChain; i++) {
                propertyStore.getRecordByCursor(
                        prop, propertyRecord, RecordLoad.NORMAL, propertyCursor, EmptyMemoryTracker.INSTANCE);
                prop = propertyRecord.getNextProp();
            }
            return getPropertiesFromRecord(prop);
        }
    }

    private List<Pair<String, Object>> getPropertiesFromRecord(long recordId) {
        final List<Pair<String, Object>> props = new ArrayList<>();
        PropertyStore propertyStore = propertyStore();
        final PropertyRecord record = propertyStore.newRecord();
        try (var cursor = propertyStore.openPageCursorForReading(0, NULL_CONTEXT)) {
            propertyStore.getRecordByCursor(recordId, record, RecordLoad.FORCE, cursor, EmptyMemoryTracker.INSTANCE);
        }
        try (StoreCursors storeCursors = createStoreCursors()) {
            record.propertyBlocks().forEach(block -> {
                final Object value = propertyStore
                        .getValue(block, storeCursors, EmptyMemoryTracker.INSTANCE)
                        .asObject();
                final String name = propertyStore
                        .getPropertyKeyTokenStore()
                        .getToken(block.getKeyIndexId(), storeCursors, EmptyMemoryTracker.INSTANCE)
                        .name();
                props.add(pair(name, value));
            });
        }
        return props;
    }

    @Test
    void largeTx() {
        try (Transaction transaction = getGraphDb().beginTx()) {
            Node node = transaction.createNode();

            node.setProperty("anchor", "hi");
            for (int i = 0; i < 255; i++) {
                node.setProperty("foo", 1);
                node.removeProperty("foo");
            }
            transaction.commit();
        }
    }

    /*
     * Creates a PropertyRecord, fills it up, removes something and
     * adds something that should fit.
     */
    @Test
    void deleteAndAddToFullPropertyRecord() {
        // Fill it up, each integer is one block
        Node node = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (int i = 0; i < PropertyType.getPayloadSizeLongs(); i++) {
                txNode.setProperty("prop" + i, i);
            }
            transaction.commit();
        }

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            // Remove all but one and add one
            for (int i = 0; i < PropertyType.getPayloadSizeLongs() - 1; i++) {
                assertEquals(i, txNode.removeProperty("prop" + i));
            }
            txNode.setProperty("profit", 5);
            transaction.commit();
        }

        // Verify
        int remainingProperty = PropertyType.getPayloadSizeLongs() - 1;
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            assertEquals(remainingProperty, txNode.getProperty("prop" + remainingProperty));
            assertEquals(5, txNode.getProperty("profit"));
            transaction.commit();
        }
    }

    @Test
    void checkPacking() {
        long inUseBefore = propertyRecordsInUse();

        // Fill it up, each integer is one block
        Node node = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("prop0", 0);
            transaction.commit();
        }

        // One record must have been added
        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            // Since integers take up one block, adding the remaining should not
            // create a new record.
            for (int i = 1; i < PropertyType.getPayloadSizeLongs(); i++) {
                txNode.setProperty("prop" + i, i);
            }
            transaction.commit();
        }

        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            // Removing one and adding one of the same size should not create a new
            // record.
            assertEquals(0, txNode.removeProperty("prop0"));
            txNode.setProperty("prop-1", -1);
            transaction.commit();
        }

        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            // Removing two that take up 1 block and adding one that takes up 2
            // should not create a new record.
            assertEquals(-1, txNode.removeProperty("prop-1"));
            // Hopefully prop1 exists, meaning payload is at least 16
            assertEquals(1, txNode.removeProperty("prop1"));
            // A double value should do the trick
            txNode.setProperty("propDouble", 1.0);
            transaction.commit();
        }

        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        // Adding just one now should create a new property record.
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("prop-2", -2);
            transaction.commit();
        }
        assertEquals(inUseBefore + 2, propertyRecordsInUse());
    }

    @Test
    void substituteOneLargeWithManySmallPropBlocks() {
        Node node = createNode();
        long inUseBefore = propertyRecordsInUse();
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            /*
             * Fill up with doubles and the rest with ints - we assume
             * the former take up two blocks, the latter 1.
             */
            for (int i = 0; i < PropertyType.getPayloadSizeLongs() / 2; i++) {
                txNode.setProperty("double" + i, i * 1.0);
            }
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            /*
             * I know this is stupid in that it is executed 0 or 1 times but it
             * is easier to maintain and change for different payload sizes.
             */
            for (int i = 0; i < PropertyType.getPayloadSizeLongs() % 2; i++) {
                txNode.setProperty("int" + i, i);
            }
            transaction.commit();
        }

        // Just checking that the assumptions above is correct
        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        // We assume at least one double has been added
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).removeProperty("double0");
            transaction.commit();
        }
        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            // Do the actual substitution, check that no record is created
            var txNode = transaction.getNodeById(node.getId());
            txNode.setProperty("int-1", -1);
            txNode.setProperty("int-2", -2);
            transaction.commit();
        }
        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        // Finally, make sure we actually are with a full prop record
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("int-3", -3);
            transaction.commit();
        }
        assertEquals(inUseBefore + 2, propertyRecordsInUse());
    }

    /*
     * Adds at least 3 1-block properties and removes the first and third.
     * Adds a 2-block property and checks if it is added in the same record.
     */
    @Test
    void testBlockDefragmentationWithTwoSpaces() {
        assumeTrue(PropertyType.getPayloadSizeLongs() > 2);
        Node node = createNode();
        long inUseBefore = propertyRecordsInUse();

        int stuffedIntegers = 0;
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (; stuffedIntegers < PropertyType.getPayloadSizeLongs(); stuffedIntegers++) {
                txNode.setProperty("int" + stuffedIntegers, stuffedIntegers);
            }
            transaction.commit();
        }

        // Basic check that integers take up one (8 byte) block.
        assertEquals(stuffedIntegers, PropertyType.getPayloadSizeLongs());

        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            // Remove first and third
            txNode.removeProperty("int0");
            txNode.removeProperty("int2");
            transaction.commit();
        }
        // Add the two block thing.
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("theDouble", 1.0);
            transaction.commit();
        }
        // Let's make sure everything is in one record and with proper values.
        assertEquals(inUseBefore + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            assertNull(txNode.getProperty("int0", null));
            assertEquals(1, txNode.getProperty("int1"));
            assertNull(txNode.getProperty("int2", null));
            for (int i = 3; i < stuffedIntegers; i++) {
                assertEquals(i, txNode.getProperty("int" + i));
            }
            assertEquals(1.0, txNode.getProperty("theDouble"));
            transaction.commit();
        }
    }

    @Test
    void checkDeletesRemoveRecordsWhenProper() {
        Node node = createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        int stuffedBooleans = 0;
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (; stuffedBooleans < PropertyType.getPayloadSizeLongs(); stuffedBooleans++) {
                txNode.setProperty("boolean" + stuffedBooleans, stuffedBooleans % 2 == 0);
            }
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("theExraOne", true);
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 2, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (int i = 0; i < stuffedBooleans; i++) {
                assertEquals(Boolean.valueOf(i % 2 == 0), txNode.removeProperty("boolean" + i));
            }
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (int i = 0; i < stuffedBooleans; i++) {
                assertFalse(txNode.hasProperty("boolean" + i));
            }
            assertEquals(Boolean.TRUE, txNode.getProperty("theExraOne"));
            transaction.commit();
        }
    }

    /*
     * Creates 3 records and deletes stuff from the middle one. Assumes that a 2 character
     * string that is a number fits in one block.
     */
    @Test
    void testMessWithMiddleRecordDeletes() {
        Node node = createNode();
        long recordsInUseAtStart = propertyRecordsInUse();
        long offset = lastUsedRecordId(propertyStore()) + 1; // expected first record id that will be used for this test

        int stuffedShortStrings = 0;
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (; stuffedShortStrings < 3 * PropertyType.getPayloadSizeLongs(); stuffedShortStrings++) {
                txNode.setProperty("shortString" + stuffedShortStrings, String.valueOf(stuffedShortStrings));
            }
            transaction.commit();
        }
        assertEquals(recordsInUseAtStart + 3, propertyRecordsInUse());

        final List<Pair<String, Object>> middleRecordProps = getPropertiesFromRecord(offset + 1);
        final Pair<String, Object> secondBlockInMiddleRecord = middleRecordProps.get(1);
        final Pair<String, Object> thirdBlockInMiddleRecord = middleRecordProps.get(2);

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            assertEquals(secondBlockInMiddleRecord.getTwo(), txNode.removeProperty(secondBlockInMiddleRecord.getOne()));
            assertEquals(thirdBlockInMiddleRecord.getTwo(), txNode.removeProperty(thirdBlockInMiddleRecord.getOne()));
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 3, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (int i = 0; i < stuffedShortStrings; i++) {
                if (secondBlockInMiddleRecord.getTwo().equals(String.valueOf(i))
                        || thirdBlockInMiddleRecord.getTwo().equals(String.valueOf(i))) {
                    assertFalse(txNode.hasProperty("shortString" + i));
                } else {
                    assertEquals(String.valueOf(i), txNode.getProperty("shortString" + i));
                }
            }
            // Start deleting stuff. First, all the middle property blocks
            int deletedProps = 0;

            for (Pair<String, Object> prop : middleRecordProps) {
                final String name = prop.getOne();
                if (txNode.hasProperty(name)) {
                    deletedProps++;
                    txNode.removeProperty(name);
                }
            }

            assertEquals(PropertyType.getPayloadSizeLongs() - 2, deletedProps);
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 2, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            middleRecordProps.forEach(nameAndValue -> assertFalse(txNode.hasProperty(nameAndValue.getOne())));
            getPropertiesFromRecord(offset).forEach(nameAndValue -> {
                final String name = nameAndValue.getOne();
                final Object value = nameAndValue.getTwo();
                assertEquals(value, txNode.removeProperty(name));
            });
            getPropertiesFromRecord(offset + 2).forEach(nameAndValue -> {
                final String name = nameAndValue.getOne();
                final Object value = nameAndValue.getTwo();
                assertEquals(value, txNode.removeProperty(name));
            });
            transaction.commit();
        }
    }

    @Test
    void mixAndPackDifferentTypes() {
        Node node = createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        int stuffedShortStrings = 0;
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            for (; stuffedShortStrings < PropertyType.getPayloadSizeLongs(); stuffedShortStrings++) {
                txNode.setProperty("shortString" + stuffedShortStrings, String.valueOf(stuffedShortStrings));
            }
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            txNode.removeProperty("shortString0");
            txNode.removeProperty("shortString2");
            txNode.setProperty("theDoubleOne", -1.0);
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());

            for (int i = 0; i < stuffedShortStrings; i++) {
                if (i == 0) {
                    assertFalse(txNode.hasProperty("shortString" + i));
                } else if (i == 2) {
                    assertFalse(txNode.hasProperty("shortString" + i));
                } else {
                    assertEquals(String.valueOf(i), txNode.getProperty("shortString" + i));
                }
            }
            assertEquals(-1.0, txNode.getProperty("theDoubleOne"));
            transaction.commit();
        }
    }

    @Test
    void testAdditionsHappenAtTheFirstRecordIfFits1() {
        Node node = createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            txNode.setProperty("int1", 1);
            txNode.setProperty("double1", 1.0);
            txNode.setProperty("int2", 2);
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).removeProperty("double1");
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("double2", 1.0);
            transaction.commit();
        }
        assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("paddingBoolean", false);
            transaction.commit();
        }
        assertEquals(recordsInUseAtStart + 2, propertyRecordsInUse());
    }

    @Test
    void testAdditionHappensInTheMiddleIfItFits() {
        Node node = createNode();

        long recordsInUseAtStart = propertyRecordsInUse();

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            txNode.setProperty("int1", 1);
            txNode.setProperty("double1", 1.0);
            txNode.setProperty("int2", 2);

            int stuffedShortStrings = 0;
            for (; stuffedShortStrings < PropertyType.getPayloadSizeLongs(); stuffedShortStrings++) {
                txNode.setProperty("shortString" + stuffedShortStrings, String.valueOf(stuffedShortStrings));
            }
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 2, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            txNode.removeProperty("shortString" + 1);
            txNode.setProperty("int3", 3);
            transaction.commit();
        }
        assertEquals(recordsInUseAtStart + 2, propertyRecordsInUse());
    }

    @Test
    void testChangePropertyType() {
        Node node = createNode();

        long recordsInUseAtStart = propertyRecordsInUse();

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            int stuffedShortStrings = 0;
            for (; stuffedShortStrings < PropertyType.getPayloadSizeLongs(); stuffedShortStrings++) {
                txNode.setProperty("shortString" + stuffedShortStrings, String.valueOf(stuffedShortStrings));
            }
            transaction.commit();
        }
        assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("shortString1", 1.0);
            transaction.commit();
        }
    }

    @Test
    void testRevertOverflowingChange() {
        long recordsInUseAtStart;
        Relationship rel;
        long valueRecordsInUseAtStart;
        try (Transaction transaction = getGraphDb().beginTx()) {
            rel = transaction
                    .createNode()
                    .createRelationshipTo(transaction.createNode(), RelationshipType.withName("INVALIDATES"));

            recordsInUseAtStart = propertyRecordsInUse();
            valueRecordsInUseAtStart = dynamicArrayRecordsInUse();

            rel.setProperty("theByte", (byte) -8);
            rel.setProperty("theDoubleThatGrows", Math.PI);
            rel.setProperty("theInteger", -444345);

            rel.setProperty("theDoubleThatGrows", new long[] {1L << 63, 1L << 63, 1L << 63});

            rel.setProperty("theDoubleThatGrows", Math.E);
            transaction.commit();
        }

        // Then
        /*
         * The following line should pass if we have packing on property block
         * size shrinking.
         */
        // assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );
        try (Transaction transaction = getGraphDb().beginTx()) {
            assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());
            assertEquals(valueRecordsInUseAtStart, dynamicArrayRecordsInUse());

            var txRel = transaction.getRelationshipById(rel.getId());
            assertEquals((byte) -8, txRel.getProperty("theByte"));
            assertEquals(-444345, txRel.getProperty("theInteger"));
            assertEquals(Math.E, txRel.getProperty("theDoubleThatGrows"));
            transaction.commit();
        }
    }

    @Test
    void testYoYoArrayPropertyWithinTx() {
        testArrayBase(false);
    }

    @Test
    void testYoYoArrayPropertyOverTxs() {
        testArrayBase(true);
    }

    private void testArrayBase(boolean withNewTx) {
        Relationship rel;
        try (Transaction transaction = getGraphDb().beginTx()) {
            rel = transaction
                    .createNode()
                    .createRelationshipTo(transaction.createNode(), RelationshipType.withName("LOCKS"));
            transaction.commit();
        }

        long recordsInUseAtStart = propertyRecordsInUse();
        long valueRecordsInUseAtStart = dynamicArrayRecordsInUse();

        List<Long> theData = new ArrayList<>();
        Transaction tx = getGraphDb().beginTx();
        for (int i = 0; i < PropertyType.getPayloadSizeLongs() - 1; i++) {
            theData.add(1L << 63);
            Long[] value = theData.toArray(new Long[] {});
            tx.getRelationshipById(rel.getId()).setProperty("yoyo", value);
            if (withNewTx) {
                tx.commit();
                tx = getGraphDb().beginTx();
                assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());
                assertEquals(valueRecordsInUseAtStart, dynamicArrayRecordsInUse());
            }
        }
        tx.commit();

        theData.add(1L << 63);
        Long[] value = theData.toArray(new Long[] {});
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getRelationshipById(rel.getId()).setProperty("yoyo", value);
            transaction.commit();
        }

        try (Transaction transaction = getGraphDb().beginTx()) {
            assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());
            assertEquals(valueRecordsInUseAtStart + 1, dynamicArrayRecordsInUse());
            transaction.getRelationshipById(rel.getId()).setProperty("filler", new long[] {1L << 63, 1L << 63, 1L << 63
            });
            transaction.commit();
        }
        assertEquals(recordsInUseAtStart + 2, propertyRecordsInUse());
    }

    @Test
    void testRemoveZigZag() {
        long recordsInUseAtStart;
        int propRecCount = 1;
        Relationship rel;
        try (Transaction transaction = getGraphDb().beginTx()) {
            rel = transaction
                    .createNode()
                    .createRelationshipTo(transaction.createNode(), RelationshipType.withName("LOCKS"));

            recordsInUseAtStart = propertyRecordsInUse();

            for (; propRecCount <= 3; propRecCount++) {
                for (int i = 1; i <= PropertyType.getPayloadSizeLongs(); i++) {
                    rel.setProperty("int" + (propRecCount * 10 + i), propRecCount * 10 + i);
                }
            }
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 3, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txRel = transaction.getRelationshipById(rel.getId());
            for (int i = 1; i <= PropertyType.getPayloadSizeLongs(); i++) {
                for (int j = 1; j < propRecCount; j++) {
                    assertEquals(j * 10 + i, txRel.removeProperty("int" + (j * 10 + i)));
                    if (i == PropertyType.getPayloadSize() - 1 && j != propRecCount - 1) {
                        assertEquals(recordsInUseAtStart + (propRecCount - j), propertyRecordsInUse());
                    } else if (i == PropertyType.getPayloadSize() - 1 && j == propRecCount - 1) {
                        assertEquals(recordsInUseAtStart, propertyRecordsInUse());
                    } else {
                        assertEquals(recordsInUseAtStart + 3, propertyRecordsInUse());
                    }
                }
            }
            for (int i = 1; i <= PropertyType.getPayloadSizeLongs(); i++) {
                for (int j = 1; j < propRecCount; j++) {
                    assertFalse(txRel.hasProperty("int" + (j * 10 + i)));
                }
            }
            transaction.commit();
        }

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txRel = transaction.getRelationshipById(rel.getId());
            for (int i = 1; i <= PropertyType.getPayloadSizeLongs(); i++) {
                for (int j = 1; j < propRecCount; j++) {
                    assertFalse(txRel.hasProperty("int" + (j * 10 + i)));
                }
            }
            transaction.commit();
        }
        assertEquals(recordsInUseAtStart, propertyRecordsInUse());
    }

    @Test
    void testSetWithSameValue() {
        Node node = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            txNode.setProperty("rev_pos", "40000633e7ad67ff");
            assertEquals("40000633e7ad67ff", txNode.getProperty("rev_pos"));
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            txNode.setProperty("rev_pos", "40000633e7ad67ef");
            assertEquals("40000633e7ad67ef", txNode.getProperty("rev_pos"));
            transaction.commit();
        }
    }

    private void testStringBase(boolean withNewTx) {
        Node node = createNode();

        long recordsInUseAtStart = propertyRecordsInUse();
        long valueRecordsInUseAtStart = dynamicStringRecordsInUse();

        String data = "0";
        int counter = 1;
        Transaction tx = getGraphDb().beginTx();
        while (dynamicStringRecordsInUse() == valueRecordsInUseAtStart) {
            data += counter++;
            tx.getNodeById(node.getId()).setProperty("yoyo", data);
            if (withNewTx) {
                tx.commit();
                tx = getGraphDb().beginTx();
                assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());
            }
        }
        tx.commit();

        data = data.substring(0, data.length() - 2);
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("yoyo", data);
            transaction.commit();
        }

        assertEquals(valueRecordsInUseAtStart, dynamicStringRecordsInUse());
        assertEquals(recordsInUseAtStart + 1, propertyRecordsInUse());

        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("fillerBoolean", true);
            transaction.commit();
        }

        assertEquals(recordsInUseAtStart + 2, propertyRecordsInUse());
    }

    @Test
    void testStringWithTx() {
        testStringBase(true);
    }

    @Test
    void testRemoveFirstOfTwo() {
        Node node = createNode();

        long recordsInUseAtStart = propertyRecordsInUse();

        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            txNode.setProperty("Double1", 1.0);
            txNode.setProperty("Int1", 1);
            txNode.setProperty("Int2", 2);
            txNode.setProperty("Int2", 1.2);
            txNode.setProperty("Int2", 2);
            txNode.setProperty("Double3", 3.0);
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            assertEquals(recordsInUseAtStart + 2, propertyRecordsInUse());
            assertEquals(1.0, txNode.getProperty("Double1"));
            assertEquals(1, txNode.getProperty("Int1"));
            assertEquals(2, txNode.getProperty("Int2"));
            assertEquals(3.0, txNode.getProperty("Double3"));
            transaction.commit();
        }
    }

    @Test
    void deleteNodeWithNewPropertyRecordShouldFreeTheNewRecord() {
        final long propcount = propertyRecordsInUse();
        Node node = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            var txNode = transaction.getNodeById(node.getId());
            txNode.setProperty("one", 1);
            txNode.setProperty("two", 2);
            txNode.setProperty("three", 3);
            txNode.setProperty("four", 4);
            transaction.commit();
        }
        assertEquals(propcount + 1, propertyRecordsInUse(), "Invalid assumption: property record count");
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("final", 666);
            transaction.commit();
        }
        assertEquals(propcount + 2, propertyRecordsInUse(), "Invalid assumption: property record count");
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node.getId()).delete();
            transaction.commit();
        }
        assertEquals(propcount, propertyRecordsInUse(), "All property records should be freed");
    }
}
