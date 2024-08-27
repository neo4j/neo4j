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

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.internal.recordstorage.TestRelType.IN;
import static org.neo4j.internal.recordstorage.TestRelType.LOOP;
import static org.neo4j.internal.recordstorage.TestRelType.OUT;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

import java.util.HashSet;
import java.util.Set;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Direction;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.storageengine.util.SingleDegree;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.storage.RecordStorageEngineSupport;

@ExtendWith(RandomExtension.class)
public class RecordStorageReaderRelTypesAndDegreeTest extends RecordStorageReaderTestBase {
    protected static final int RELATIONSHIPS_COUNT = 20;

    @Inject
    protected RandomSupport random;

    @Override
    protected RecordStorageEngineSupport.Builder modify(RecordStorageEngineSupport.Builder builder) {
        return builder.setting(GraphDatabaseSettings.dense_node_threshold, RELATIONSHIPS_COUNT);
    }

    @Test
    void degreesForDenseNodeWithPartiallyDeletedRelGroupChain() throws Exception {
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain();

        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain(IN);
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain(OUT);
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain(LOOP);

        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain(IN, OUT);
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain(OUT, LOOP);
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain(IN, LOOP);

        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain(IN, OUT, LOOP);
    }

    @Test
    void degreesForDenseNodeWithPartiallyDeletedRelChains() throws Exception {
        testDegreesForDenseNodeWithPartiallyDeletedRelChains(false, false, false);

        testDegreesForDenseNodeWithPartiallyDeletedRelChains(true, false, false);
        testDegreesForDenseNodeWithPartiallyDeletedRelChains(false, true, false);
        testDegreesForDenseNodeWithPartiallyDeletedRelChains(false, false, true);

        testDegreesForDenseNodeWithPartiallyDeletedRelChains(true, true, false);
        testDegreesForDenseNodeWithPartiallyDeletedRelChains(true, true, true);
        testDegreesForDenseNodeWithPartiallyDeletedRelChains(true, false, true);

        testDegreesForDenseNodeWithPartiallyDeletedRelChains(true, true, true);
    }

    @Test
    void degreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain() throws Exception {
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain();

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain(IN);
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain(OUT);
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain(LOOP);

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain(IN, OUT);
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain(IN, LOOP);
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain(OUT, LOOP);

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain(IN, OUT, LOOP);
    }

    @Test
    void degreeByDirectionForDenseNodeWithPartiallyDeletedRelChains() throws Exception {
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains(false, false, false);

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains(true, false, false);
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains(false, true, false);
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains(false, false, true);

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains(true, true, false);
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains(true, true, true);
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains(true, false, true);

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains(true, true, true);
    }

    @Test
    void degreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain() throws Exception {
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain();

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain(IN);
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain(OUT);
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain(LOOP);

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain(IN, OUT);
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain(OUT, LOOP);
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain(IN, LOOP);

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain(IN, OUT, LOOP);
    }

    @Test
    void degreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains() throws Exception {
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains(false, false, false);

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains(true, false, false);
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains(false, true, false);
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains(false, false, true);

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains(true, true, false);
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains(true, true, true);
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains(true, false, true);

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains(true, true, true);
    }

    protected void testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain(TestRelType... typesToDelete)
            throws Exception {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode(inRelCount, outRelCount, loopRelCount);
        StorageNodeCursor cursor = newCursor(nodeId);

        for (TestRelType type : typesToDelete) {
            markRelGroupNotInUse(nodeId, type);
            switch (type) {
                case IN:
                    inRelCount = 0;
                    break;
                case OUT:
                    outRelCount = 0;
                    break;
                case LOOP:
                    loopRelCount = 0;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " + type);
            }
        }

        assertEquals(outRelCount + loopRelCount, degreeForDirection(cursor, OUTGOING));
        assertEquals(inRelCount + loopRelCount, degreeForDirection(cursor, INCOMING));
        assertEquals(inRelCount + outRelCount + loopRelCount, degreeForDirection(cursor, BOTH));
    }

    protected void testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains(
            boolean modifyInChain, boolean modifyOutChain, boolean modifyLoopChain) throws Exception {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode(inRelCount, outRelCount, loopRelCount);
        StorageNodeCursor cursor = newCursor(nodeId);

        if (modifyInChain) {
            markRandomRelsInGroupNotInUse(nodeId, IN);
        }
        if (modifyOutChain) {
            markRandomRelsInGroupNotInUse(nodeId, OUT);
        }
        if (modifyLoopChain) {
            markRandomRelsInGroupNotInUse(nodeId, LOOP);
        }

        assertEquals(outRelCount + loopRelCount, degreeForDirection(cursor, OUTGOING));
        assertEquals(inRelCount + loopRelCount, degreeForDirection(cursor, INCOMING));
        assertEquals(inRelCount + outRelCount + loopRelCount, degreeForDirection(cursor, BOTH));
    }

    protected static int degreeForDirection(StorageNodeCursor cursor, Direction direction) {
        return degree(cursor, selection(direction));
    }

    protected static int degreeForDirectionAndType(StorageNodeCursor cursor, Direction direction, int relType) {
        return degree(cursor, selection(relType, direction));
    }

    private static int degree(StorageNodeCursor cursor, RelationshipSelection selection) {
        SingleDegree degree = new SingleDegree();
        cursor.degrees(selection, degree);
        return degree.getTotal();
    }

    protected void testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain(
            TestRelType... typesToDelete) throws Exception {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode(inRelCount, outRelCount, loopRelCount);
        StorageNodeCursor cursor = newCursor(nodeId);

        for (TestRelType type : typesToDelete) {
            markRelGroupNotInUse(nodeId, type);
            switch (type) {
                case IN:
                    inRelCount = 0;
                    break;
                case OUT:
                    outRelCount = 0;
                    break;
                case LOOP:
                    loopRelCount = 0;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " + type);
            }
        }

        assertEquals(0, degreeForDirectionAndType(cursor, OUTGOING, relTypeId(IN)));
        assertEquals(outRelCount, degreeForDirectionAndType(cursor, OUTGOING, relTypeId(OUT)));
        assertEquals(loopRelCount, degreeForDirectionAndType(cursor, OUTGOING, relTypeId(LOOP)));

        assertEquals(0, degreeForDirectionAndType(cursor, INCOMING, relTypeId(OUT)));
        assertEquals(inRelCount, degreeForDirectionAndType(cursor, INCOMING, relTypeId(IN)));
        assertEquals(loopRelCount, degreeForDirectionAndType(cursor, INCOMING, relTypeId(LOOP)));

        assertEquals(inRelCount, degreeForDirectionAndType(cursor, BOTH, relTypeId(IN)));
        assertEquals(outRelCount, degreeForDirectionAndType(cursor, BOTH, relTypeId(OUT)));
        assertEquals(loopRelCount, degreeForDirectionAndType(cursor, BOTH, relTypeId(LOOP)));
    }

    protected void testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains(
            boolean modifyInChain, boolean modifyOutChain, boolean modifyLoopChain) throws Exception {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode(inRelCount, outRelCount, loopRelCount);
        StorageNodeCursor cursor = newCursor(nodeId);

        if (modifyInChain) {
            markRandomRelsInGroupNotInUse(nodeId, IN);
        }
        if (modifyOutChain) {
            markRandomRelsInGroupNotInUse(nodeId, OUT);
        }
        if (modifyLoopChain) {
            markRandomRelsInGroupNotInUse(nodeId, LOOP);
        }

        assertEquals(0, degreeForDirectionAndType(cursor, OUTGOING, relTypeId(IN)));
        assertEquals(outRelCount, degreeForDirectionAndType(cursor, OUTGOING, relTypeId(OUT)));
        assertEquals(loopRelCount, degreeForDirectionAndType(cursor, OUTGOING, relTypeId(LOOP)));

        assertEquals(0, degreeForDirectionAndType(cursor, INCOMING, relTypeId(OUT)));
        assertEquals(inRelCount, degreeForDirectionAndType(cursor, INCOMING, relTypeId(IN)));
        assertEquals(loopRelCount, degreeForDirectionAndType(cursor, INCOMING, relTypeId(LOOP)));

        assertEquals(inRelCount, degreeForDirectionAndType(cursor, BOTH, relTypeId(IN)));
        assertEquals(outRelCount, degreeForDirectionAndType(cursor, BOTH, relTypeId(OUT)));
        assertEquals(loopRelCount, degreeForDirectionAndType(cursor, BOTH, relTypeId(LOOP)));
    }

    @Test
    void relationshipTypesForDenseNodeWithPartiallyDeletedRelGroupChain() throws Exception {
        testRelationshipTypesForDenseNode(this::noNodeChange, asSet(IN, OUT, LOOP));

        testRelationshipTypesForDenseNode(nodeId -> markRelGroupNotInUse(nodeId, IN), asSet(OUT, LOOP));
        testRelationshipTypesForDenseNode(nodeId -> markRelGroupNotInUse(nodeId, OUT), asSet(IN, LOOP));
        testRelationshipTypesForDenseNode(nodeId -> markRelGroupNotInUse(nodeId, LOOP), asSet(IN, OUT));

        testRelationshipTypesForDenseNode(nodeId -> markRelGroupNotInUse(nodeId, IN, OUT), asSet(LOOP));
        testRelationshipTypesForDenseNode(nodeId -> markRelGroupNotInUse(nodeId, IN, LOOP), asSet(OUT));
        testRelationshipTypesForDenseNode(nodeId -> markRelGroupNotInUse(nodeId, OUT, LOOP), asSet(IN));

        testRelationshipTypesForDenseNode(nodeId -> markRelGroupNotInUse(nodeId, IN, OUT, LOOP), emptySet());
    }

    @Test
    void relationshipTypesForDenseNodeWithPartiallyDeletedRelChains() throws Exception {
        testRelationshipTypesForDenseNode(this::markRandomRelsNotInUse, asSet(IN, OUT, LOOP));
    }

    protected void testRelationshipTypesForDenseNode(LongConsumer nodeChanger, Set<TestRelType> expectedTypes)
            throws Exception {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode(inRelCount, outRelCount, loopRelCount);
        nodeChanger.accept(nodeId);

        StorageNodeCursor cursor = newCursor(nodeId);

        assertEquals(expectedTypes, relTypes(cursor));
    }

    protected Set<TestRelType> relTypes(StorageNodeCursor cursor) {
        Set<TestRelType> types = new HashSet<>();
        for (int relType : cursor.relationshipTypes()) {
            types.add(relTypeForId(relType));
        }
        return types;
    }

    protected void testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain(TestRelType... typesToDelete)
            throws Exception {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode(inRelCount, outRelCount, loopRelCount);
        StorageNodeCursor cursor = newCursor(nodeId);

        for (TestRelType type : typesToDelete) {
            markRelGroupNotInUse(nodeId, type);
            switch (type) {
                case IN:
                    inRelCount = 0;
                    break;
                case OUT:
                    outRelCount = 0;
                    break;
                case LOOP:
                    loopRelCount = 0;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " + type);
            }
        }

        Set<TestDegreeItem> expectedDegrees = new HashSet<>();
        if (outRelCount != 0) {
            expectedDegrees.add(new TestDegreeItem(relTypeId(OUT), outRelCount, 0));
        }
        if (inRelCount != 0) {
            expectedDegrees.add(new TestDegreeItem(relTypeId(IN), 0, inRelCount));
        }
        if (loopRelCount != 0) {
            expectedDegrees.add(new TestDegreeItem(relTypeId(LOOP), loopRelCount, loopRelCount));
        }

        Set<TestDegreeItem> actualDegrees = degrees(cursor);

        assertEquals(expectedDegrees, actualDegrees);
    }

    protected void testDegreesForDenseNodeWithPartiallyDeletedRelChains(
            boolean modifyInChain, boolean modifyOutChain, boolean modifyLoopChain) throws Exception {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode(inRelCount, outRelCount, loopRelCount);
        StorageNodeCursor cursor = newCursor(nodeId);

        if (modifyInChain) {
            markRandomRelsInGroupNotInUse(nodeId, IN);
        }
        if (modifyOutChain) {
            markRandomRelsInGroupNotInUse(nodeId, OUT);
        }
        if (modifyLoopChain) {
            markRandomRelsInGroupNotInUse(nodeId, LOOP);
        }

        Set<TestDegreeItem> expectedDegrees = new HashSet<>(asList(
                new TestDegreeItem(relTypeId(OUT), outRelCount, 0),
                new TestDegreeItem(relTypeId(IN), 0, inRelCount),
                new TestDegreeItem(relTypeId(LOOP), loopRelCount, loopRelCount)));

        Set<TestDegreeItem> actualDegrees = degrees(cursor);

        assertEquals(expectedDegrees, actualDegrees);
    }

    protected static Set<TestDegreeItem> degrees(StorageNodeCursor nodeCursor) {
        Set<TestDegreeItem> degrees = new HashSet<>();
        EagerDegrees nodeDegrees = new EagerDegrees();
        nodeCursor.degrees(ALL_RELATIONSHIPS, nodeDegrees);
        for (int type : nodeDegrees.types()) {
            degrees.add(new TestDegreeItem(type, nodeDegrees.outgoingDegree(type), nodeDegrees.incomingDegree(type)));
        }
        return degrees;
    }

    protected StorageNodeCursor newCursor(long nodeId) {
        StorageNodeCursor nodeCursor =
                storageReader.allocateNodeCursor(NULL_CONTEXT, storageCursors, EmptyMemoryTracker.INSTANCE);
        nodeCursor.single(nodeId);
        assertTrue(nodeCursor.next());
        return nodeCursor;
    }

    protected void noNodeChange(long nodeId) {}

    protected void markRandomRelsNotInUse(long nodeId) {
        for (TestRelType type : TestRelType.values()) {
            markRandomRelsInGroupNotInUse(nodeId, type);
        }
    }

    protected void markRandomRelsInGroupNotInUse(long nodeId, TestRelType type) {
        NodeRecord node = getNodeRecord(nodeId);
        assertTrue(node.isDense());

        long relGroupId = node.getNextRel();
        while (relGroupId != NO_NEXT_RELATIONSHIP.intValue()) {
            RelationshipGroupRecord relGroup = getRelGroupRecord(relGroupId);

            if (type == relTypeForId(relGroup.getType())) {
                markRandomRelsInChainNotInUse(relGroup.getFirstOut());
                markRandomRelsInChainNotInUse(relGroup.getFirstIn());
                markRandomRelsInChainNotInUse(relGroup.getFirstLoop());
                return;
            }

            relGroupId = relGroup.getNext();
        }

        throw new IllegalStateException("No relationship group with type: " + type + " found");
    }

    protected void markRandomRelsInChainNotInUse(long relId) {
        if (relId != NO_NEXT_RELATIONSHIP.intValue()) {
            RelationshipRecord record = getRelRecord(relId);

            boolean shouldBeMarked = random.nextBoolean();
            if (shouldBeMarked) {
                record.setInUse(false);
                update(record);
            }

            markRandomRelsInChainNotInUse(record.getFirstNextRel());
            boolean isLoopRelationship = record.getFirstNextRel() == record.getSecondNextRel();
            if (!isLoopRelationship) {
                markRandomRelsInChainNotInUse(record.getSecondNextRel());
            }
        }
    }

    protected void markRelGroupNotInUse(long nodeId, TestRelType... types) {
        NodeRecord node = getNodeRecord(nodeId);
        assertTrue(node.isDense());

        Set<TestRelType> typesToRemove = asSet(types);

        long relGroupId = node.getNextRel();
        while (relGroupId != NO_NEXT_RELATIONSHIP.intValue()) {
            RelationshipGroupRecord relGroup = getRelGroupRecord(relGroupId);
            TestRelType type = relTypeForId(relGroup.getType());

            if (typesToRemove.contains(type)) {
                relGroup.setInUse(false);
                update(relGroup);
            }

            relGroupId = relGroup.getNext();
        }
    }

    protected int relTypeId(TestRelType type) {
        int id = relationshipTypeId(type);
        assertNotEquals(NO_TOKEN, id);
        return id;
    }

    protected long createNode(int inRelCount, int outRelCount, int loopRelCount) throws Exception {
        long nodeId = createNode(map());
        for (int i = 0; i < inRelCount; i++) {
            createRelationship(createNode(map()), nodeId, IN);
        }
        for (int i = 0; i < outRelCount; i++) {
            createRelationship(nodeId, createNode(map()), OUT);
        }
        for (int i = 0; i < loopRelCount; i++) {
            createRelationship(nodeId, nodeId, LOOP);
        }
        return nodeId;
    }

    protected TestRelType relTypeForId(int id) {
        try {
            return TestRelType.valueOf(relationshipType(id));
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    protected static <R extends AbstractBaseRecord> R getRecord(RecordStore<R> store, long id, PageCursor storeCursor) {
        return store.getRecordByCursor(
                id, store.newRecord(), RecordLoad.FORCE, storeCursor, EmptyMemoryTracker.INSTANCE);
    }

    protected NodeRecord getNodeRecord(long id) {
        return getRecord(resolveNeoStores().getNodeStore(), id, storageCursors.readCursor(NODE_CURSOR));
    }

    protected RelationshipRecord getRelRecord(long id) {
        return getRecord(resolveNeoStores().getRelationshipStore(), id, storageCursors.readCursor(RELATIONSHIP_CURSOR));
    }

    protected RelationshipGroupRecord getRelGroupRecord(long id) {
        return getRecord(resolveNeoStores().getRelationshipGroupStore(), id, storageCursors.readCursor(GROUP_CURSOR));
    }

    protected void update(RelationshipGroupRecord record) {
        RelationshipGroupStore store = resolveNeoStores().getRelationshipGroupStore();
        try (var storeCursor = storageCursors.writeCursor(GROUP_CURSOR)) {
            store.updateRecord(record, storeCursor, NULL_CONTEXT, storageCursors);
        }
    }

    protected void update(RelationshipRecord record) {
        RelationshipStore relationshipStore = resolveNeoStores().getRelationshipStore();
        try (var storeCursor = storageCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.updateRecord(record, storeCursor, NULL_CONTEXT, storageCursors);
        }
    }

    protected NeoStores resolveNeoStores() {
        return storageEngine.testAccessNeoStores();
    }

    protected int randomRelCount() {
        return RELATIONSHIPS_COUNT + random.nextInt(20);
    }
}
