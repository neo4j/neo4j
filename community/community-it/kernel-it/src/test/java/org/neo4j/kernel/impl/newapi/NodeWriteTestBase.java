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

import static java.util.Arrays.stream;
import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

@SuppressWarnings("Duplicates")
@ExtendWith(RandomExtension.class)
public abstract class NodeWriteTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G> {
    private static final String propertyKey = "prop";
    private static final String labelName = "Town";

    @Inject
    private RandomSupport random;

    @Test
    void shouldCreateNode() throws Exception {
        long node;
        try (KernelTransaction tx = beginTransaction()) {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertEquals(node, tx.getNodeById(node).getId());
        }
    }

    @Test
    void shouldRollbackOnFailure() throws Exception {
        long node;
        try (KernelTransaction tx = beginTransaction()) {
            node = tx.dataWrite().nodeCreate();
            tx.rollback();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            tx.getNodeById(node);
            fail("There should be no node");
        } catch (NotFoundException e) {
            // expected
        }
    }

    @Test
    void shouldRemoveNode() throws Exception {
        long node = createNode();

        try (KernelTransaction tx = beginTransaction()) {
            tx.dataWrite().nodeDelete(node);
            tx.commit();
        }
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            try {
                tx.getNodeById(node);
                fail("Did not remove node");
            } catch (NotFoundException e) {
                // expected
            }
        }
    }

    @Test
    void shouldNotRemoveNodeThatDoesNotExist() throws Exception {
        long node = 0;

        try (KernelTransaction tx = beginTransaction()) {
            assertFalse(tx.dataWrite().nodeDelete(node));
            tx.rollback();
        }
        try (KernelTransaction tx = beginTransaction()) {
            assertFalse(tx.dataWrite().nodeDelete(node));
            tx.commit();
        }
        // should not crash
    }

    @Test
    void shouldAddLabelNode() throws Exception {
        // Given
        long node = createNode();

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int labelId = tx.token().labelGetOrCreateForName(labelName);
            assertTrue(tx.dataWrite().nodeAddLabel(node, labelId));
            tx.commit();
        }

        // Then
        assertLabels(node, labelName);
    }

    @Test
    void shouldAddLabelNodeOnce() throws Exception {
        long node = createNodeWithLabels(labelName);

        try (KernelTransaction tx = beginTransaction()) {
            int labelId = tx.token().labelGetOrCreateForName(labelName);
            assertFalse(tx.dataWrite().nodeAddLabel(node, labelId));
            tx.commit();
        }

        assertLabels(node, labelName);
    }

    @Test
    void shouldRemoveLabel() throws Exception {
        long nodeId = createNodeWithLabels(labelName);

        try (KernelTransaction tx = beginTransaction()) {
            int labelId = tx.token().labelGetOrCreateForName(labelName);
            assertTrue(tx.dataWrite().nodeRemoveLabel(nodeId, labelId));
            tx.commit();
        }

        assertNoLabels(nodeId);
    }

    @Test
    void shouldNotAddLabelToNonExistingNode() throws Exception {
        long node = 1337L;

        try (KernelTransaction tx = beginTransaction()) {
            int labelId = tx.token().labelGetOrCreateForName(labelName);
            assertThrows(KernelException.class, () -> tx.dataWrite().nodeAddLabel(node, labelId));
        }
    }

    @Test
    void shouldRemoveLabelOnce() throws Exception {
        int labelId;
        long nodeId = createNodeWithLabels(labelName);

        try (KernelTransaction tx = beginTransaction()) {
            labelId = tx.token().labelGetOrCreateForName(labelName);
            assertTrue(tx.dataWrite().nodeRemoveLabel(nodeId, labelId));
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            labelId = tx.token().labelGetOrCreateForName(labelName);
            assertFalse(tx.dataWrite().nodeRemoveLabel(nodeId, labelId));
            tx.commit();
        }

        assertNoLabels(nodeId);
    }

    @Test
    void shouldAddPropertyToNode() throws Exception {
        // Given
        long node = createNode();

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().nodeSetProperty(node, token, stringValue("hello")))
                    .isEqualTo(NO_VALUE);
            tx.commit();
        }

        // Then
        assertProperty(node, propertyKey, "hello");
    }

    @Test
    void shouldRollbackSetNodeProperty() throws Exception {
        // Given
        long node = createNode();

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().nodeSetProperty(node, token, stringValue("hello")))
                    .isEqualTo(NO_VALUE);
            tx.rollback();
        }

        // Then
        assertNoProperty(node, propertyKey);
    }

    @Test
    void shouldThrowWhenSettingPropertyOnDeletedNode() throws Exception {
        // Given
        long node = createNode();
        deleteNode(node);

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThatThrownBy(() -> tx.dataWrite().nodeSetProperty(node, token, stringValue("hello")))
                    .isInstanceOf(EntityNotFoundException.class);
        }
    }

    @Test
    void shouldUpdatePropertyToNode() throws Exception {
        // Given
        long node = createNodeWithProperty(propertyKey, 42);

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().nodeSetProperty(node, token, stringValue("hello")))
                    .isEqualTo(intValue(42));
            tx.commit();
        }

        // Then
        assertProperty(node, propertyKey, "hello");
    }

    @Test
    void shouldRemovePropertyFromNode() throws Exception {
        // Given
        long node = createNodeWithProperty(propertyKey, 42);

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().nodeRemoveProperty(node, token)).isEqualTo(intValue(42));
            tx.commit();
        }

        // Then
        assertNoProperty(node, propertyKey);
    }

    @Test
    void shouldRemoveNonExistingPropertyFromNode() throws Exception {
        // Given
        long node = createNode();

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().nodeRemoveProperty(node, token)).isEqualTo(NO_VALUE);
            tx.commit();
        }

        // Then
        assertNoProperty(node, propertyKey);
    }

    @Test
    void shouldRemovePropertyFromNodeTwice() throws Exception {
        // Given
        long node = createNodeWithProperty(propertyKey, 42);

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().nodeRemoveProperty(node, token)).isEqualTo(intValue(42));
            assertThat(tx.dataWrite().nodeRemoveProperty(node, token)).isEqualTo(NO_VALUE);
            tx.commit();
        }

        // Then
        assertNoProperty(node, propertyKey);
    }

    @Test
    void shouldUpdatePropertyToNodeInTransaction() throws Exception {
        // Given
        long node = createNode();

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().nodeSetProperty(node, token, stringValue("hello")))
                    .isEqualTo(NO_VALUE);
            assertThat(tx.dataWrite().nodeSetProperty(node, token, stringValue("world")))
                    .isEqualTo(stringValue("hello"));
            assertThat(tx.dataWrite().nodeSetProperty(node, token, intValue(1337)))
                    .isEqualTo(stringValue("world"));
            tx.commit();
        }

        // Then
        assertProperty(node, propertyKey, 1337);
    }

    @Test
    void shouldRemoveReSetAndTwiceRemovePropertyOnNode() throws Exception {
        // given
        long node = createNodeWithProperty(propertyKey, "bar");

        // when

        try (KernelTransaction tx = beginTransaction()) {
            int prop = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            tx.dataWrite().nodeRemoveProperty(node, prop);
            tx.dataWrite().nodeSetProperty(node, prop, Values.of("bar"));
            tx.dataWrite().nodeRemoveProperty(node, prop);
            tx.dataWrite().nodeRemoveProperty(node, prop);
            tx.commit();
        }

        // then
        assertNoProperty(node, propertyKey);
    }

    @Test
    void shouldNotWriteWhenSettingPropertyToSameValue() throws Exception {
        // Given
        Value theValue = stringValue("The Value");
        long nodeId = createNodeWithProperty(propertyKey, theValue.asObject());

        // When
        KernelTransaction tx = beginTransaction();
        int property = tx.token().propertyKeyGetOrCreateForName(propertyKey);
        assertThat(tx.dataWrite().nodeSetProperty(nodeId, property, theValue)).isEqualTo(theValue);

        assertThat(tx.commit()).isEqualTo(KernelTransaction.READ_ONLY_ID);
    }

    @Test
    void shouldSetAndReadLargeByteArrayPropertyToNode() throws Exception {
        // Given
        int prop;
        long node = createNode();
        Value largeByteArray = Values.of(new byte[100_000]);

        // When
        try (KernelTransaction tx = beginTransaction()) {
            prop = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().nodeSetProperty(node, prop, largeByteArray))
                    .isEqualTo(NO_VALUE);
            tx.commit();
        }

        // Then
        try (KernelTransaction tx = beginTransaction();
                NodeCursor nodeCursor = tx.cursors().allocateNodeCursor(tx.cursorContext());
                PropertyCursor propertyCursor =
                        tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
            tx.dataRead().singleNode(node, nodeCursor);
            assertTrue(nodeCursor.next());
            nodeCursor.properties(propertyCursor);
            assertTrue(propertyCursor.next());
            assertEquals(propertyCursor.propertyKey(), prop);
            assertThat(propertyCursor.propertyValue()).isEqualTo(largeByteArray);
        }
    }

    @Test
    void nodeApplyChangesShouldAddNonExistentLabel() throws Exception {
        // Given
        long node = createNode();

        // When
        int label;
        try (KernelTransaction tx = beginTransaction()) {
            label = tx.tokenWrite().labelGetOrCreateForName("Label");
            tx.dataWrite()
                    .nodeApplyChanges(
                            node,
                            IntSets.immutable.of(label),
                            IntSets.immutable.empty(),
                            IntObjectMaps.immutable.empty());
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                assertThat(nodeCursor.next()).isTrue();
                assertThat(nodeCursor.labels().all()).isEqualTo(new int[] {label});
            }
        });
    }

    @Test
    void nodeApplyChangesShouldNotAddExistingLabel() throws Exception {
        // Given
        String labelName = "Label";
        long node = createNodeWithLabels(labelName);

        // When
        int label;
        try (KernelTransaction tx = beginTransaction()) {
            label = tx.tokenRead().nodeLabel(labelName);
            tx.dataWrite()
                    .nodeApplyChanges(
                            node,
                            IntSets.immutable.of(label),
                            IntSets.immutable.empty(),
                            IntObjectMaps.immutable.empty());
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                assertThat(nodeCursor.next()).isTrue();
                assertThat(nodeCursor.labels().all()).isEqualTo(new int[] {label});
            }
        });
    }

    @Test
    void nodeApplyChangesShouldRemoveExistingLabel() throws Exception {
        // Given
        String labelName = "Label";
        long node = createNodeWithLabels(labelName);

        // When
        int label;
        try (KernelTransaction tx = beginTransaction()) {
            label = tx.tokenRead().nodeLabel(labelName);
            tx.dataWrite()
                    .nodeApplyChanges(
                            node,
                            IntSets.immutable.empty(),
                            IntSets.immutable.of(label),
                            IntObjectMaps.immutable.empty());
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                assertThat(nodeCursor.next()).isTrue();
                assertThat(nodeCursor.labels().all()).isEmpty();
            }
        });
    }

    @Test
    void nodeApplyChangesShouldNotRemoveNonExistentLabel() throws Exception {
        // Given
        String labelName = "Label";
        long node = createNodeWithLabels(labelName);

        // When
        int label;
        try (KernelTransaction tx = beginTransaction()) {
            label = tx.tokenRead().nodeLabel(labelName);
            int otherLabel = tx.tokenWrite().labelGetOrCreateForName("OtherLabel");
            tx.dataWrite()
                    .nodeApplyChanges(
                            node,
                            IntSets.immutable.empty(),
                            IntSets.immutable.of(otherLabel),
                            IntObjectMaps.immutable.empty());
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                assertThat(nodeCursor.next()).isTrue();
                assertThat(nodeCursor.labels().all()).isEqualTo(new int[] {label});
            }
        });
    }

    @Test
    void nodeApplyChangesShouldAddMultipleLabels() throws Exception {
        // Given
        long node = createNode();

        // When
        int label1;
        int label2;
        try (KernelTransaction tx = beginTransaction()) {
            label1 = tx.tokenWrite().labelGetOrCreateForName("Label1");
            label2 = tx.tokenWrite().labelGetOrCreateForName("Label2");
            tx.dataWrite()
                    .nodeApplyChanges(
                            node,
                            IntSets.immutable.of(label1, label2),
                            IntSets.immutable.empty(),
                            IntObjectMaps.immutable.empty());
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                assertThat(nodeCursor.next()).isTrue();
                assertThat(nodeCursor.labels().all()).isEqualTo(new int[] {label1, label2});
            }
        });
    }

    @Test
    void nodeApplyChangesShouldRemoveMultipleLabels() throws Exception {
        // Given
        String labelName1 = "Label1";
        String labelName2 = "Label2";
        String labelName3 = "Label3";
        long node = createNodeWithLabels(labelName1, labelName2, labelName3);

        // When
        int label1;
        int label2;
        int label3;
        try (KernelTransaction tx = beginTransaction()) {
            label1 = tx.tokenRead().nodeLabel(labelName1);
            label2 = tx.tokenRead().nodeLabel(labelName2);
            label3 = tx.tokenRead().nodeLabel(labelName3);
            tx.dataWrite()
                    .nodeApplyChanges(
                            node,
                            IntSets.immutable.empty(),
                            IntSets.immutable.of(label1, label2),
                            IntObjectMaps.immutable.empty());
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                assertThat(nodeCursor.next()).isTrue();
                assertThat(nodeCursor.labels().all()).isEqualTo(new int[] {label3});
            }
        });
    }

    @Test
    void nodeApplyChangesShouldAddAndRemoveMultipleLabels() throws Exception {
        // Given
        long node;
        int[] labels = new int[10];
        int[] initialLabels;
        try (KernelTransaction tx = beginTransaction()) {
            for (int i = 0; i < labels.length; i++) {
                labels[i] = tx.tokenWrite().labelGetOrCreateForName("Label" + i);
            }
            initialLabels = random.selection(labels, 0, labels.length, false);
            node = tx.dataWrite().nodeCreateWithLabels(initialLabels);
            tx.commit();
        }

        // When
        int[] addedLabels = random.selection(labels, 1, labels.length, false);
        int[] removedLabels = random.selection(labels, 1, labels.length, false);
        removedLabels = stream(removedLabels)
                .filter(label -> !contains(addedLabels, label))
                .toArray();
        try (KernelTransaction tx = beginTransaction()) {
            tx.dataWrite()
                    .nodeApplyChanges(
                            node,
                            IntSets.immutable.of(addedLabels),
                            IntSets.immutable.of(removedLabels),
                            IntObjectMaps.immutable.empty());
            tx.commit();
        }

        // Then
        MutableIntSet expectedLabels = IntSets.mutable.empty();
        stream(initialLabels).forEach(expectedLabels::add);
        stream(addedLabels).forEach(expectedLabels::add);
        stream(removedLabels).forEach(expectedLabels::remove);
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                assertThat(nodeCursor.next()).isTrue();
                assertThat(nodeCursor.labels().all()).isEqualTo(expectedLabels.toSortedArray());
            }
        });
    }

    @Test
    void nodeApplyChangesShouldAddProperty() throws Exception {
        // Given
        long node = createNode();
        String keyName = "key";
        Value value = intValue(123);

        // When
        int key;
        try (KernelTransaction tx = beginTransaction()) {
            key = tx.tokenWrite().propertyKeyGetOrCreateForName(keyName);
            tx.dataWrite()
                    .nodeApplyChanges(
                            node,
                            IntSets.immutable.empty(),
                            IntSets.immutable.empty(),
                            IntObjectMaps.immutable.of(key, value));
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT);
                    var propertyCursor = cursorFactory(ktx)
                            .allocatePropertyCursor(CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                nodeCursor.next();
                assertProperties(nodeCursor, propertyCursor, IntObjectMaps.immutable.of(key, value));
            }
        });
    }

    @Test
    void nodeApplyChangesShouldChangeProperty() throws Exception {
        // Given
        String keyName = "key";
        Value changedValue = stringValue("value");
        long node = createNodeWithProperty(keyName, 123);

        // When
        int key;
        try (KernelTransaction tx = beginTransaction()) {
            key = tx.tokenRead().propertyKey(keyName);
            tx.dataWrite()
                    .nodeApplyChanges(
                            node,
                            IntSets.immutable.empty(),
                            IntSets.immutable.empty(),
                            IntObjectMaps.immutable.of(key, changedValue));
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT);
                    var propertyCursor = cursorFactory(ktx)
                            .allocatePropertyCursor(CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                nodeCursor.next();
                assertProperties(nodeCursor, propertyCursor, IntObjectMaps.immutable.of(key, changedValue));
            }
        });
    }

    @Test
    void nodeApplyChangesShouldRemoveProperty() throws Exception {
        // Given
        String keyName = "key";
        long node = createNodeWithProperty(keyName, 123);

        // When
        int key;
        try (KernelTransaction tx = beginTransaction()) {
            key = tx.tokenRead().propertyKey(keyName);
            tx.dataWrite()
                    .nodeApplyChanges(
                            node,
                            IntSets.immutable.empty(),
                            IntSets.immutable.empty(),
                            IntObjectMaps.immutable.of(key, NO_VALUE));
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT);
                    var propertyCursor = cursorFactory(ktx)
                            .allocatePropertyCursor(CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                nodeCursor.next();
                assertProperties(nodeCursor, propertyCursor, IntObjectMaps.immutable.empty());
            }
        });
    }

    @Test
    void nodeApplyChangesShouldSetAndRemoveMultipleProperties() throws Exception {
        // Given
        long node;
        int[] keys = new int[10];
        MutableIntObjectMap<Value> initialProperties = IntObjectMaps.mutable.empty();
        try (KernelTransaction tx = beginTransaction()) {
            for (int i = 0; i < keys.length; i++) {
                keys[i] = tx.tokenWrite().propertyKeyGetOrCreateForName("key" + i);
            }
            node = tx.dataWrite().nodeCreate();
            for (int key : random.selection(keys, 0, keys.length, false)) {
                Value value = random.nextValue();
                initialProperties.put(key, value);
                tx.dataWrite().nodeSetProperty(node, key, value);
            }
            tx.commit();
        }

        // When
        MutableIntObjectMap<Value> propertyChanges = IntObjectMaps.mutable.empty();
        for (int key : random.selection(keys, 1, keys.length, false)) {
            propertyChanges.put(key, random.nextValue());
        }
        for (int key : random.selection(keys, 1, keys.length, false)) {
            propertyChanges.put(key, NO_VALUE);
        }
        try (KernelTransaction tx = beginTransaction()) {
            tx.dataWrite()
                    .nodeApplyChanges(node, IntSets.immutable.empty(), IntSets.immutable.empty(), propertyChanges);
            tx.commit();
        }

        // Then
        MutableIntObjectMap<Value> expectedProperties = IntObjectMaps.mutable.empty();
        expectedProperties.putAll(initialProperties);
        propertyChanges.forEachKeyValue((key, value) -> {
            if (value == NO_VALUE) {
                expectedProperties.remove(key);
            } else {
                expectedProperties.put(key, value);
            }
        });
        transaction(ktx -> {
            try (var nodeCursor = cursorFactory(ktx).allocateNodeCursor(CursorContext.NULL_CONTEXT);
                    var propertyCursor = cursorFactory(ktx)
                            .allocatePropertyCursor(CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                ktx.dataRead().singleNode(node, nodeCursor);
                assertThat(nodeCursor.next()).isTrue();
                assertProperties(nodeCursor, propertyCursor, expectedProperties);
            }
        });
    }

    @Test
    void nodeApplyChangesShouldApplyAllTypesOfChanges() throws Exception {
        // Given
        int[] possibleLabelIds = new int[20];
        int[] possiblePropertyKeyIds = new int[possibleLabelIds.length];
        try (KernelTransaction tx = beginTransaction()) {
            for (int i = 0; i < possibleLabelIds.length; i++) {
                possibleLabelIds[i] = tx.tokenWrite().labelGetOrCreateForName("Label" + i);
                possiblePropertyKeyIds[i] = tx.tokenWrite().propertyKeyGetOrCreateForName("Key" + i);
            }
        }
        IntSet initialLabels =
                IntSets.immutable.of(random.selection(possibleLabelIds, 0, possibleLabelIds.length / 2, false));
        MutableIntObjectMap<Value> initialProperties = IntObjectMaps.mutable.empty();
        long nodeId;
        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreateWithLabels(initialLabels.toArray());
            for (int key : random.selection(possiblePropertyKeyIds, 0, possiblePropertyKeyIds.length, false)) {
                Value value = random.nextValue();
                initialProperties.put(key, value);
                tx.dataWrite().nodeSetProperty(nodeId, key, value);
            }
            tx.commit();
        }

        // When
        MutableIntSet addedLabels = IntSets.mutable.empty();
        int numAddedLabels = random.nextInt(possibleLabelIds.length / 2);
        for (int i = 0; i < numAddedLabels; i++) {
            int labelId;
            do {
                labelId = random.among(possibleLabelIds);
            } while (initialLabels.contains(labelId) || !addedLabels.add(labelId));
        }
        IntSet removedLabels =
                IntSets.immutable.of(random.selection(initialLabels.toArray(), 0, initialLabels.size(), false));
        MutableIntObjectMap<Value> changedProperties = IntObjectMaps.mutable.empty();
        int numChangedProperties = random.nextInt(possiblePropertyKeyIds.length);
        for (int i = 0; i < numChangedProperties; i++) {
            int key;
            do {
                key = random.among(possiblePropertyKeyIds);
            } while (changedProperties.containsKey(key));

            boolean remove = changedProperties.containsKey(key) && random.nextBoolean();
            changedProperties.put(key, remove ? NO_VALUE : random.nextValue());
        }

        try (KernelTransaction tx = beginTransaction()) {
            tx.dataWrite().nodeApplyChanges(nodeId, addedLabels, removedLabels, changedProperties);
            tx.commit();
        }

        // Then
        MutableIntSet expectedLabels = IntSets.mutable.of(initialLabels.toArray());
        expectedLabels.addAll(addedLabels);
        expectedLabels.removeAll(removedLabels);
        MutableIntObjectMap<Value> expectedProperties = IntObjectMaps.mutable.ofAll(initialProperties);
        changedProperties.forEachKeyValue((key, value) -> {
            if (value == NO_VALUE) {
                expectedProperties.remove(key);
            } else {
                expectedProperties.put(key, value);
            }
        });
        try (KernelTransaction tx = beginTransaction()) {
            // A bit cheesy cast to get the cursor factory
            CursorFactory cursorFactory = cursorFactory(tx);
            try (NodeCursor nodeCursor = cursorFactory.allocateNodeCursor(CursorContext.NULL_CONTEXT);
                    PropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor(
                            CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                tx.dataRead().singleNode(nodeId, nodeCursor);
                assertThat(nodeCursor.next()).isTrue();
                assertLabels(nodeCursor, expectedLabels);
                assertProperties(nodeCursor, propertyCursor, expectedProperties);
            }
        }
    }

    @Test
    void nodeApplyChangesShouldCheckUniquenessAfterAllChanges() throws Exception {
        // Given
        Label label = Label.label("Label");
        String key1Name = "key1";
        String key2Name = "key2";
        try (Transaction tx = graphDb.beginTx()) {
            tx.schema().constraintFor(label).assertPropertyIsUnique(key1Name).assertPropertyIsUnique(key2Name);
            tx.commit();
        }
        long node;
        try (Transaction tx = graphDb.beginTx()) {
            Node n1 = tx.createNode(label);
            n1.setProperty(key1Name, "A");
            n1.setProperty(key2Name, "B");
            node = n1.getId();
            Node n2 = tx.createNode(label);
            n2.setProperty(key1Name, "A");
            n2.setProperty(key2Name, "C");
            tx.commit();
        }

        // When
        int key1;
        int key2;
        try (KernelTransaction tx = beginTransaction()) {
            key1 = tx.tokenRead().propertyKey(key1Name);
            key2 = tx.tokenRead().propertyKey(key2Name);
            MutableIntObjectMap<Value> propertyChanges = IntObjectMaps.mutable.empty();
            propertyChanges.put(key1, stringValue("D"));
            propertyChanges.put(key2, stringValue("C"));
            tx.dataWrite()
                    .nodeApplyChanges(node, IntSets.immutable.empty(), IntSets.immutable.empty(), propertyChanges);
            tx.commit();
        }

        // Then
        try (Transaction tx = graphDb.beginTx()) {
            try (ResourceIterator<Node> nodes = tx.findNodes(label, map(key1Name, "D", key2Name, "C"))) {
                assertThat(nodes.hasNext()).isTrue();
                assertThat(nodes.next().getId()).isEqualTo(node);
                assertThat(nodes.hasNext()).isFalse();
            }
        }
    }

    @Test
    void nodeApplyChangesShouldEnforceUniquenessCorrectly() throws Exception {
        // Given
        long[] nodes = new long[10];
        Label[] labels = new Label[4];
        String[] keys = new String[labels.length];
        for (int i = 0; i < labels.length; i++) {
            labels[i] = Label.label("Label" + i);
            keys[i] = "key" + i;
        }
        try (Transaction tx = graphDb.beginTx()) {
            for (int i = 0; i < nodes.length; i++) {
                Node node = tx.createNode();
                nodes[i] = node.getId();
            }
            tx.commit();
        }

        List<Label> constraintLabels = new ArrayList<>();
        List<String[]> constraintPropertyKeys = new ArrayList<>();
        try (Transaction tx = graphDb.beginTx()) {
            // Create one single-key constraint
            {
                Label constraintLabel = random.among(labels);
                String constraintKey = random.among(keys);
                tx.schema()
                        .constraintFor(constraintLabel)
                        .assertPropertyIsUnique(constraintKey)
                        .create();
                constraintLabels.add(constraintLabel);
                constraintPropertyKeys.add(new String[] {constraintKey});
            }
            // Create one double-key constraint
            {
                Label constraintLabel = random.among(labels);
                String[] constraintKeys = random.selection(keys, 2, 2, false);
                tx.schema()
                        .constraintFor(constraintLabel)
                        .assertPropertyIsUnique(constraintKeys[0])
                        .assertPropertyIsUnique(constraintKeys[1])
                        .create();
                constraintLabels.add(constraintLabel);
                constraintPropertyKeys.add(constraintKeys);
            }
            tx.commit();
        }
        int[] labelIds = new int[labels.length];
        int[] keyIds = new int[keys.length];
        try (KernelTransaction tx = beginTransaction()) {
            for (int i = 0; i < labels.length; i++) {
                labelIds[i] = tx.tokenWrite().labelGetOrCreateForName(labels[i].name());
            }
            for (int i = 0; i < keys.length; i++) {
                keyIds[i] = tx.tokenWrite().propertyKeyGetOrCreateForName(keys[i]);
            }
        }

        // Race
        Race race = new Race().withEndCondition(() -> false);
        race.addContestants(
                1,
                throwing(() -> {
                    try (KernelTransaction tx = beginTransaction()) {
                        long node = random.among(nodes);
                        MutableIntSet addedLabels = IntSets.mutable.of(random.selection(labelIds, 0, 2, false));
                        MutableIntSet removedLabels = IntSets.mutable.of(random.selection(labelIds, 0, 2, false));
                        removedLabels.removeAll(addedLabels);
                        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
                        for (int key : random.selection(keyIds, 0, 2, false)) {
                            Value value = random.nextFloat() < 0.2 ? NO_VALUE : intValue(random.nextInt(5));
                            properties.put(key, value);
                        }
                        tx.dataWrite().nodeApplyChanges(node, addedLabels, removedLabels, properties);
                        tx.commit();
                    } catch (UniquePropertyValueValidationException e) {
                        // This is OK and somewhat expected to happen for some of the operations, it's what we test here
                    }
                }),
                100);
        race.goUnchecked();

        // Then, check so that all data really conforms to the uniqueness constraints
        try (Transaction tx = graphDb.beginTx()) {
            for (int i = 0; i < constraintLabels.size(); i++) {
                Label label = constraintLabels.get(i);
                String[] propertyKeys = constraintPropertyKeys.get(i);
                Set<ValueTuple> entries = new HashSet<>();
                try (ResourceIterator<Node> nodesWithLabel = tx.findNodes(label)) {
                    while (nodesWithLabel.hasNext()) {
                        Node node = nodesWithLabel.next();
                        Map<String, Object> properties = node.getProperties(propertyKeys);
                        if (properties.size() == propertyKeys.length) {
                            Object[] values = new Object[propertyKeys.length];
                            for (int v = 0; v < propertyKeys.length; v++) {
                                String key = propertyKeys[v];
                                values[v] = properties.get(key);
                            }
                            assertThat(entries.add(ValueTuple.of(values))).isTrue();
                        }
                    }
                }
            }
            tx.commit();
        }
    }

    // HELPERS

    private void assertLabels(NodeCursor nodeCursor, IntSet expectedLabels) {
        MutableIntSet readLabels = IntSets.mutable.empty();
        TokenSet labels = nodeCursor.labels();
        for (int i = 0; i < labels.numberOfTokens(); i++) {
            readLabels.add(labels.token(i));
        }
        assertThat(readLabels).isEqualTo(expectedLabels);
    }

    private long createNode() {
        long node;
        try (org.neo4j.graphdb.Transaction ctx = graphDb.beginTx()) {
            node = ctx.createNode().getId();
            ctx.commit();
        }
        return node;
    }

    private void deleteNode(long node) {
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            tx.getNodeById(node).delete();
            tx.commit();
        }
    }

    private long createNodeWithLabels(String... labelNames) {
        long node;
        try (org.neo4j.graphdb.Transaction ctx = graphDb.beginTx()) {
            node = ctx.createNode(stream(labelNames).map(Label::label).toArray(Label[]::new))
                    .getId();
            ctx.commit();
        }
        return node;
    }

    private long createNodeWithProperty(String propertyKey, Object value) {
        Node node;
        try (org.neo4j.graphdb.Transaction ctx = graphDb.beginTx()) {
            node = ctx.createNode();
            node.setProperty(propertyKey, value);
            ctx.commit();
        }
        return node.getId();
    }

    private void assertNoLabels(long nodeId) {
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(nodeId).getLabels()).isEqualTo(Iterables.empty());
        }
    }

    private void assertLabels(long nodeId, String label) {
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(nodeId).getLabels()).contains(label(label));
        }
    }

    private void assertNoProperty(long node, String propertyKey) {
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertFalse(tx.getNodeById(node).hasProperty(propertyKey));
        }
    }

    private void assertProperty(long node, String propertyKey, Object value) {
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(node).getProperty(propertyKey)).isEqualTo(value);
        }
    }
}
