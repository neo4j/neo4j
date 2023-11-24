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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
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

@SuppressWarnings("Duplicates")
@ExtendWith(RandomExtension.class)
public abstract class RelationshipWriteTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G> {
    protected static final RelationshipType TYPE = RelationshipType.withName("R");

    @Inject
    private RandomSupport random;

    @Test
    void shouldCreateRelationship() throws Exception {
        long n1, n2;
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            n1 = tx.createNode().getId();
            n2 = tx.createNode().getId();
            tx.commit();
        }

        long r;
        try (KernelTransaction tx = beginTransaction()) {
            int label = tx.token().relationshipTypeGetOrCreateForName("R");
            r = tx.dataWrite().relationshipCreate(n1, label, n2);
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            List<Relationship> relationships =
                    Iterables.asList(tx.getNodeById(n1).getRelationships());
            assertEquals(1, relationships.size());
            assertEquals(relationships.get(0).getId(), r);
        }
    }

    @Test
    void shouldCreateRelationshipBetweenInTransactionNodes() throws Exception {
        long n1, n2, r;
        try (KernelTransaction tx = beginTransaction()) {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            int label = tx.token().relationshipTypeGetOrCreateForName("R");
            r = tx.dataWrite().relationshipCreate(n1, label, n2);
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            List<Relationship> relationships =
                    Iterables.asList(tx.getNodeById(n1).getRelationships());
            assertEquals(1, relationships.size());
            assertEquals(relationships.get(0).getId(), r);
        }
    }

    @Test
    void shouldRollbackRelationshipOnFailure() throws Exception {
        long n1, n2;
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            n1 = tx.createNode().getId();
            n2 = tx.createNode().getId();
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            int label = tx.token().relationshipTypeGetOrCreateForName("R");
            tx.dataWrite().relationshipCreate(n1, label, n2);
            tx.rollback();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertEquals(0, tx.getNodeById(n1).getDegree());
        }
    }

    @Test
    void shouldDeleteRelationship() throws Exception {
        long n1, r;
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();

            n1 = node1.getId();
            r = node1.createRelationshipTo(node2, TYPE).getId();

            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            assertTrue(tx.dataWrite().relationshipDelete(r), "should delete relationship");
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertEquals(0, tx.getNodeById(n1).getDegree());
        }
    }

    @Test
    void shouldNotDeleteRelationshipThatDoesNotExist() throws Exception {
        long relationship = 0;

        try (KernelTransaction tx = beginTransaction()) {
            assertFalse(tx.dataWrite().relationshipDelete(relationship));
            tx.rollback();
        }
        try (KernelTransaction tx = beginTransaction()) {
            assertFalse(tx.dataWrite().relationshipDelete(relationship));
            tx.commit();
        }
        // should not crash
    }

    @Test
    void shouldDeleteRelationshipAddedInTransaction() throws Exception {
        long n1, n2;
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            n1 = tx.createNode().getId();
            n2 = tx.createNode().getId();
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            int label = tx.token().relationshipTypeGetOrCreateForName("R");
            long r = tx.dataWrite().relationshipCreate(n1, label, n2);

            assertTrue(tx.dataWrite().relationshipDelete(r));
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertEquals(0, tx.getNodeById(n1).getDegree());
        }
    }

    @Test
    void shouldAddPropertyToRelationship() throws Exception {
        // Given
        long relationshipId;
        String propertyKey = "prop";
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();

            relationshipId = node1.createRelationshipTo(node2, TYPE).getId();

            tx.commit();
        }

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().relationshipSetProperty(relationshipId, token, stringValue("hello")))
                    .isEqualTo(NO_VALUE);
            tx.commit();
        }

        // Then
        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            assertThat(transaction.getRelationshipById(relationshipId).getProperty("prop"))
                    .isEqualTo("hello");
        }
    }

    @Test
    void shouldUpdatePropertyToRelationship() throws Exception {
        // Given
        long relationshipId;
        String propertyKey = "prop";
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();

            Relationship r = node1.createRelationshipTo(node2, TYPE);
            r.setProperty(propertyKey, 42);
            relationshipId = r.getId();

            tx.commit();
        }

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().relationshipSetProperty(relationshipId, token, stringValue("hello")))
                    .isEqualTo(intValue(42));
            tx.commit();
        }

        // Then
        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            assertThat(transaction.getRelationshipById(relationshipId).getProperty("prop"))
                    .isEqualTo("hello");
        }
    }

    @Test
    void shouldRemovePropertyFromRelationship() throws Exception {
        // Given
        long relationshipId;
        String propertyKey = "prop";
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();

            Relationship proxy = node1.createRelationshipTo(node2, TYPE);
            relationshipId = proxy.getId();
            proxy.setProperty(propertyKey, 42);
            tx.commit();
        }

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().relationshipRemoveProperty(relationshipId, token))
                    .isEqualTo(intValue(42));
            tx.commit();
        }

        // Then
        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            assertFalse(transaction.getRelationshipById(relationshipId).hasProperty("prop"));
        }
    }

    @Test
    void shouldRemoveNonExistingPropertyFromRelationship() throws Exception {
        // Given
        long relationshipId;
        String propertyKey = "prop";
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();

            Relationship proxy = node1.createRelationshipTo(node2, TYPE);
            relationshipId = proxy.getId();
            tx.commit();
        }
        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().relationshipRemoveProperty(relationshipId, token))
                    .isEqualTo(NO_VALUE);
            tx.commit();
        }

        // Then
        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            assertFalse(transaction.getRelationshipById(relationshipId).hasProperty("prop"));
        }
    }

    @Test
    void shouldRemovePropertyFromRelationshipTwice() throws Exception {
        // Given
        long relationshipId;
        String propertyKey = "prop";
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();

            Relationship proxy = node1.createRelationshipTo(node2, TYPE);
            relationshipId = proxy.getId();
            proxy.setProperty(propertyKey, 42);
            tx.commit();
        }

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().relationshipRemoveProperty(relationshipId, token))
                    .isEqualTo(intValue(42));
            assertThat(tx.dataWrite().relationshipRemoveProperty(relationshipId, token))
                    .isEqualTo(NO_VALUE);
            tx.commit();
        }

        // Then
        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            assertFalse(transaction.getRelationshipById(relationshipId).hasProperty("prop"));
        }
    }

    @Test
    void shouldUpdatePropertyToRelationshipInTransaction() throws Exception {
        // Given
        long relationshipId;
        String propertyKey = "prop";
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();

            relationshipId = node1.createRelationshipTo(node2, TYPE).getId();

            tx.commit();
        }

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int token = tx.token().propertyKeyGetOrCreateForName(propertyKey);
            assertThat(tx.dataWrite().relationshipSetProperty(relationshipId, token, stringValue("hello")))
                    .isEqualTo(NO_VALUE);
            assertThat(tx.dataWrite().relationshipSetProperty(relationshipId, token, stringValue("world")))
                    .isEqualTo(stringValue("hello"));
            assertThat(tx.dataWrite().relationshipSetProperty(relationshipId, token, intValue(1337)))
                    .isEqualTo(stringValue("world"));
            tx.commit();
        }

        // Then
        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            assertThat(transaction.getRelationshipById(relationshipId).getProperty("prop"))
                    .isEqualTo(1337);
        }
    }

    @Test
    void shouldNotWriteWhenSettingPropertyToSameValue() throws Exception {
        // Given
        long relationshipId;
        String propertyKey = "prop";
        Value theValue = stringValue("The Value");

        try (org.neo4j.graphdb.Transaction ctx = graphDb.beginTx()) {
            Node node1 = ctx.createNode();
            Node node2 = ctx.createNode();

            Relationship r = node1.createRelationshipTo(node2, TYPE);

            r.setProperty(propertyKey, theValue.asObject());
            relationshipId = r.getId();
            ctx.commit();
        }

        // When
        KernelTransaction tx = beginTransaction();
        int property = tx.token().propertyKeyGetOrCreateForName(propertyKey);
        assertThat(tx.dataWrite().relationshipSetProperty(relationshipId, property, theValue))
                .isEqualTo(theValue);

        assertThat(tx.commit()).isEqualTo(KernelTransaction.READ_ONLY_ID);
    }

    @Test
    void relationshipApplyChangesShouldAddProperty() throws Exception {
        // Given
        long relationship = createRelationship(TYPE);
        String keyName = "key";
        Value value = intValue(123);

        // When
        int key;
        try (KernelTransaction tx = beginTransaction()) {
            key = tx.tokenWrite().propertyKeyGetOrCreateForName(keyName);
            tx.dataWrite().relationshipApplyChanges(relationship, IntObjectMaps.immutable.of(key, value));
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var relationshipCursor =
                            cursorFactory(ktx).allocateRelationshipScanCursor(CursorContext.NULL_CONTEXT);
                    var propertyCursor = cursorFactory(ktx)
                            .allocatePropertyCursor(CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                ktx.dataRead().singleRelationship(relationship, relationshipCursor);
                relationshipCursor.next();
                assertProperties(relationshipCursor, propertyCursor, IntObjectMaps.immutable.of(key, value));
            }
        });
    }

    @Test
    void relationshipApplyChangesShouldChangeProperty() throws Exception {
        // Given
        String keyName = "key";
        Value changedValue = stringValue("value");
        long relationship = createRelationshipWithProperty(TYPE, keyName, 123);

        // When
        int key;
        try (KernelTransaction tx = beginTransaction()) {
            key = tx.tokenRead().propertyKey(keyName);
            tx.dataWrite().relationshipApplyChanges(relationship, IntObjectMaps.immutable.of(key, changedValue));
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var relationshipCursor =
                            cursorFactory(ktx).allocateRelationshipScanCursor(CursorContext.NULL_CONTEXT);
                    var propertyCursor = cursorFactory(ktx)
                            .allocatePropertyCursor(CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                ktx.dataRead().singleRelationship(relationship, relationshipCursor);
                relationshipCursor.next();
                assertProperties(relationshipCursor, propertyCursor, IntObjectMaps.immutable.of(key, changedValue));
            }
        });
    }

    @Test
    void relationshipApplyChangesShouldRemoveProperty() throws Exception {
        // Given
        String keyName = "key";
        long relationship = createRelationshipWithProperty(TYPE, keyName, 123);

        // When
        int key;
        try (KernelTransaction tx = beginTransaction()) {
            key = tx.tokenRead().propertyKey(keyName);
            tx.dataWrite().relationshipApplyChanges(relationship, IntObjectMaps.immutable.of(key, NO_VALUE));
            tx.commit();
        }

        // Then
        transaction(ktx -> {
            try (var relationshipCursor =
                            cursorFactory(ktx).allocateRelationshipScanCursor(CursorContext.NULL_CONTEXT);
                    var propertyCursor = cursorFactory(ktx)
                            .allocatePropertyCursor(CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                ktx.dataRead().singleRelationship(relationship, relationshipCursor);
                relationshipCursor.next();
                assertProperties(relationshipCursor, propertyCursor, IntObjectMaps.immutable.empty());
            }
        });
    }

    @Test
    void relationshipApplyChangesShouldSetAndRemoveMultipleProperties() throws Exception {
        // Given
        long relationship;
        int[] keys = new int[10];
        MutableIntObjectMap<Value> initialProperties = IntObjectMaps.mutable.empty();
        try (KernelTransaction tx = beginTransaction()) {
            for (int i = 0; i < keys.length; i++) {
                keys[i] = tx.tokenWrite().propertyKeyGetOrCreateForName("key" + i);
            }
            var sourceNode = tx.dataWrite().nodeCreate();
            var targetNode = tx.dataWrite().nodeCreate();
            var type = tx.tokenWrite().relationshipTypeGetOrCreateForName(TYPE.name());
            relationship = tx.dataWrite().relationshipCreate(sourceNode, type, targetNode);
            for (int key : random.selection(keys, 0, keys.length, false)) {
                Value value = random.nextValue();
                initialProperties.put(key, value);
                tx.dataWrite().relationshipSetProperty(relationship, key, value);
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
            tx.dataWrite().relationshipApplyChanges(relationship, propertyChanges);
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
            try (var relationshipCursor =
                            cursorFactory(ktx).allocateRelationshipScanCursor(CursorContext.NULL_CONTEXT);
                    var propertyCursor = cursorFactory(ktx)
                            .allocatePropertyCursor(CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                ktx.dataRead().singleRelationship(relationship, relationshipCursor);
                assertThat(relationshipCursor.next()).isTrue();
                assertProperties(relationshipCursor, propertyCursor, expectedProperties);
            }
        });
    }

    @Test
    void relationshipApplyChangesShouldCheckUniquenessAfterAllChanges() throws Exception {
        // Given
        RelationshipType type = RelationshipType.withName("Type");
        String key1Name = "key1";
        String key2Name = "key2";
        try (Transaction tx = graphDb.beginTx()) {
            tx.schema()
                    .constraintFor(type)
                    .assertPropertyIsUnique(key1Name)
                    .assertPropertyIsUnique(key2Name)
                    .create();
            tx.commit();
        }
        long rel;
        try (Transaction tx = graphDb.beginTx()) {
            Node n1 = tx.createNode();
            Relationship r1 = n1.createRelationshipTo(n1, type);
            r1.setProperty(key1Name, "A");
            r1.setProperty(key2Name, "B");
            rel = r1.getId();
            Node n2 = tx.createNode();
            Relationship r2 = n2.createRelationshipTo(n2, type);
            r2.setProperty(key1Name, "A");
            r2.setProperty(key2Name, "C");
            tx.commit();
        }

        // When
        try (KernelTransaction tx = beginTransaction()) {
            int key1 = tx.tokenRead().propertyKey(key1Name);
            int key2 = tx.tokenRead().propertyKey(key2Name);
            MutableIntObjectMap<Value> propertyChanges = IntObjectMaps.mutable.empty();
            propertyChanges.put(key2, stringValue("C"));
            propertyChanges.put(key1, stringValue("D"));
            tx.dataWrite().relationshipApplyChanges(rel, propertyChanges);
            tx.commit();
        }

        // Then
        try (Transaction tx = graphDb.beginTx()) {
            try (ResourceIterator<Relationship> relationships =
                    tx.findRelationships(type, map(key1Name, "D", key2Name, "C"))) {
                assertThat(relationships.hasNext()).isTrue();
                assertThat(relationships.next().getId()).isEqualTo(rel);
                assertThat(relationships.hasNext()).isFalse();
            }
        }
    }

    @Test
    void relationshipApplyChangesShouldEnforceUniquenessCorrectly() throws Exception {
        // Given
        long[] rels = new long[30];
        RelationshipType[] types = new RelationshipType[4];
        String[] keys = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = RelationshipType.withName("Type" + i);
            keys[i] = "key" + i;
        }
        try (Transaction tx = graphDb.beginTx()) {
            for (int i = 0; i < rels.length; i++) {
                Node node = tx.createNode();
                Relationship rel = node.createRelationshipTo(node, random.among(types));
                rels[i] = rel.getId();
            }
            tx.commit();
        }

        List<RelationshipType> constraintTypes = new ArrayList<>();
        List<String[]> constraintPropertyKeys = new ArrayList<>();
        try (Transaction tx = graphDb.beginTx()) {
            // Create one single-key constraint
            {
                RelationshipType constraintType = random.among(types);
                String constraintKey = random.among(keys);
                tx.schema()
                        .constraintFor(constraintType)
                        .assertPropertyIsUnique(constraintKey)
                        .create();
                constraintTypes.add(constraintType);
                constraintPropertyKeys.add(new String[] {constraintKey});
            }
            // Create one double-key constraint
            {
                RelationshipType constraintType = random.among(types);
                String[] constraintKeys = random.selection(keys, 2, 2, false);
                tx.schema()
                        .constraintFor(constraintType)
                        .assertPropertyIsUnique(constraintKeys[0])
                        .assertPropertyIsUnique(constraintKeys[1])
                        .create();
                constraintTypes.add(constraintType);
                constraintPropertyKeys.add(constraintKeys);
            }
            tx.commit();
        }
        int[] keyIds = new int[keys.length];
        try (KernelTransaction tx = beginTransaction()) {
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
                        long rel = random.among(rels);
                        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
                        for (int key : random.selection(keyIds, 1, 4, false)) {
                            Value value = random.nextFloat() < 0.2 ? NO_VALUE : intValue(random.nextInt(5));
                            properties.put(key, value);
                        }
                        tx.dataWrite().relationshipApplyChanges(rel, properties);
                        tx.commit();
                    } catch (UniquePropertyValueValidationException e) {
                        // This is OK and somewhat expected to happen for some of the operations, it's what we test here
                    }
                }),
                150);
        race.goUnchecked();

        // Then, check so that all data really conforms to the uniqueness constraints
        try (Transaction tx = graphDb.beginTx()) {
            for (int i = 0; i < constraintTypes.size(); i++) {
                RelationshipType type = constraintTypes.get(i);
                String[] propertyKeys = constraintPropertyKeys.get(i);
                Set<ValueTuple> entries = new HashSet<>();
                try (ResourceIterator<Relationship> relsWithType = tx.findRelationships(type)) {
                    while (relsWithType.hasNext()) {
                        Relationship rel = relsWithType.next();
                        Map<String, Object> properties = rel.getProperties(propertyKeys);

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

    private long createRelationship(RelationshipType type) {
        try (var ctx = graphDb.beginTx()) {
            var relationship = ctx.createNode()
                    .createRelationshipTo(ctx.createNode(), type)
                    .getId();
            ctx.commit();
            return relationship;
        }
    }

    private long createRelationshipWithProperty(RelationshipType type, String key, Object value) {
        try (var ctx = graphDb.beginTx()) {
            var relationship = ctx.createNode().createRelationshipTo(ctx.createNode(), type);
            relationship.setProperty(key, value);
            ctx.commit();
            return relationship.getId();
        }
    }
}
