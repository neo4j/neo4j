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
package org.neo4j.index;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.count;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IndexTxStateLookupTest {
    private static final String TRIGGER_LAZY = "this is supposed to be a really long property to trigger lazy loading";
    private static final Random random = new Random();

    protected static Stream<Arguments> argumentsProvider() {
        List<Arguments> parameters = new ArrayList<>();
        parameters.addAll(asList(
                Arguments.of(new String("name"), new String("name")),
                Arguments.of(7, 7L),
                Arguments.of(9L, 9),
                Arguments.of(2, 2.0),
                Arguments.of(3L, 3.0),
                Arguments.of(4, 4.0f),
                Arguments.of(5L, 5.0f),
                Arguments.of(12.0, 12),
                Arguments.of(13.0, 13L),
                Arguments.of(14.0f, 14),
                Arguments.of(15.0f, 15L),
                Arguments.of(2.5f, 2.5),
                Arguments.of(16.25, 16.25f),
                Arguments.of(stringArray("a", "b", "c"), charArray('a', 'b', 'c')),
                Arguments.of(charArray('d', 'e', 'f'), stringArray("d", "e", "f")),
                Arguments.of(splitStrings(TRIGGER_LAZY), splitChars(TRIGGER_LAZY)),
                Arguments.of(splitChars(TRIGGER_LAZY), splitStrings(TRIGGER_LAZY)),
                Arguments.of(stringArray("foo", "bar"), stringArray("foo", "bar"))));
        Class[] numberTypes = {byte.class, short.class, int.class, long.class, float.class, double.class};
        for (Class lhs : numberTypes) {
            for (Class rhs : numberTypes) {
                parameters.add(randomNumbers(3, lhs, rhs));
                parameters.add(randomNumbers(200, lhs, rhs));
            }
        }
        return parameters.stream();
    }

    private static class NamedObject {
        private final Object object;
        private final String name;

        NamedObject(Object object, String name) {
            this.object = object;
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static NamedObject stringArray(String... items) {
        return new NamedObject(items, arrayToString(items));
    }

    private static NamedObject charArray(char... items) {
        return new NamedObject(items, arrayToString(items));
    }

    private static Arguments randomNumbers(int length, Class<?> lhsType, Class<?> rhsType) {
        Object lhs = Array.newInstance(lhsType, length);
        Object rhs = Array.newInstance(rhsType, length);
        for (int i = 0; i < length; i++) {
            int value = random.nextInt(128);
            Array.set(lhs, i, convert(value, lhsType));
            Array.set(rhs, i, convert(value, rhsType));
        }
        return Arguments.of(new NamedObject(lhs, arrayToString(lhs)), new NamedObject(rhs, arrayToString(rhs)));
    }

    private static String arrayToString(Object arrayObject) {
        int length = Array.getLength(arrayObject);
        String type = arrayObject.getClass().getComponentType().getSimpleName();
        StringBuilder builder = new StringBuilder().append('(').append(type).append(") {");
        for (int i = 0; i < length; i++) {
            builder.append(i > 0 ? "," : "").append(Array.get(arrayObject, i));
        }
        return builder.append('}').toString();
    }

    private static Object convert(int value, Class<?> type) {
        switch (type.getName()) {
            case "byte":
                return (byte) value;
            case "short":
                return (short) value;
            case "int":
                return value;
            case "long":
                return (long) value;
            case "float":
                return (float) value;
            case "double":
                return (double) value;
            default:
                return value;
        }
    }

    private static NamedObject splitStrings(String string) {
        char[] chars = internalSplitChars(string);
        String[] result = new String[chars.length];
        for (int i = 0; i < chars.length; i++) {
            result[i] = Character.toString(chars[i]);
        }
        return stringArray(result);
    }

    private static char[] internalSplitChars(String string) {
        char[] result = new char[string.length()];
        string.getChars(0, result.length, result, 0);
        return result;
    }

    private static NamedObject splitChars(String string) {
        char[] result = internalSplitChars(string);
        return charArray(result);
    }

    @Inject
    private GraphDatabaseAPI db;

    private Object store;
    private Object lookup;

    public void init(Object store, Object lookup) {
        this.store = realValue(store);
        this.lookup = realValue(lookup);
    }

    private static Object realValue(Object object) {
        return object instanceof NamedObject ? ((NamedObject) object).object : object;
    }

    @BeforeAll
    public void given() {
        // database with an index on `(:Node).prop`
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(label("Node")).on("prop").create();
            tx.schema().indexFor(RelationshipType.withName("Rel")).on("prop").create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(30, SECONDS);
            tx.commit();
        }
    }

    @ParameterizedTest(name = "store=<{0}> lookup=<{1}>")
    @MethodSource("argumentsProvider")
    public void lookupWithinTransaction(Object store, Object lookup) {
        init(store, lookup);

        try (Transaction tx = db.beginTx()) {
            // when
            tx.createNode(label("Node")).setProperty("prop", this.store);

            // then
            assertEquals(1, count(tx.findNodes(label("Node"), "prop", this.lookup)));

            // no need to actually commit this node
        }
    }

    @ParameterizedTest(name = "store=<{0}> lookup=<{1}>")
    @MethodSource("argumentsProvider")
    public void lookupWithoutTransaction(Object store, Object lookup) {
        init(store, lookup);

        // when
        Node node;
        try (Transaction tx = db.beginTx()) {
            (node = tx.createNode(label("Node"))).setProperty("prop", this.store);
            tx.commit();
        }
        // then
        try (Transaction tx = db.beginTx()) {
            assertEquals(1, count(tx.findNodes(label("Node"), "prop", this.lookup)));
            tx.commit();
        }
        deleteNode(node);
    }

    private void deleteNode(Node node) {
        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(node.getId()).delete();
            tx.commit();
        }
    }

    @ParameterizedTest(name = "store=<{0}> lookup=<{1}>")
    @MethodSource("argumentsProvider")
    public void lookupWithoutTransactionWithCacheEviction(Object store, Object lookup) {
        init(store, lookup);

        // when
        Node node;
        try (Transaction tx = db.beginTx()) {
            (node = tx.createNode(label("Node"))).setProperty("prop", this.store);
            tx.commit();
        }
        // then
        try (Transaction tx = db.beginTx()) {
            assertEquals(1, count(tx.findNodes(label("Node"), "prop", this.lookup)));
            tx.commit();
        }
        deleteNode(node);
    }

    @ParameterizedTest(name = "store=<{0}> lookup=<{1}>")
    @MethodSource("argumentsProvider")
    void shouldRemoveDeletedNodeCreatedInSameTransactionFromIndexTxState(Object store, Object lookup) {
        init(store, lookup);

        try (Transaction tx = db.beginTx()) {
            // given
            Label label = label("Node");
            String key = "prop";
            Node node = tx.createNode(label);
            node.setProperty(key, this.store);
            assertTrue(exists(tx.findNodes(label, key, this.lookup)));

            // when
            node.delete();

            // then
            assertFalse(exists(tx.findNodes(label, key, this.lookup)));
        }
    }

    @ParameterizedTest(name = "store=<{0}> lookup=<{1}>")
    @MethodSource("argumentsProvider")
    void shouldRemoveDeletedRelationshipCreatedInSameTransactionFromIndexTxState(Object store, Object lookup) {
        init(store, lookup);

        try (Transaction tx = db.beginTx()) {
            // given
            RelationshipType type = RelationshipType.withName("Rel");
            String key = "prop";
            Relationship relationship = tx.createNode().createRelationshipTo(tx.createNode(), type);
            relationship.setProperty(key, this.store);
            assertTrue(exists(tx.findRelationships(type, key, this.lookup)));

            // when
            relationship.delete();

            // then
            assertFalse(exists(tx.findRelationships(type, key, this.lookup)));
        }
    }

    @ParameterizedTest(name = "store=<{0}> lookup=<{1}>")
    @MethodSource("argumentsProvider")
    void shouldRemoveDeletedNodeCreatedInSameTransactionFromIndexTxStateEvenWithMultipleProperties(
            Object store, Object lookup) {
        init(store, lookup);

        try (Transaction tx = db.beginTx()) {
            // given
            Label label = label("Node");
            String key = "prop";
            String key2 = "prop2";
            Node node = tx.createNode(label);
            node.setProperty(key, this.store);
            node.setProperty(key2, this.store);
            assertTrue(exists(tx.findNodes(label, key, this.lookup)));

            // when
            node.delete();

            // then
            assertFalse(exists(tx.findNodes(label, key, this.lookup)));
        }
    }

    @ParameterizedTest(name = "store=<{0}> lookup=<{1}>")
    @MethodSource("argumentsProvider")
    void shouldRemoveDeletedRelationshipCreatedInSameTransactionFromIndexTxStateEvenWithMultipleProperties(
            Object store, Object lookup) {
        init(store, lookup);

        try (Transaction tx = db.beginTx()) {
            // given
            RelationshipType type = RelationshipType.withName("Rel");
            String key = "prop";
            String key2 = "prop2";
            Relationship relationship = tx.createNode().createRelationshipTo(tx.createNode(), type);
            relationship.setProperty(key, this.store);
            relationship.setProperty(key2, this.store);
            assertTrue(exists(tx.findRelationships(type, key, this.lookup)));

            // when
            relationship.delete();

            // then
            assertFalse(exists(tx.findRelationships(type, key, this.lookup)));
        }
    }

    private static boolean exists(ResourceIterator<?> iterator) {
        try (iterator) {
            return iterator.hasNext();
        }
    }
}
