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
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;
import static org.neo4j.values.storable.Values.longValue;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

public abstract class PropertyCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G> {
    @SuppressWarnings("SpellCheckingInspection")
    private static final String LONG_STRING = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque "
            + "eget nibh cursus, efficitur risus non, ultrices justo. Nulla laoreet eros mi, non molestie magna "
            + "luctus in. Fusce nibh neque, tristique ultrices laoreet et, aliquet non dolor. Donec ultrices nisi "
            + "eget urna luctus volutpat. Vivamus hendrerit eget justo vel scelerisque. Morbi interdum volutpat diam,"
            + " et cursus arcu efficitur consectetur. Cras vitae facilisis ipsum, vitae ullamcorper orci. Nullam "
            + "tristique ante sed nibh consequat posuere. Curabitur mauris nisl, condimentum ac varius vel, imperdiet"
            + " a neque. Sed euismod condimentum nisl, vel efficitur turpis tempus id.\n"
            + "\n"
            + "Sed in tempor arcu. Suspendisse molestie rutrum risus a dignissim. Donec et orci non diam tincidunt "
            + "sollicitudin non id nisi. Aliquam vehicula imperdiet viverra. Cras et lacinia eros. Etiam imperdiet ac"
            + " dolor ut tristique. Phasellus ut lacinia ex. Pellentesque habitant morbi tristique senectus et netus "
            + "et malesuada fames ac turpis egestas. Integer libero justo, tincidunt ut felis non, interdum "
            + "consectetur mauris. Cras eu felis ante. Sed dapibus nulla urna, at elementum tortor ultricies pretium."
            + " Maecenas sed augue non urna consectetur fringilla vitae eu libero. Vivamus interdum bibendum risus, "
            + "quis luctus eros.\n"
            + "\n"
            + "Sed neque augue, fermentum sit amet iaculis ut, porttitor ac odio. Phasellus et sapien non sapien "
            + "consequat fermentum accumsan non dolor. Integer eget pellentesque lectus, vitae lobortis ante. Nam "
            + "elementum, dui ut finibus rutrum, purus mauris efficitur purus, efficitur tempus ante metus bibendum "
            + "velit. Curabitur commodo, risus et eleifend facilisis, eros augue posuere tortor, eu dictum erat "
            + "tortor consectetur orci. Fusce a velit dignissim, tempus libero nec, faucibus risus. Nullam pharetra "
            + "mauris sit amet volutpat facilisis. Pellentesque habitant morbi tristique senectus et netus et "
            + "malesuada fames ac turpis egestas. Praesent lacinia non felis ut lobortis.\n"
            + "\n"
            + "Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Sed eu nisi dui"
            + ". Suspendisse imperdiet lorem vel eleifend faucibus. Mauris non venenatis metus. Aenean neque magna, "
            + "rhoncus vel velit in, dictum convallis leo. Phasellus pulvinar eu sapien ac vehicula. Praesent "
            + "placerat augue quam, egestas vehicula velit porttitor in. Vivamus velit metus, pellentesque quis "
            + "fermentum et, porta quis velit. Curabitur sed lacus quis nibh convallis tincidunt.\n"
            + "\n"
            + "Etiam eu elit eget dolor dignissim lacinia. Vivamus tortor ex, dapibus id elementum non, suscipit ac "
            + "nisl. Aenean vel tempor libero, eu venenatis elit. Nunc nec velit eu odio interdum pellentesque sed et"
            + " eros. Nam quis mi in metus tristique aliquam. Nullam facilisis dapibus lacus, nec lacinia velit. "
            + "Proin massa enim, accumsan ac libero at, iaculis sodales tellus. Vivamus fringilla justo sed luctus "
            + "tincidunt. Sed placerat fringilla ex, vel placerat sem faucibus eget. Vestibulum semper dui sit amet "
            + "efficitur blandit. Donec eu tellus velit. Etiam a mi nec massa euismod posuere. Cras eget lacus leo.";

    private static final String DATE_PROP = "dateProp";
    private static final String POINT_PROP = "pointProp";
    private static final String BYTE_PROP = "byteProp";
    private static final String SHORT_PROP = "shortProp";
    private static final String INT_PROP = "intProp";
    private static final String INLINE_LONG_PROP = "inlineLongProp";
    private static final String LONG_PROP = "longProp";
    private static final String FLOAT_PROP = "floatProp";
    private static final String DOUBLE_PROP = "doubleProp";
    private static final String TRUE_PROP = "trueProp";
    private static final String FALSE_PROP = "falseProp";
    private static final String CHAR_PROP = "charProp";
    private static final String EMPTY_STRING_PROP = "emptyStringProp";
    private static final String SHORT_STRING_PROP = "shortStringProp";
    private static final String LONG_STRING_PROP = "longStringProp";
    private static final String UTF_8_PROP = "utf8Prop";
    private static final String SMALL_ARRAY_PROP = "smallArrayProp";
    private static final String BIG_ARRAY_PROP = "bigArrayProp";

    private static long bareNodeId,
            bytePropNodeId,
            shortPropNodeId,
            intPropNodeId,
            inlineLongPropNodeId,
            longPropNodeId,
            floatPropNodeId,
            doublePropNodeId,
            truePropNodeId,
            falsePropNodeId,
            charPropNodeId,
            emptyStringPropNodeId,
            shortStringPropNodeId,
            longStringPropNodeId,
            utf8PropNodeId,
            smallArrayNodeId,
            bigArrayNodeId,
            pointPropNodeId,
            datePropNodeId,
            allPropsNodeId,
            bareRelId,
            bytePropRelId,
            shortPropRelId,
            intPropRelId,
            inlineLongPropRelId,
            longPropRelId,
            floatPropRelId,
            doublePropRelId,
            truePropRelId,
            falsePropRelId,
            charPropRelId,
            emptyStringPropRelId,
            shortStringPropRelId,
            longStringPropRelId,
            utf8PropRelId,
            smallArrayRelId,
            bigArrayRelId,
            pointPropRelId,
            datePropRelId,
            allPropsRelId;

    private static final String CHINESE = "造Unicode之";
    private static final Value POINT_VALUE = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 10, 20);
    private static final Value DATE_VALUE = Values.temporalValue(LocalDate.of(2018, 7, 26));

    private static boolean supportsBigProperties() {
        return true;
    }

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            // Nodes
            bareNodeId = tx.createNode().getId();

            bytePropNodeId = createNodeWithProperty(tx, BYTE_PROP, (byte) 13);
            shortPropNodeId = createNodeWithProperty(tx, SHORT_PROP, (short) 13);
            intPropNodeId = createNodeWithProperty(tx, INT_PROP, 13);
            inlineLongPropNodeId = createNodeWithProperty(tx, INLINE_LONG_PROP, 13L);
            longPropNodeId = createNodeWithProperty(tx, LONG_PROP, Long.MAX_VALUE);

            floatPropNodeId = createNodeWithProperty(tx, FLOAT_PROP, 13.0f);
            doublePropNodeId = createNodeWithProperty(tx, DOUBLE_PROP, 13.0);

            truePropNodeId = createNodeWithProperty(tx, TRUE_PROP, true);
            falsePropNodeId = createNodeWithProperty(tx, FALSE_PROP, false);

            charPropNodeId = createNodeWithProperty(tx, CHAR_PROP, 'x');
            emptyStringPropNodeId = createNodeWithProperty(tx, EMPTY_STRING_PROP, "");
            shortStringPropNodeId = createNodeWithProperty(tx, SHORT_STRING_PROP, "hello");
            longStringPropNodeId = createNodeWithProperty(tx, LONG_STRING_PROP, LONG_STRING);
            utf8PropNodeId = createNodeWithProperty(tx, UTF_8_PROP, CHINESE);

            smallArrayNodeId = createNodeWithProperty(tx, SMALL_ARRAY_PROP, new int[] {1, 2, 3, 4});
            bigArrayNodeId = createNodeWithProperty(tx, BIG_ARRAY_PROP, new String[] {LONG_STRING});

            pointPropNodeId = createNodeWithProperty(tx, POINT_PROP, POINT_VALUE);
            datePropNodeId = createNodeWithProperty(tx, DATE_PROP, DATE_VALUE);

            Node allPropsNode = tx.createNode();
            // first property record
            allPropsNode.setProperty(BYTE_PROP, (byte) 13);
            allPropsNode.setProperty(SHORT_PROP, (short) 13);
            allPropsNode.setProperty(INT_PROP, 13);
            allPropsNode.setProperty(INLINE_LONG_PROP, 13L);
            // second property record
            allPropsNode.setProperty(LONG_PROP, Long.MAX_VALUE);
            allPropsNode.setProperty(FLOAT_PROP, 13.0f);
            allPropsNode.setProperty(DOUBLE_PROP, 13.0);
            //                  ^^^
            // third property record halfway through double?
            allPropsNode.setProperty(TRUE_PROP, true);
            allPropsNode.setProperty(FALSE_PROP, false);

            allPropsNode.setProperty(CHAR_PROP, 'x');
            allPropsNode.setProperty(EMPTY_STRING_PROP, "");
            allPropsNode.setProperty(SHORT_STRING_PROP, "hello");
            allPropsNode.setProperty(UTF_8_PROP, CHINESE);
            allPropsNode.setProperty(POINT_PROP, POINT_VALUE);
            allPropsNode.setProperty(DATE_PROP, DATE_VALUE);
            if (supportsBigProperties()) {
                allPropsNode.setProperty(LONG_STRING_PROP, LONG_STRING);
                allPropsNode.setProperty(SMALL_ARRAY_PROP, new int[] {1, 2, 3, 4});
                allPropsNode.setProperty(BIG_ARRAY_PROP, new String[] {LONG_STRING});
            }

            allPropsNodeId = allPropsNode.getId();

            // Relationships
            RelationshipType type = RelationshipType.withName("REL");
            bareRelId =
                    tx.createNode().createRelationshipTo(tx.createNode(), type).getId();

            bytePropRelId = createRelWithProperty(tx, type, BYTE_PROP, (byte) 13);
            shortPropRelId = createRelWithProperty(tx, type, SHORT_PROP, (short) 13);
            intPropRelId = createRelWithProperty(tx, type, INT_PROP, 13);
            inlineLongPropRelId = createRelWithProperty(tx, type, INLINE_LONG_PROP, 13L);
            longPropRelId = createRelWithProperty(tx, type, LONG_PROP, Long.MAX_VALUE);
            floatPropRelId = createRelWithProperty(tx, type, FLOAT_PROP, 13.0f);
            doublePropRelId = createRelWithProperty(tx, type, DOUBLE_PROP, 13.0);
            truePropRelId = createRelWithProperty(tx, type, TRUE_PROP, true);
            falsePropRelId = createRelWithProperty(tx, type, FALSE_PROP, false);
            charPropRelId = createRelWithProperty(tx, type, CHAR_PROP, 'x');
            emptyStringPropRelId = createRelWithProperty(tx, type, EMPTY_STRING_PROP, "");
            shortStringPropRelId = createRelWithProperty(tx, type, SHORT_STRING_PROP, "hello");
            longStringPropRelId = createRelWithProperty(tx, type, LONG_STRING_PROP, LONG_STRING);
            utf8PropRelId = createRelWithProperty(tx, type, UTF_8_PROP, CHINESE);
            smallArrayRelId = createRelWithProperty(tx, type, SMALL_ARRAY_PROP, new int[] {1, 2, 3, 4});
            bigArrayRelId = createRelWithProperty(tx, type, BIG_ARRAY_PROP, new String[] {LONG_STRING});
            pointPropRelId = createRelWithProperty(tx, type, POINT_PROP, POINT_VALUE);
            datePropRelId = createRelWithProperty(tx, type, DATE_PROP, DATE_VALUE);

            Relationship allPropsRel = allPropsNode.createRelationshipTo(tx.createNode(), type);
            allPropsRelId = allPropsRel.getId();

            allPropsRel.setProperty(BYTE_PROP, (byte) 13);
            allPropsRel.setProperty(SHORT_PROP, (short) 13);
            allPropsRel.setProperty(INT_PROP, 13);
            allPropsRel.setProperty(INLINE_LONG_PROP, 13L);
            allPropsRel.setProperty(LONG_PROP, Long.MAX_VALUE);
            allPropsRel.setProperty(FLOAT_PROP, 13.0f);
            allPropsRel.setProperty(DOUBLE_PROP, 13.0);
            allPropsRel.setProperty(TRUE_PROP, true);
            allPropsRel.setProperty(FALSE_PROP, false);
            allPropsRel.setProperty(CHAR_PROP, 'x');
            allPropsRel.setProperty(EMPTY_STRING_PROP, "");
            allPropsRel.setProperty(SHORT_STRING_PROP, "hello");
            allPropsRel.setProperty(UTF_8_PROP, CHINESE);
            allPropsRel.setProperty(POINT_PROP, POINT_VALUE);
            allPropsRel.setProperty(DATE_PROP, DATE_VALUE);
            if (supportsBigProperties()) {
                allPropsRel.setProperty(LONG_STRING_PROP, LONG_STRING);
                allPropsRel.setProperty(SMALL_ARRAY_PROP, new int[] {1, 2, 3, 4});
                allPropsRel.setProperty(BIG_ARRAY_PROP, new String[] {LONG_STRING});
            }
            tx.commit();
        }
    }

    private static long createNodeWithProperty(Transaction tx, String propertyKey, Object value) {
        Node p = tx.createNode();
        p.setProperty(propertyKey, value);
        return p.getId();
    }

    private static long createRelWithProperty(Transaction tx, RelationshipType type, String propertyKey, Object value) {
        Relationship r = tx.createNode().createRelationshipTo(tx.createNode(), type);
        r.setProperty(propertyKey, value);
        return r.getId();
    }

    @Test
    void shouldNotAccessNonExistentNodeProperties() {
        // given
        try (NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
                PropertyCursor props = cursors.allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
            // when
            read.singleNode(bareNodeId, node);
            assertTrue(node.next(), "node by reference");
            assertFalse(hasProperties(node, props), "no properties");

            node.properties(props);
            assertFalse(props.next(), "no properties by direct method");

            read.nodeProperties(node.nodeReference(), node.propertiesReference(), ALL_PROPERTIES, props);
            assertFalse(props.next(), "no properties via property ref");

            assertFalse(node.next(), "only one node");
        }
    }

    @Test
    void shouldNotAccessNonExistentRelationshipProperties() {
        try (RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor(NULL_CONTEXT);
                PropertyCursor props = cursors.allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
            // when
            read.singleRelationship(bareRelId, relationship);
            assertTrue(relationship.next(), "relationship by reference");
            assertFalse(hasProperties(relationship, props), "no properties");

            relationship.properties(props);
            assertFalse(props.next(), "no properties by direct method");

            read.relationshipProperties(
                    relationship.relationshipReference(), relationship.propertiesReference(), ALL_PROPERTIES, props);
            assertFalse(props.next(), "no properties via property ref");

            assertFalse(relationship.next(), "only one node");
        }
    }

    @Test
    void shouldAccessSingleNodeProperty() {
        assertAccessSingleNodeProperty(bytePropNodeId, Values.of((byte) 13), ValueGroup.NUMBER);
        assertAccessSingleNodeProperty(shortPropNodeId, Values.of((short) 13), ValueGroup.NUMBER);
        assertAccessSingleNodeProperty(intPropNodeId, Values.of(13), ValueGroup.NUMBER);
        assertAccessSingleNodeProperty(inlineLongPropNodeId, Values.of(13L), ValueGroup.NUMBER);
        assertAccessSingleNodeProperty(longPropNodeId, Values.of(Long.MAX_VALUE), ValueGroup.NUMBER);
        assertAccessSingleNodeProperty(floatPropNodeId, Values.of(13.0f), ValueGroup.NUMBER);
        assertAccessSingleNodeProperty(doublePropNodeId, Values.of(13.0), ValueGroup.NUMBER);
        assertAccessSingleNodeProperty(truePropNodeId, Values.of(true), ValueGroup.BOOLEAN);
        assertAccessSingleNodeProperty(falsePropNodeId, Values.of(false), ValueGroup.BOOLEAN);
        assertAccessSingleNodeProperty(charPropNodeId, Values.of('x'), ValueGroup.TEXT);
        assertAccessSingleNodeProperty(emptyStringPropNodeId, Values.of(""), ValueGroup.TEXT);
        assertAccessSingleNodeProperty(shortStringPropNodeId, Values.of("hello"), ValueGroup.TEXT);
        if (supportsBigProperties()) {
            assertAccessSingleNodeProperty(longStringPropNodeId, Values.of(LONG_STRING), ValueGroup.TEXT);
        }
        assertAccessSingleNodeProperty(utf8PropNodeId, Values.of(CHINESE), ValueGroup.TEXT);
        if (supportsBigProperties()) {
            assertAccessSingleNodeProperty(
                    smallArrayNodeId, Values.of(new int[] {1, 2, 3, 4}), ValueGroup.NUMBER_ARRAY);
            assertAccessSingleNodeProperty(
                    bigArrayNodeId, Values.of(new String[] {LONG_STRING}), ValueGroup.TEXT_ARRAY);
        }
        assertAccessSingleNodeProperty(pointPropNodeId, Values.of(POINT_VALUE), ValueGroup.GEOMETRY);
        assertAccessSingleNodeProperty(datePropNodeId, Values.of(DATE_VALUE), ValueGroup.DATE);
    }

    @Test
    void shouldAssertSingleRelationshipProperty() {
        assertAccessSingleRelationshipProperty(bytePropRelId, Values.of((byte) 13), ValueGroup.NUMBER);
        assertAccessSingleRelationshipProperty(shortPropRelId, Values.of((short) 13), ValueGroup.NUMBER);
        assertAccessSingleRelationshipProperty(intPropRelId, Values.of(13), ValueGroup.NUMBER);
        assertAccessSingleRelationshipProperty(inlineLongPropRelId, Values.of(13L), ValueGroup.NUMBER);
        assertAccessSingleRelationshipProperty(longPropRelId, Values.of(Long.MAX_VALUE), ValueGroup.NUMBER);
        assertAccessSingleRelationshipProperty(floatPropRelId, Values.of(13.0f), ValueGroup.NUMBER);
        assertAccessSingleRelationshipProperty(doublePropRelId, Values.of(13.0), ValueGroup.NUMBER);
        assertAccessSingleRelationshipProperty(truePropRelId, Values.of(true), ValueGroup.BOOLEAN);
        assertAccessSingleRelationshipProperty(falsePropRelId, Values.of(false), ValueGroup.BOOLEAN);
        assertAccessSingleRelationshipProperty(charPropRelId, Values.of('x'), ValueGroup.TEXT);
        assertAccessSingleRelationshipProperty(emptyStringPropRelId, Values.of(""), ValueGroup.TEXT);
        assertAccessSingleRelationshipProperty(shortStringPropRelId, Values.of("hello"), ValueGroup.TEXT);
        if (supportsBigProperties()) {
            assertAccessSingleRelationshipProperty(longStringPropRelId, Values.of(LONG_STRING), ValueGroup.TEXT);
        }
        assertAccessSingleRelationshipProperty(utf8PropRelId, Values.of(CHINESE), ValueGroup.TEXT);
        if (supportsBigProperties()) {
            assertAccessSingleRelationshipProperty(
                    smallArrayRelId, Values.of(new int[] {1, 2, 3, 4}), ValueGroup.NUMBER_ARRAY);
            assertAccessSingleRelationshipProperty(
                    bigArrayRelId, Values.of(new String[] {LONG_STRING}), ValueGroup.TEXT_ARRAY);
        }
        assertAccessSingleRelationshipProperty(pointPropRelId, Values.of(POINT_VALUE), ValueGroup.GEOMETRY);
        assertAccessSingleRelationshipProperty(datePropRelId, Values.of(DATE_VALUE), ValueGroup.DATE);
    }

    @Test
    void shouldAccessAllNodeProperties() {
        // given
        try (NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
                PropertyCursor props = cursors.allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
            // when
            read.singleNode(allPropsNodeId, node);
            assertTrue(node.next(), "node by reference");
            assertTrue(hasProperties(node, props), "has properties");

            node.properties(props);
            Set<Object> values = new HashSet<>();
            while (props.next()) {
                values.add(props.propertyValue().asObject());
            }

            assertTrue(values.contains((byte) 13), BYTE_PROP);
            assertTrue(values.contains((short) 13), SHORT_PROP);
            assertTrue(values.contains(13), INT_PROP);
            assertTrue(values.contains(13L), INLINE_LONG_PROP);
            assertTrue(values.contains(Long.MAX_VALUE), LONG_PROP);
            assertTrue(values.contains(13.0f), FLOAT_PROP);
            assertTrue(values.contains(13.0), DOUBLE_PROP);
            assertTrue(values.contains(true), TRUE_PROP);
            assertTrue(values.contains(false), FALSE_PROP);
            assertTrue(values.contains('x'), CHAR_PROP);
            assertTrue(values.contains(""), EMPTY_STRING_PROP);
            assertTrue(values.contains("hello"), SHORT_STRING_PROP);
            assertTrue(values.contains(CHINESE), UTF_8_PROP);
            if (supportsBigProperties()) {
                assertTrue(values.contains(LONG_STRING), LONG_STRING_PROP);
                assertThat(values).as(SMALL_ARRAY_PROP).contains(new int[] {1, 2, 3, 4});
                assertThat(values).as(BIG_ARRAY_PROP).contains(LONG_STRING);
            }
            assertTrue(values.contains(POINT_VALUE), POINT_PROP);
            assertTrue(values.contains(DATE_VALUE.asObject()), DATE_PROP);

            int expected = supportsBigProperties() ? 18 : 15;
            assertEquals(expected, values.size(), "number of values");
        }
    }

    @Test
    void shouldAccessAllRelationshipProperties() {
        // given
        try (RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor(NULL_CONTEXT);
                PropertyCursor props = cursors.allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
            // when
            read.singleRelationship(allPropsRelId, relationship);
            assertTrue(relationship.next(), "relationship by reference");
            assertTrue(hasProperties(relationship, props), "has properties");

            relationship.properties(props);
            Set<Object> values = new HashSet<>();
            while (props.next()) {
                values.add(props.propertyValue().asObject());
            }

            assertTrue(values.contains((byte) 13), BYTE_PROP);
            assertTrue(values.contains((short) 13), SHORT_PROP);
            assertTrue(values.contains(13), INT_PROP);
            assertTrue(values.contains(13L), INLINE_LONG_PROP);
            assertTrue(values.contains(Long.MAX_VALUE), LONG_PROP);
            assertTrue(values.contains(13.0f), FLOAT_PROP);
            assertTrue(values.contains(13.0), DOUBLE_PROP);
            assertTrue(values.contains(true), TRUE_PROP);
            assertTrue(values.contains(false), FALSE_PROP);
            assertTrue(values.contains('x'), CHAR_PROP);
            assertTrue(values.contains(""), EMPTY_STRING_PROP);
            assertTrue(values.contains("hello"), SHORT_STRING_PROP);
            assertTrue(values.contains(CHINESE), UTF_8_PROP);
            if (supportsBigProperties()) {
                assertTrue(values.contains(LONG_STRING), LONG_STRING_PROP);
                assertThat(values).as(SMALL_ARRAY_PROP).contains(new int[] {1, 2, 3, 4});
                assertThat(values).as(BIG_ARRAY_PROP).contains(LONG_STRING);
            }
            assertTrue(values.contains(POINT_VALUE), POINT_PROP);
            assertTrue(values.contains(DATE_VALUE.asObject()), DATE_PROP);

            int expected = supportsBigProperties() ? 18 : 15;
            assertEquals(expected, values.size(), "number of values");
        }
    }

    @Test
    void supportExcludingWithChangesInTxState() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            var newToken = tx.tokenWrite().propertyKeyGetOrCreateForName("FAIRLY_RANDOM" + System.nanoTime());
            tx.dataWrite().nodeSetProperty(allPropsNodeId, newToken, longValue(42));
            var read = tx.dataRead();
            try (var nodes = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    var props = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                read.singleNode(allPropsNodeId, nodes);
                while (nodes.next()) {
                    nodes.properties(props, PropertySelection.ALL_PROPERTIES.excluding(i -> i != newToken));
                    assertTrue(props.next());
                    assertEquals(props.propertyKey(), newToken);
                    assertEquals(props.propertyValue(), longValue(42));
                    assertFalse(props.next());
                }
            }
            tx.rollback();
        }
    }

    private void assertAccessSingleNodeProperty(long nodeId, Object expectedValue, ValueGroup expectedValueType) {
        // given
        try (NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
                PropertyCursor props = cursors.allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
            // when
            read.singleNode(nodeId, node);
            assertTrue(node.next(), "node by reference");
            assertTrue(hasProperties(node, props), "has properties");

            node.properties(props);
            assertTrue(props.next(), "has properties by direct method");
            assertEquals(expectedValue, props.propertyValue(), "correct value");
            assertEquals(expectedValueType, props.propertyType(), "correct value type ");
            assertFalse(props.next(), "single property");

            read.nodeProperties(node.nodeReference(), node.propertiesReference(), ALL_PROPERTIES, props);
            assertTrue(props.next(), "has properties via property ref");
            assertEquals(expectedValue, props.propertyValue(), "correct value");
            assertFalse(props.next(), "single property");
        }
    }

    private void assertAccessSingleRelationshipProperty(
            long relationshipId, Object expectedValue, ValueGroup expectedValueType) {
        // given
        try (RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor(NULL_CONTEXT);
                PropertyCursor props = cursors.allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
            // when
            read.singleRelationship(relationshipId, relationship);
            assertTrue(relationship.next(), "relationship by reference");

            assertTrue(hasProperties(relationship, props), "has properties");

            relationship.properties(props);
            assertTrue(props.next(), "has properties by direct method");
            assertEquals(expectedValue, props.propertyValue(), "correct value");
            assertEquals(expectedValueType, props.propertyType(), "correct value type ");
            assertFalse(props.next(), "single property");

            read.relationshipProperties(
                    relationship.relationshipReference(), relationship.propertiesReference(), ALL_PROPERTIES, props);
            assertTrue(props.next(), "has properties via property ref");
            assertEquals(expectedValue, props.propertyValue(), "correct value");
            assertFalse(props.next(), "single property");
        }
    }

    private static boolean hasProperties(NodeCursor node, PropertyCursor props) {
        node.properties(props);
        return props.next();
    }

    private static boolean hasProperties(RelationshipDataAccessor relationship, PropertyCursor props) {
        relationship.properties(props);
        return props.next();
    }
}
