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
package org.neo4j.kernel.api.impl.fulltext;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.internal.helpers.collection.Iterators.array;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

class FulltextIndexTextArrayTest extends FulltextProceduresTestSupport {

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void shouldFindIndexedTextArray(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        String id;
        try (Transaction tx = db.beginTx()) {
            id = entityUtil.createEntityWithProperty(tx, array("foo", "bar"));
            tx.commit();
        }

        assertEntityFound(entityUtil, id, "foo");
        assertEntityFound(entityUtil, id, "bar");
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void shouldFindIndexedTextArraySingleElement(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        String id;
        try (Transaction tx = db.beginTx()) {
            id = entityUtil.createEntityWithProperty(tx, array("foo"));
            tx.commit();
        }

        assertEntityFound(entityUtil, id, "foo");
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void shouldFindIndexedTextArrayWithEmptyElement(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        String id;
        try (Transaction tx = db.beginTx()) {
            id = entityUtil.createEntityWithProperty(tx, array("foo", "bar", ""));
            tx.commit();
        }

        assertEntityFound(entityUtil, id, "foo");
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void testEmptyArray(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        try (Transaction tx = db.beginTx()) {
            entityUtil.createEntityWithProperty(tx, EMPTY_STRING_ARRAY);
            tx.commit();
        }

        assertNothingFound(entityUtil, "*");
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void shouldFindIndexedTextArrayReferencingProperty(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        String id;
        try (Transaction tx = db.beginTx()) {
            id = entityUtil.createEntityWithProperty(tx, array("foo", "bar"));
            tx.commit();
        }

        assertEntityFound(entityUtil, id, "prop:bar");
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void shouldFindIndexedCharArray(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        String id;
        try (Transaction tx = db.beginTx()) {
            id = entityUtil.createEntityWithProperty(tx, array('a', 'b', 'c'));
            tx.commit();
        }

        assertEntityFound(entityUtil, id, "a");
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void shouldFindInCompositeIndex(EntityUtil entityUtil) {
        createCompositeIndexAndWait(entityUtil);

        String id;
        try (Transaction tx = db.beginTx()) {
            id = entityUtil.createEntityWithProperties(tx, "fred", array("foo", "bar"));
            tx.commit();
        }

        assertEntityFound(entityUtil, id, "prop2:bar");
    }

    private void assertEntityFound(EntityUtil entityUtil, String id, String query) {
        try (Transaction tx = db.beginTx();
                ResourceIterator<Entity> iterator = entityUtil.queryIndexWithOptions(tx, query, "{}")) {
            assertThat(iterator.next().getElementId()).isEqualTo(id);
            assertFalse(iterator.hasNext());
            tx.commit();
        }
    }

    private void assertNothingFound(EntityUtil entityUtil, String query) {
        try (Transaction tx = db.beginTx();
                ResourceIterator<Entity> iterator = entityUtil.queryIndexWithOptions(tx, query, "{}")) {
            assertFalse(iterator.hasNext());
            tx.commit();
        }
    }
}
