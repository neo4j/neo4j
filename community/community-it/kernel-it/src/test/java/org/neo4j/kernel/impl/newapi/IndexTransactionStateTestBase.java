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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.collector.Collectors2;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

abstract class IndexTransactionStateTestBase extends KernelAPIWriteTestBase<WriteTestSupport> {
    static final String INDEX_NAME = "myIndex";
    static final String DEFAULT_PROPERTY_NAME = "prop";

    @ParameterizedTest
    @MethodSource("parametersForSuffixAndContains")
    void shouldPerformStringSuffixSearch(IndexType indexType, boolean needsValues) throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "1suff"));
            entityWithProp(tx, "pluff");
            tx.commit();
        }

        createIndex(indexType);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            expected.add(entityWithProp(tx, "2suff"));
            entityWithPropId(tx, "skruff");
            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            assertEntityAndValueForSeek(
                    expected,
                    tx,
                    index,
                    needsValues,
                    "pasuff",
                    PropertyIndexQuery.stringSuffix(prop, stringValue("suff")));
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldPerformScan(IndexType indexType, boolean needsValues) throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        long entityToDelete;
        long entityToChange;
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "suff1"));
            expected.add(entityWithProp(tx, "supp"));
            entityToDelete = entityWithPropId(tx, "supp");
            entityToChange = entityWithPropId(tx, "supper");
            tx.commit();
        }

        createIndex(indexType);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "suff2"));
            deleteEntity(tx, entityToDelete);
            removeProperty(tx, entityToChange);

            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);

            assertEntityAndValueForScan(expected, tx, index, needsValues, "noff");
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexType.class,
            names = {"RANGE", "TEXT"})
    void shouldPerformEqualitySeek(IndexType indexType) throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "banana"));
            entityWithProp(tx, "apple");
            tx.commit();
        }

        createIndex(indexType);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "banana"));
            entityWithProp(tx, "dragonfruit");
            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            // Equality seek does never provide values
            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            assertEntityAndValueForSeek(expected, tx, index, false, "banana", PropertyIndexQuery.exact(prop, "banana"));
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldPerformStringPrefixSearch(IndexType indexType, boolean needsValues) throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "suff1"));
            entityWithPropId(tx, "supp");
            tx.commit();
        }

        createIndex(indexType);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            expected.add(entityWithProp(tx, "suff2"));
            entityWithPropId(tx, "skruff");
            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);

            assertEntityAndValueForSeek(
                    expected,
                    tx,
                    index,
                    needsValues,
                    "suffpa",
                    PropertyIndexQuery.stringPrefix(prop, stringValue("suff")));
        }
    }

    @ParameterizedTest
    @MethodSource("parametersForRange")
    void shouldPerformStringRangeSearch(IndexType indexType, boolean needsValues) throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "banana"));
            entityWithProp(tx, "apple");
            tx.commit();
        }

        createIndex(indexType);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            expected.add(entityWithProp(tx, "cherry"));
            entityWithProp(tx, "dragonfruit");
            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            assertEntityAndValueForSeek(
                    expected, tx, index, needsValues, "berry", PropertyIndexQuery.range(prop, "b", true, "d", false));
        }
    }

    @ParameterizedTest
    @MethodSource("parametersForRange")
    void shouldPerformStringRangeSearchWithAddedEntityInTxState(IndexType indexType, boolean needsValues)
            throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        long entityToChange;
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "banana"));
            entityToChange = entityWithPropId(tx, "apple");
            tx.commit();
        }

        createIndex(indexType);

        // when
        try (KernelTransaction tx = beginTransaction()) {

            expected.add(entityWithProp(tx, "cherry"));
            entityWithProp(tx, "dragonfruit");
            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            TextValue newProperty = stringValue("blueberry");
            setProperty(tx, entityToChange, newProperty);
            expected.add(Pair.of(entityToChange, newProperty));

            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            assertEntityAndValueForSeek(
                    expected, tx, index, needsValues, "berry", PropertyIndexQuery.range(prop, "b", true, "d", false));
        }
    }

    @ParameterizedTest
    @MethodSource("parametersForRange")
    void shouldPerformStringRangeSearchWithChangedEntityInTxState(IndexType indexType, boolean needsValues)
            throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        long entityToChange;
        try (KernelTransaction tx = beginTransaction()) {
            entityToChange = entityWithPropId(tx, "banana");
            entityWithPropId(tx, "apple");
            tx.commit();
        }

        createIndex(indexType);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "cherry"));
            entityWithProp(tx, "dragonfruit");
            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            TextValue newProperty = stringValue("kiwi");
            setProperty(tx, entityToChange, newProperty);

            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            assertEntityAndValueForSeek(
                    expected, tx, index, needsValues, "berry", PropertyIndexQuery.range(prop, "b", true, "d", false));
        }
    }

    @ParameterizedTest
    @MethodSource("parametersForRange")
    void shouldPerformStringRangeSearchWithRemovedRemovedPropertyInTxState(IndexType indexType, boolean needsValues)
            throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        long entityToChange;
        try (KernelTransaction tx = beginTransaction()) {
            entityToChange = entityWithPropId(tx, "banana");
            entityWithPropId(tx, "apple");
            tx.commit();
        }

        createIndex(indexType);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "cherry"));
            entityWithProp(tx, "dragonfruit");
            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            removeProperty(tx, entityToChange);

            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            assertEntityAndValueForSeek(
                    expected, tx, index, needsValues, "berry", PropertyIndexQuery.range(prop, "b", true, "d", false));
        }
    }

    @ParameterizedTest
    @MethodSource("parametersForRange")
    void shouldPerformStringRangeSearchWithDeletedEntityInTxState(IndexType indexType, boolean needsValues)
            throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        long entityToChange;
        try (KernelTransaction tx = beginTransaction()) {
            entityToChange = entityWithPropId(tx, "banana");
            entityWithPropId(tx, "apple");
            tx.commit();
        }

        createIndex(indexType);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            expected.add(entityWithProp(tx, "cherry"));
            entityWithProp(tx, "dragonfruit");
            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            deleteEntity(tx, entityToChange);

            assertEntityAndValueForSeek(
                    expected, tx, index, needsValues, "berry", PropertyIndexQuery.range(prop, "b", true, "d", false));
        }
    }

    @ParameterizedTest
    @MethodSource("parametersForSuffixAndContains")
    void shouldPerformStringContainsSearch(IndexType indexType, boolean needsValues) throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "gnomebat"));
            entityWithPropId(tx, "fishwombat");
            tx.commit();
        }

        createIndex(indexType);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "homeopatic"));
            entityWithPropId(tx, "telephonecompany");
            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);

            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            assertEntityAndValueForSeek(
                    expected,
                    tx,
                    index,
                    needsValues,
                    "immense",
                    PropertyIndexQuery.stringContains(prop, stringValue("me")));
        }
    }

    @Test
    void shouldThrowIfTransactionTerminated() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            // given
            terminate(tx);

            // when
            assertThrows(TransactionTerminatedException.class, () -> entityExists(tx, 42));
        }
    }

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }

    private static Stream<Arguments> parameters() {
        // Text index doesn't contain values
        return Stream.of(
                Arguments.of(IndexType.RANGE, true),
                Arguments.of(IndexType.RANGE, false),
                Arguments.of(IndexType.TEXT, false));
    }

    private static Stream<Arguments> parametersForRange() {
        return Stream.of(Arguments.of(IndexType.RANGE, true), Arguments.of(IndexType.RANGE, false));
    }

    // Range index doesn't support suffix/contains queries, and text index doesn't contain values
    private static Stream<Arguments> parametersForSuffixAndContains() {
        return Stream.of(Arguments.of(IndexType.TEXT, false));
    }

    private static void terminate(KernelTransaction transaction) {
        transaction.markForTermination(Status.Transaction.Terminated);
    }

    long entityWithPropId(KernelTransaction tx, Object value) throws Exception {
        return entityWithProp(tx, value).first();
    }

    void assertEntityAndValue(
            Set<Pair<Long, Value>> expected,
            KernelTransaction tx,
            boolean needsValues,
            Object anotherValueFoundByQuery,
            EntityValueIndexCursor entities)
            throws Exception {
        // Modify tx state with changes that should not be reflected in the cursor,
        // since the cursor was already initialized in the code calling this method
        entityWithPropId(tx, anotherValueFoundByQuery);

        if (needsValues) {
            Set<Pair<Long, Value>> found = new HashSet<>();
            while (entities.next()) {
                found.add(Pair.of(entities.entityReference(), entities.propertyValue(0)));
            }

            assertThat(found).isEqualTo(expected);
        } else {
            Set<Long> foundIds = new HashSet<>();
            while (entities.next()) {
                foundIds.add(entities.entityReference());
            }
            ImmutableSet<Long> expectedIds = expected.stream().map(Pair::first).collect(Collectors2.toImmutableSet());

            assertThat(foundIds).isEqualTo(expectedIds);
        }
    }

    abstract Pair<Long, Value> entityWithProp(KernelTransaction tx, Object value) throws Exception;

    abstract void createIndex(IndexType indexType);

    abstract void deleteEntity(KernelTransaction tx, long entity) throws Exception;

    abstract boolean entityExists(KernelTransaction tx, long entity);

    abstract void removeProperty(KernelTransaction tx, long entity) throws Exception;

    abstract void setProperty(KernelTransaction tx, long entity, Value value) throws Exception;

    /**
     * Perform an index seek and assert that the correct entities and values were found.
     *
     * Since this method modifies TX state for the test it is not safe to call this method more than once in the same transaction.
     *
     * @param expected the expected entities and values
     * @param tx the transaction
     * @param index the index
     * @param needsValues if the index is expected to provide values
     * @param anotherValueFoundByQuery a value that would be found by the index queries, if an entity with that value existed. This method
     * will create an entity with that value after initializing the cursor and assert that the new entity is not found.
     * @param queries the index queries
     */
    abstract void assertEntityAndValueForSeek(
            Set<Pair<Long, Value>> expected,
            KernelTransaction tx,
            IndexDescriptor index,
            boolean needsValues,
            Object anotherValueFoundByQuery,
            PropertyIndexQuery... queries)
            throws Exception;

    /**
     * Perform an index scan and assert that the correct entities and values were found.
     *
     * Since this method modifies TX state for the test it is not safe to call this method more than once in the same transaction.
     *
     * @param expected the expected entities and values
     * @param tx the transaction
     * @param index the index
     * @param needsValues if the index is expected to provide values
     * @param anotherValueFoundByQuery a value that would be found if an entity with that value existed. This method
     * will create an entity with that value after initializing the cursor and assert that the new entity is not found.
     */
    abstract void assertEntityAndValueForScan(
            Set<Pair<Long, Value>> expected,
            KernelTransaction tx,
            IndexDescriptor index,
            boolean needsValues,
            Object anotherValueFoundByQuery)
            throws Exception;

    interface EntityValueIndexCursor {
        boolean next();

        Value propertyValue(int offset);

        long entityReference();
    }
}
