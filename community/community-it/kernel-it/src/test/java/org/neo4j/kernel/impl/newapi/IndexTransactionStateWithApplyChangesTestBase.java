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

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.collector.Collectors2;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;

abstract class IndexTransactionStateWithApplyChangesTestBase extends KernelAPIWriteTestBase<WriteTestSupport> {
    static final String INDEX_NAME = "myIndex";
    static final String PROP1 = "prop1";
    static final String PROP2 = "prop2";

    @ParameterizedTest
    @MethodSource("parameters")
    void applyChangesShouldAddPropertiesToIndexTxState(IndexType indexType, boolean needsValues) throws Exception {
        createIndex(indexType);

        Set<EntityWithProps> expected = new HashSet<>();

        try (var tx = beginTransaction()) {
            EntityWithProps entityWithProps = entityWithProps(tx, "books", "looks");
            expected.add(entityWithProps);

            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            assertEntityAndValueForScan(expected, tx, index, needsValues, "something", "else");
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void applyChangesShouldChangePropertiesInIndexTxState(IndexType indexType, boolean needsValues) throws Exception {
        createIndex(indexType);

        EntityWithProps entityWithProps;
        try (var tx = beginTransaction()) {
            entityWithProps = entityWithProps(tx, "books", "looks");
            tx.commit();
        }

        Set<EntityWithProps> expected = new HashSet<>();
        try (var tx = beginTransaction()) {
            expected.add(setProperties(tx, entityWithProps.entityId, "val1", "val2"));

            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            assertEntityAndValueForScan(expected, tx, index, needsValues, "something", "else");
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void applyChangesShouldRemovePropertiesInIndexTxState(IndexType indexType, boolean needsValues) throws Exception {
        createIndex(indexType);

        EntityWithProps entityWithProps;
        try (var tx = beginTransaction()) {
            entityWithProps = entityWithProps(tx, "books", "looks");
            tx.commit();
        }

        Set<EntityWithProps> expected = new HashSet<>();
        try (var tx = beginTransaction()) {
            removeProperties(tx, entityWithProps.entityId);

            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            assertEntityAndValueForScan(expected, tx, index, needsValues, "something", "else");
        }
    }

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }

    void assertEntityAndValue(
            Set<EntityWithProps> expected,
            KernelTransaction tx,
            boolean needsValues,
            Object anotherValueFoundByQuery,
            Object anotherValueFoundByQuery2,
            EntityValueIndexCursor entities)
            throws Exception {
        // Modify tx state with changes that should not be reflected in the cursor,
        // since the cursor was already initialized in the code calling this method
        for (EntityWithProps entityWithProps : expected) {
            deleteEntity(tx, entityWithProps.entityId);
        }
        entityWithProps(tx, anotherValueFoundByQuery, anotherValueFoundByQuery2);

        if (needsValues) {
            Set<EntityWithProps> found = new HashSet<>();
            while (entities.next()) {
                found.add(new EntityWithProps(
                        entities.entityReference(), entities.propertyValue(0), entities.propertyValue(1)));
            }

            assertThat(found).isEqualTo(expected);
        } else {
            Set<Long> foundIds = new HashSet<>();
            while (entities.next()) {
                foundIds.add(entities.entityReference());
            }
            ImmutableSet<Long> expectedIds =
                    expected.stream().map(EntityWithProps::entityId).collect(Collectors2.toImmutableSet());

            assertThat(foundIds).isEqualTo(expectedIds);
        }
    }

    abstract EntityWithProps entityWithProps(KernelTransaction tx, Object value, Object value2) throws Exception;

    abstract void createIndex(IndexType indexType);

    abstract void deleteEntity(KernelTransaction tx, long entity) throws Exception;

    abstract void removeProperties(KernelTransaction tx, long entity) throws Exception;

    abstract EntityWithProps setProperties(KernelTransaction tx, long entity, Object value, Object value2)
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
     * @param anotherValueFoundByQuery a values that would be found by, if a entity with that value existed. This method
     * will create a entity with that value, after initializing the cursor and assert that the new entity is not found.
     */
    abstract void assertEntityAndValueForScan(
            Set<EntityWithProps> expected,
            KernelTransaction tx,
            IndexDescriptor index,
            boolean needsValues,
            Object anotherValueFoundByQuery,
            Object anotherValueFoundByQuery2)
            throws Exception;

    interface EntityValueIndexCursor {
        boolean next();

        Value propertyValue(int offset);

        long entityReference();
    }

    private static Stream<Arguments> parameters() {
        return Stream.of(Arguments.of(IndexType.RANGE, true), Arguments.of(IndexType.RANGE, false));
    }

    record EntityWithProps(long entityId, Value propValue1, Value propValue2) {}
}
