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
package org.neo4j.kernel.impl.index.schema;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracker.NO_USAGE_TRACKER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

/**
 * Because of the lack of imagination, 'generic' in the name means native indexes using {@link GenericKey}.
 */
abstract class GenericNativeIndexAccessorTests<KEY extends NativeIndexKey<KEY>> extends NativeIndexAccessorTests<KEY> {
    @Test
    void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndExclusiveEnd() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates =
                someUpdatesSingleTypeNoDuplicates(supportedTypesExcludingNonOrderable());
        processAll(updates);
        ValueCreatorUtil.sort(updates);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);
        try (NodeValueIterator result = query(
                reader,
                ValueCreatorUtil.rangeQuery(valueOf(updates[0]), true, valueOf(updates[updates.length - 1]), false))) {
            assertEntityIdHits(extractEntityIds(Arrays.copyOf(updates, updates.length - 1), alwaysTrue()), result);
        }
    }

    @Test
    void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndInclusiveEnd() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates =
                someUpdatesSingleTypeNoDuplicates(supportedTypesExcludingNonOrderable());

        processAll(updates);
        ValueCreatorUtil.sort(updates);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);
        try (NodeValueIterator result = query(
                reader,
                ValueCreatorUtil.rangeQuery(valueOf(updates[0]), true, valueOf(updates[updates.length - 1]), true))) {
            assertEntityIdHits(extractEntityIds(updates, alwaysTrue()), result);
        }
    }

    @Test
    void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndExclusiveEnd() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates =
                someUpdatesSingleTypeNoDuplicates(supportedTypesExcludingNonOrderable());
        processAll(updates);
        ValueCreatorUtil.sort(updates);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);
        try (NodeValueIterator result = query(
                reader,
                ValueCreatorUtil.rangeQuery(valueOf(updates[0]), false, valueOf(updates[updates.length - 1]), false))) {
            assertEntityIdHits(
                    extractEntityIds(Arrays.copyOfRange(updates, 1, updates.length - 1), alwaysTrue()), result);
        }
    }

    @Test
    void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndInclusiveEnd() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates =
                someUpdatesSingleTypeNoDuplicates(supportedTypesExcludingNonOrderable());
        processAll(updates);
        ValueCreatorUtil.sort(updates);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);
        try (NodeValueIterator result = query(
                reader,
                ValueCreatorUtil.rangeQuery(valueOf(updates[0]), false, valueOf(updates[updates.length - 1]), true))) {
            assertEntityIdHits(extractEntityIds(Arrays.copyOfRange(updates, 1, updates.length), alwaysTrue()), result);
        }
    }

    @Test
    void shouldReturnNoEntriesForRangePredicateOutsideAnyMatch() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates =
                someUpdatesSingleTypeNoDuplicates(supportedTypesExcludingNonOrderable());
        ValueCreatorUtil.sort(updates);
        processAll(updates[0], updates[1], updates[updates.length - 1], updates[updates.length - 2]);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);
        try (NodeValueIterator result = query(
                reader,
                ValueCreatorUtil.rangeQuery(valueOf(updates[2]), true, valueOf(updates[updates.length - 3]), true))) {
            assertEntityIdHits(EMPTY_LONG_ARRAY, result);
        }
    }

    @Test
    void mustHandleNestedQueries() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates =
                someUpdatesSingleTypeNoDuplicates(supportedTypesExcludingNonOrderable());

        processAll(updates);
        ValueCreatorUtil.sort(updates);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);

        PropertyIndexQuery outerQuery =
                ValueCreatorUtil.rangeQuery(valueOf(updates[2]), true, valueOf(updates[3]), true);
        PropertyIndexQuery innerQuery =
                ValueCreatorUtil.rangeQuery(valueOf(updates[0]), true, valueOf(updates[1]), true);

        long[] expectedOuter = {entityIdOf(updates[2]), entityIdOf(updates[3])};
        long[] expectedInner = {entityIdOf(updates[0]), entityIdOf(updates[1])};

        Collection<Long> outerResult;
        try (NodeValueIterator outerIter = query(reader, outerQuery)) {
            outerResult = new ArrayList<>();
            while (outerIter.hasNext()) {
                outerResult.add(outerIter.next());
                try (NodeValueIterator innerIter = query(reader, innerQuery)) {
                    assertEntityIdHits(expectedInner, innerIter);
                }
            }
        }
        assertEntityIdHits(expectedOuter, outerResult);
    }

    @Test
    void mustHandleMultipleNestedQueries() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates =
                someUpdatesSingleTypeNoDuplicates(supportedTypesExcludingNonOrderable());

        processAll(updates);
        ValueCreatorUtil.sort(updates);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);

        PropertyIndexQuery query1 = ValueCreatorUtil.rangeQuery(valueOf(updates[4]), true, valueOf(updates[5]), true);
        PropertyIndexQuery query2 = ValueCreatorUtil.rangeQuery(valueOf(updates[2]), true, valueOf(updates[3]), true);
        PropertyIndexQuery query3 = ValueCreatorUtil.rangeQuery(valueOf(updates[0]), true, valueOf(updates[1]), true);

        long[] expected1 = {entityIdOf(updates[4]), entityIdOf(updates[5])};
        long[] expected2 = {entityIdOf(updates[2]), entityIdOf(updates[3])};
        long[] expected3 = {entityIdOf(updates[0]), entityIdOf(updates[1])};

        Collection<Long> result1 = new ArrayList<>();
        try (NodeValueIterator iter1 = query(reader, query1)) {
            while (iter1.hasNext()) {
                result1.add(iter1.next());

                Collection<Long> result2 = new ArrayList<>();
                try (NodeValueIterator iter2 = query(reader, query2)) {
                    while (iter2.hasNext()) {
                        result2.add(iter2.next());

                        Collection<Long> result3 = new ArrayList<>();
                        try (NodeValueIterator iter3 = query(reader, query3)) {
                            while (iter3.hasNext()) {
                                result3.add(iter3.next());
                            }
                        }
                        assertEntityIdHits(expected3, result3);
                    }
                }
                assertEntityIdHits(expected2, result2);
            }
        }
        assertEntityIdHits(expected1, result1);
    }

    @Test
    void shouldNotSeeFilteredEntries() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates =
                someUpdatesSingleTypeNoDuplicates(supportedTypesExcludingNonOrderable());
        processAll(updates);
        ValueCreatorUtil.sort(updates);
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);

        // when
        try (NodeValueIterator iter = new NodeValueIterator()) {
            PropertyIndexQuery.ExactPredicate filter = PropertyIndexQuery.exact(0, valueOf(updates[1]));
            PropertyIndexQuery rangeQuery =
                    ValueCreatorUtil.rangeQuery(valueOf(updates[0]), true, valueOf(updates[2]), true);
            IndexProgressor.EntityValueClient filterClient = filterClient(iter, filter);
            reader.query(filterClient, NULL_CONTEXT, unconstrained(), rangeQuery);

            // then
            assertTrue(iter.hasNext());
            assertEquals(entityIdOf(updates[1]), iter.next());
            assertFalse(iter.hasNext());
        }
    }

    @Test
    void respectIndexOrder() throws Exception {
        // given
        int nUpdates = 10000;
        ValueType[] types = supportedTypesExcludingNonOrderable();
        Iterator<ValueIndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator =
                valueCreatorUtil.randomUpdateGenerator(random, types);
        //noinspection unchecked
        ValueIndexEntryUpdate<IndexDescriptor>[] someUpdates = new ValueIndexEntryUpdate[nUpdates];
        for (int i = 0; i < nUpdates; i++) {
            someUpdates[i] = randomUpdateGenerator.next();
        }
        processAll(someUpdates);
        Value[] allValues = ValueCreatorUtil.extractValuesFromUpdates(someUpdates);

        // when
        try (var reader = accessor.newValueReader(NO_USAGE_TRACKER)) {
            PropertyIndexQuery.AllEntriesPredicate supportedQuery = PropertyIndexQuery.allEntries();

            expectIndexOrder(allValues, reader, IndexOrder.ASCENDING, supportedQuery);

            expectIndexOrder(allValues, reader, IndexOrder.DESCENDING, supportedQuery);
        }
    }

    @Test
    void shouldReturnAllEntriesForExistsPredicate() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll(updates);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);
        try (NodeValueIterator result = query(reader, PropertyIndexQuery.exists(0))) {
            // then
            assertEntityIdHits(extractEntityIds(updates, alwaysTrue()), result);
        }
    }

    @Test
    void shouldReturnNoEntriesForExistsPredicateForEmptyIndex() throws Exception {
        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);
        long[] actual;
        try (NodeValueIterator result = query(reader, PropertyIndexQuery.exists(0))) {
            // then
            actual = PrimitiveLongCollections.asArray(result);
            assertEquals(0, actual.length);
        }
    }

    private ValueType[] supportedTypesExcludingNonOrderable() {
        return RandomValues.excluding(valueCreatorUtil.supportedTypes(), type -> switch (type) {
            case STRING, STRING_ARRAY -> true; // exclude strings outside the Basic Multilingual Plane
            default -> switch (type.valueGroup) {
                case GEOMETRY, GEOMETRY_ARRAY, DURATION, DURATION_ARRAY -> true; // exclude spacial types and durations
                default -> false;
            };
        });
    }

    private static long entityIdOf(ValueIndexEntryUpdate<IndexDescriptor> update) {
        return update.getEntityId();
    }

    private static void expectIndexOrder(
            Value[] expectedValues,
            ValueIndexReader reader,
            IndexOrder supportedOrder,
            PropertyIndexQuery.AllEntriesPredicate supportedQuery)
            throws IndexNotApplicableKernelException {
        if (supportedOrder == IndexOrder.ASCENDING) {
            Arrays.sort(expectedValues, Values.COMPARATOR);
        } else if (supportedOrder == IndexOrder.DESCENDING) {
            Arrays.sort(expectedValues, Values.COMPARATOR.reversed());
        }
        SimpleEntityValueClient client = new SimpleEntityValueClient();
        reader.query(client, NULL_CONTEXT, constrained(supportedOrder, true), supportedQuery);
        int i = 0;
        while (client.next()) {
            assertEquals(expectedValues[i++], client.values[0], "values in order");
        }
        assertEquals(i, expectedValues.length, "found all values");
    }

    protected static Value valueOf(ValueIndexEntryUpdate<IndexDescriptor> update) {
        return update.values()[0];
    }

    private static IndexProgressor.EntityValueClient filterClient(
            final NodeValueIterator iter, final PropertyIndexQuery filter) {
        return new IndexProgressor.EntityValueClient() {
            @Override
            public void initializeQuery(
                    IndexDescriptor descriptor,
                    IndexProgressor progressor,
                    boolean indexIncludesTransactionState,
                    boolean needStoreFilter,
                    IndexQueryConstraints constraints,
                    PropertyIndexQuery... query) {
                iter.initializeQuery(
                        descriptor, progressor, indexIncludesTransactionState, needStoreFilter, constraints, query);
            }

            @Override
            public boolean acceptEntity(long reference, float score, Value... values) {
                //noinspection SimplifiableIfStatement
                if (values.length > 1) {
                    return false;
                }
                return filter.acceptsValue(values[0]) && iter.acceptEntity(reference, score, values);
            }

            @Override
            public boolean needsValues() {
                return true;
            }
        };
    }

    private static void assertEntityIdHits(long[] expected, Collection<Long> result) {
        long[] actual = new long[result.size()];
        int index = 0;
        for (Long aLong : result) {
            actual[index++] = aLong;
        }
        assertSameContent(expected, actual);
    }
}
