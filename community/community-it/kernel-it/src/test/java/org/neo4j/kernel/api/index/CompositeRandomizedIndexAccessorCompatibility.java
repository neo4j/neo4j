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
package org.neo4j.kernel.api.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.storageengine.api.EagerValueIndexEntryUpdate.add;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

abstract class CompositeRandomizedIndexAccessorCompatibility extends IndexAccessorCompatibility {
    CompositeRandomizedIndexAccessorCompatibility(
            PropertyIndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype) {
        super(testSuite, prototype);
    }

    abstract static class Exact extends CompositeRandomizedIndexAccessorCompatibility {
        Exact(PropertyIndexProviderCompatibilityTestSuite testSuite) {
            // composite index of 4 properties
            super(testSuite, IndexPrototype.forSchema(forLabel(1000, 0, 1, 2, 3)));
        }

        @Test
        void testExactMatchOnRandomCompositeValues() throws Exception {
            // given
            RandomValues randomValues = randomValues(4);
            ValueType[] types = randomSetOfSupportedTypes();
            List<EagerValueIndexEntryUpdate> updates = new ArrayList<>();
            Set<ValueTuple> duplicateChecker = new HashSet<>();
            for (long id = 0; id < 30_000; id++) {
                EagerValueIndexEntryUpdate update;
                do {
                    update = add(
                            id,
                            descriptor,
                            randomValues.nextValueOfTypes(types),
                            randomValues.nextValueOfTypes(types),
                            randomValues.nextValueOfTypes(types),
                            randomValues.nextValueOfTypes(types));
                } while (!duplicateChecker.add(ValueTuple.of(update.values())));
                updates.add(update);
            }
            updateAndCommit(updates);

            // when
            TokenNameLookup tokens = new InMemoryTokens();
            for (EagerValueIndexEntryUpdate update : updates) {
                // then
                List<Long> hits = query(
                        exact(0, update.values()[0]),
                        exact(1, update.values()[1]),
                        exact(2, update.values()[2]),
                        exact(3, update.values()[3]));
                assertEquals(1, hits.size(), update.describe(tokens) + " " + hits);
                assertThat(single(hits)).isEqualTo(update.getEntityId());
            }
        }
    }

    abstract static class Range extends CompositeRandomizedIndexAccessorCompatibility {
        Range(PropertyIndexProviderCompatibilityTestSuite testSuite) {
            // composite index of 2 properties
            super(testSuite, IndexPrototype.forSchema(forLabel(1000, 0, 1)));
        }

        /**
         * All entries in composite index look like (booleanValue, randomValue ).
         * Range queries in composite only work if all predicates before it is exact.
         * We use boolean values for exact part so that we get some real ranges to work
         * on in second composite slot where the random values are.
         */
        @Test
        void testRangeMatchOnRandomValues() throws Exception {
            assumeTrue(testSuite.supportsGranularCompositeQueries(), "Assume support for granular composite queries");
            // given
            ValueType[] types = randomSetOfSupportedAndSortableTypes();
            Set<ValueTuple> uniqueValues = new HashSet<>();
            TreeSet<ValueAndId> sortedValues =
                    new TreeSet<>((v1, v2) -> ValueTuple.COMPARATOR.compare(v1.value, v2.value));
            MutableLong nextId = new MutableLong();

            for (int i = 0; i < 5; i++) {
                List<EagerValueIndexEntryUpdate> updates = new ArrayList<>();
                if (i == 0) {
                    // The initial batch of data can simply be additions
                    updates = generateUpdatesFromValues(generateValuesFromType(types, uniqueValues, 20_000), nextId);
                    sortedValues.addAll(updates.stream()
                            .map(u -> new ValueAndId(ValueTuple.of(u.values()), u.getEntityId()))
                            .toList());
                } else {
                    // Then do all sorts of updates
                    for (int j = 0; j < 1_000; j++) {
                        int type = random.intBetween(0, 2);
                        if (type == 0) { // add
                            ValueTuple value = generateUniqueRandomValue(types, uniqueValues);
                            if (value == null) {
                                continue;
                            }
                            long id = nextId.getAndIncrement();
                            sortedValues.add(new ValueAndId(value, id));
                            updates.add(add(id, descriptor, value.getValues()));
                        } else if (type == 1) { // update
                            ValueAndId existing = random.among(sortedValues.toArray(new ValueAndId[0]));
                            ValueTuple newValue = generateUniqueRandomValue(types, uniqueValues);
                            if (newValue == null) {
                                continue;
                            }
                            sortedValues.remove(existing);
                            uniqueValues.remove(existing.value);
                            sortedValues.add(new ValueAndId(newValue, existing.id));
                            updates.add(EagerValueIndexEntryUpdate.change(
                                    existing.id, descriptor, existing.value.getValues(), newValue.getValues()));
                        } else { // remove
                            ValueAndId existing = random.among(sortedValues.toArray(new ValueAndId[0]));
                            sortedValues.remove(existing);
                            uniqueValues.remove(existing.value);
                            updates.add(EagerValueIndexEntryUpdate.remove(
                                    existing.id, descriptor, existing.value.getValues()));
                        }
                    }
                }
                updateAndCommit(updates);
                verifyRandomRanges(types, sortedValues);
            }
        }

        private void verifyRandomRanges(ValueType[] types, TreeSet<ValueAndId> sortedValues) throws Exception {
            for (int i = 0; i < 100; i++) {
                Value booleanValue = random.randomValues().nextBooleanValue();
                ValueType type = random.among(types);
                Value from = random.randomValues().nextValueOfType(type);
                Value to = random.randomValues().nextValueOfType(type);
                if (Values.COMPARATOR.compare(from, to) > 0) {
                    Value tmp = from;
                    from = to;
                    to = tmp;
                }
                boolean fromInclusive = random.nextBoolean();
                boolean toInclusive = random.nextBoolean();

                // when
                List<Long> expectedIds = expectedIds(sortedValues, booleanValue, from, to, fromInclusive, toInclusive);

                // Depending on order capabilities we verify ids or order and ids.
                PropertyIndexQuery[] predicates = new PropertyIndexQuery[] {
                    exact(0, booleanValue), PropertyIndexQuery.range(1, from, fromInclusive, to, toInclusive)
                };

                List<Long> actualIds = assertInOrder(IndexOrder.ASCENDING, predicates);
                actualIds.sort(Long::compare);
                // then
                assertThat(actualIds).isEqualTo(expectedIds);

                actualIds = assertInOrder(IndexOrder.DESCENDING, predicates);
                actualIds.sort(Long::compare);
                // then
                assertThat(actualIds).isEqualTo(expectedIds);
            }
        }

        static List<Long> expectedIds(
                TreeSet<ValueAndId> sortedValues,
                Value booleanValue,
                Value from,
                Value to,
                boolean fromInclusive,
                boolean toInclusive) {
            return sortedValues
                    .subSet(
                            new ValueAndId(ValueTuple.of(booleanValue, from), 0), fromInclusive,
                            new ValueAndId(ValueTuple.of(booleanValue, to), 0), toInclusive)
                    .stream()
                    .map(v -> v.id)
                    .sorted(Long::compare)
                    .toList();
        }

        private List<ValueTuple> generateValuesFromType(
                ValueType[] types, Set<ValueTuple> duplicateChecker, int count) {
            List<ValueTuple> values = new ArrayList<>();
            for (long i = 0; i < count; i++) {
                ValueTuple value = generateUniqueRandomValue(types, duplicateChecker);
                if (value != null) {
                    values.add(value);
                }
            }
            return values;
        }

        private ValueTuple generateUniqueRandomValue(ValueType[] types, Set<ValueTuple> duplicateChecker) {
            ValueTuple value;
            RandomValues randomValues = RandomValues.create(
                    random.random(),
                    RandomValues.newConfigurationBuilder()
                            .maxVectorNumBytes(RandomValues.MAX_NUM_BYTES_IN_INDEX_KEY / 2)
                            .build());
            int maxTries = 0;
            do {
                value = ValueTuple.of(
                        // Use boolean for first slot in composite because we will use exact match on this part.x
                        randomValues.nextBooleanValue(), randomValues.nextValueOfTypes(types));
                if (maxTries++ == 1000) {
                    return null;
                }
            } while (!duplicateChecker.add(value));
            return value;
        }

        private List<EagerValueIndexEntryUpdate> generateUpdatesFromValues(
                List<ValueTuple> values, MutableLong nextId) {
            List<EagerValueIndexEntryUpdate> updates = new ArrayList<>();
            for (ValueTuple value : values) {
                updates.add(add(nextId.getAndIncrement(), descriptor, value.getValues()));
            }
            return updates;
        }
    }

    private static class ValueAndId {
        private final ValueTuple value;
        private final long id;

        ValueAndId(ValueTuple value, long id) {
            this.value = value;
            this.id = id;
        }
    }
}
