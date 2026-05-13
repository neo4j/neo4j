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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.test.RandomSupport;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

record ValueCreatorUtil<KEY extends NativeIndexKey<KEY>>(
        IndexDescriptor indexDescriptor, ValueType[] supportedTypes, double fractionDuplicates) {

    static final double FRACTION_DUPLICATE_UNIQUE = 0;
    static final double FRACTION_DUPLICATE_NON_UNIQUE = 0.1;
    private static final double FRACTION_EXTREME_VALUE = 0.25;
    private static final Comparator<EagerValueIndexEntryUpdate> UPDATE_COMPARATOR =
            (u1, u2) -> Values.COMPARATOR.compare(u1.values()[0], u2.values()[0]);
    public static final int N_VALUES = 10;

    int compareIndexedPropertyValue(KEY key1, KEY key2) {
        return Values.COMPARATOR.compare(key1.asValues()[0], key2.asValues()[0]);
    }

    static PropertyIndexQuery rangeQuery(Value from, boolean fromInclusive, Value to, boolean toInclusive) {
        return PropertyIndexQuery.range(0, from, fromInclusive, to, toInclusive);
    }

    EagerValueIndexEntryUpdate[] someUpdates(RandomSupport randomRule) {
        return someUpdates(randomRule, supportedTypes(), fractionDuplicates());
    }

    EagerValueIndexEntryUpdate[] someUpdates(RandomSupport random, ValueType[] types, boolean allowDuplicates) {
        double fractionDuplicates = allowDuplicates ? FRACTION_DUPLICATE_NON_UNIQUE : FRACTION_DUPLICATE_UNIQUE;
        return someUpdates(random, types, fractionDuplicates);
    }

    private EagerValueIndexEntryUpdate[] someUpdates(
            RandomSupport random, ValueType[] types, double fractionDuplicates) {
        RandomValues rv = RandomValues.create(
                random.random(),
                RandomValues.newConfigurationBuilder()
                        .maxVectorNumBytes(RandomValues.MAX_NUM_BYTES_IN_INDEX_KEY / N_VALUES)
                        .build());
        RandomValueGenerator valueGenerator = new RandomValueGenerator(rv, types, fractionDuplicates);
        RandomUpdateGenerator randomUpdateGenerator = new RandomUpdateGenerator(valueGenerator);
        EagerValueIndexEntryUpdate[] result = new EagerValueIndexEntryUpdate[N_VALUES];
        for (int i = 0; i < N_VALUES; i++) {
            result[i] = randomUpdateGenerator.next();
        }
        return result;
    }

    EagerValueIndexEntryUpdate[] someUpdatesWithDuplicateValues(RandomSupport randomRule) {
        RandomValues randomValues = RandomValues.create(
                randomRule.random(),
                RandomValues.newConfigurationBuilder()
                        .maxVectorNumBytes(RandomValues.MAX_NUM_BYTES_IN_INDEX_KEY / N_VALUES)
                        .build());
        Iterator<Value> valueIterator = new RandomValueGenerator(randomValues, supportedTypes(), fractionDuplicates());
        Value[] someValues = new Value[N_VALUES];
        for (int i = 0; i < N_VALUES; i++) {
            someValues[i] = valueIterator.next();
        }
        return generateAddUpdatesFor(ArrayUtils.addAll(someValues, someValues));
    }

    Iterator<EagerValueIndexEntryUpdate> randomUpdateGenerator(RandomSupport randomRule) {
        return randomUpdateGenerator(randomRule, supportedTypes());
    }

    Iterator<EagerValueIndexEntryUpdate> randomUpdateGenerator(RandomValues randomValues) {
        return randomUpdateGenerator(randomValues, supportedTypes());
    }

    Iterator<EagerValueIndexEntryUpdate> randomUpdateGenerator(RandomSupport random, ValueType[] types) {
        Iterator<Value> valueIterator = new RandomValueGenerator(
                RandomValues.create(
                        random.random(),
                        RandomValues.newConfigurationBuilder()
                                .maxVectorNumBytes(RandomValues.MAX_NUM_BYTES_IN_INDEX_KEY)
                                .build()),
                types,
                fractionDuplicates());
        return new RandomUpdateGenerator(valueIterator);
    }

    Iterator<EagerValueIndexEntryUpdate> randomUpdateGenerator(RandomValues randomValues, ValueType[] types) {
        Iterator<Value> valueIterator = new RandomValueGenerator(randomValues, types, fractionDuplicates());
        return new RandomUpdateGenerator(valueIterator);
    }

    EagerValueIndexEntryUpdate[] generateAddUpdatesFor(Value[] values) {
        EagerValueIndexEntryUpdate[] indexEntryUpdates = new EagerValueIndexEntryUpdate[values.length];
        for (int i = 0; i < indexEntryUpdates.length; i++) {
            indexEntryUpdates[i] = add(i, values[i]);
        }
        return indexEntryUpdates;
    }

    static Value[] extractValuesFromUpdates(EagerValueIndexEntryUpdate[] updates) {
        Value[] values = new Value[updates.length];
        for (int i = 0; i < updates.length; i++) {
            if (updates[i].values().length > 1) {
                throw new UnsupportedOperationException("This method does not support composite entries");
            }
            values[i] = updates[i].values()[0];
        }
        return values;
    }

    EagerValueIndexEntryUpdate add(long nodeId, Value value) {
        return EagerValueIndexEntryUpdate.add(nodeId, indexDescriptor, value);
    }

    static long countUniqueValues(EagerValueIndexEntryUpdate[] updates) {
        return Stream.of(updates).map(update -> update.values()[0]).distinct().count();
    }

    static long countUniqueValues(Value[] updates) {
        return Arrays.stream(updates).distinct().count();
    }

    static void sort(EagerValueIndexEntryUpdate[] updates) {
        Arrays.sort(updates, UPDATE_COMPARATOR);
    }

    private static class RandomValueGenerator extends PrefetchingIterator<Value> {
        private final Set<Value> uniqueCompareValues;
        private final List<Value> uniqueValues;
        private final ValueType[] types;
        private final double fractionDuplicates;
        private final RandomValues randomValues;

        RandomValueGenerator(RandomValues randomValues, ValueType[] types, double fractionDuplicates) {
            this.types = types;
            this.fractionDuplicates = fractionDuplicates;
            this.randomValues = randomValues;
            this.uniqueCompareValues = new HashSet<>();
            this.uniqueValues = new ArrayList<>();
        }

        @Override
        protected Value fetchNextOrNull() {
            Value value;
            if (fractionDuplicates > 0 && !uniqueValues.isEmpty() && randomValues.nextFloat() < fractionDuplicates) {
                value = randomValues.among(uniqueValues);
            } else {
                value = newUniqueValue(randomValues, uniqueCompareValues, uniqueValues);
            }

            return value;
        }

        private Value newUniqueValue(RandomValues random, Set<Value> uniqueCompareValues, List<Value> uniqueValues) {
            int attempts = 0;
            int maxAttempts = 10; // To avoid infinite loop on booleans
            Value value;
            do {
                attempts++;
                ValueType type = randomValues.among(types);
                boolean useExtremeValue = attempts == 1
                        && extremeValueOfTypeCanBeStoredInIndex(type)
                        && randomValues.nextDouble() < FRACTION_EXTREME_VALUE;
                if (useExtremeValue) {
                    value = randomValues.among(type.extremeValues());
                } else {
                    value = random.nextValueOfType(type);
                }
            } while (attempts < maxAttempts && !uniqueCompareValues.add(value));
            uniqueValues.add(value);
            return value;
        }

        private boolean extremeValueOfTypeCanBeStoredInIndex(ValueType value) {
            return switch (value) {
                /* The extreme value of these types can not be stored in a single page, and does hence not fit in the index key */
                case INT16_VECTOR, INT32_VECTOR, INT64_VECTOR, FLOAT32_VECTOR, FLOAT64_VECTOR -> false;
                default -> true;
            };
        }
    }

    private class RandomUpdateGenerator extends PrefetchingIterator<EagerValueIndexEntryUpdate> {
        private final Iterator<Value> valueIterator;
        private long currentEntityId;

        RandomUpdateGenerator(Iterator<Value> valueIterator) {
            this.valueIterator = valueIterator;
        }

        @Override
        protected EagerValueIndexEntryUpdate fetchNextOrNull() {
            Value value = valueIterator.next();
            return add(currentEntityId++, value);
        }
    }
}
