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
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
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
    private static final Comparator<ValueIndexEntryUpdate<IndexDescriptor>> UPDATE_COMPARATOR =
            (u1, u2) -> Values.COMPARATOR.compare(u1.values()[0], u2.values()[0]);
    private static final int N_VALUES = 10;

    int compareIndexedPropertyValue(KEY key1, KEY key2) {
        return Values.COMPARATOR.compare(key1.asValues()[0], key2.asValues()[0]);
    }

    static PropertyIndexQuery rangeQuery(Value from, boolean fromInclusive, Value to, boolean toInclusive) {
        return PropertyIndexQuery.range(0, from, fromInclusive, to, toInclusive);
    }

    ValueIndexEntryUpdate<IndexDescriptor>[] someUpdates(RandomSupport randomRule) {
        return someUpdates(randomRule, supportedTypes(), fractionDuplicates());
    }

    ValueIndexEntryUpdate<IndexDescriptor>[] someUpdates(
            RandomSupport random, ValueType[] types, boolean allowDuplicates) {
        double fractionDuplicates = allowDuplicates ? FRACTION_DUPLICATE_NON_UNIQUE : FRACTION_DUPLICATE_UNIQUE;
        return someUpdates(random, types, fractionDuplicates);
    }

    private ValueIndexEntryUpdate<IndexDescriptor>[] someUpdates(
            RandomSupport random, ValueType[] types, double fractionDuplicates) {
        RandomValueGenerator valueGenerator =
                new RandomValueGenerator(random.randomValues(), types, fractionDuplicates);
        RandomUpdateGenerator randomUpdateGenerator = new RandomUpdateGenerator(valueGenerator);
        //noinspection unchecked
        ValueIndexEntryUpdate<IndexDescriptor>[] result = new ValueIndexEntryUpdate[N_VALUES];
        for (int i = 0; i < N_VALUES; i++) {
            result[i] = randomUpdateGenerator.next();
        }
        return result;
    }

    ValueIndexEntryUpdate<IndexDescriptor>[] someUpdatesWithDuplicateValues(RandomSupport randomRule) {
        Iterator<Value> valueIterator =
                new RandomValueGenerator(randomRule.randomValues(), supportedTypes(), fractionDuplicates());
        Value[] someValues = new Value[N_VALUES];
        for (int i = 0; i < N_VALUES; i++) {
            someValues[i] = valueIterator.next();
        }
        return generateAddUpdatesFor(ArrayUtils.addAll(someValues, someValues));
    }

    Iterator<ValueIndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator(RandomSupport randomRule) {
        return randomUpdateGenerator(randomRule, supportedTypes());
    }

    Iterator<ValueIndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator(RandomSupport random, ValueType[] types) {
        Iterator<Value> valueIterator = new RandomValueGenerator(random.randomValues(), types, fractionDuplicates());
        return new RandomUpdateGenerator(valueIterator);
    }

    ValueIndexEntryUpdate<IndexDescriptor>[] generateAddUpdatesFor(Value[] values) {
        //noinspection unchecked
        ValueIndexEntryUpdate<IndexDescriptor>[] indexEntryUpdates = new ValueIndexEntryUpdate[values.length];
        for (int i = 0; i < indexEntryUpdates.length; i++) {
            indexEntryUpdates[i] = add(i, values[i]);
        }
        return indexEntryUpdates;
    }

    static Value[] extractValuesFromUpdates(ValueIndexEntryUpdate<IndexDescriptor>[] updates) {
        Value[] values = new Value[updates.length];
        for (int i = 0; i < updates.length; i++) {
            if (updates[i].values().length > 1) {
                throw new UnsupportedOperationException("This method does not support composite entries");
            }
            values[i] = updates[i].values()[0];
        }
        return values;
    }

    ValueIndexEntryUpdate<IndexDescriptor> add(long nodeId, Value value) {
        return ValueIndexEntryUpdate.add(nodeId, indexDescriptor, value);
    }

    static long countUniqueValues(ValueIndexEntryUpdate<IndexDescriptor>[] updates) {
        return Stream.of(updates).map(update -> update.values()[0]).distinct().count();
    }

    static long countUniqueValues(Value[] updates) {
        return Arrays.stream(updates).distinct().count();
    }

    static void sort(ValueIndexEntryUpdate<IndexDescriptor>[] updates) {
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
                boolean useExtremeValue = attempts == 1 && randomValues.nextDouble() < FRACTION_EXTREME_VALUE;
                if (useExtremeValue) {
                    value = randomValues.among(type.extremeValues());
                } else {
                    value = random.nextValueOfType(type);
                }
            } while (attempts < maxAttempts && !uniqueCompareValues.add(value));
            uniqueValues.add(value);
            return value;
        }
    }

    private class RandomUpdateGenerator extends PrefetchingIterator<ValueIndexEntryUpdate<IndexDescriptor>> {
        private final Iterator<Value> valueIterator;
        private long currentEntityId;

        RandomUpdateGenerator(Iterator<Value> valueIterator) {
            this.valueIterator = valueIterator;
        }

        @Override
        protected ValueIndexEntryUpdate<IndexDescriptor> fetchNextOrNull() {
            Value value = valueIterator.next();
            return add(currentEntityId++, value);
        }
    }
}
