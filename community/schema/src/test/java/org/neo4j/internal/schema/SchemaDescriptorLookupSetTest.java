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
package org.neo4j.internal.schema;

import static java.util.Arrays.stream;
import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.schema.SchemaPatternMatchingType.COMPLETE_ALL_TOKENS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class SchemaDescriptorLookupSetTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldLookupSingleKeyDescriptors() {
        // given
        SchemaDescriptorLookupSet<SchemaDescriptorSupplier> set = new SchemaDescriptorLookupSet<>();
        var expected = of(SchemaDescriptors.forLabel(1, 2));
        set.add(expected);

        // when
        var descriptors = new HashSet<SchemaDescriptorSupplier>();
        set.matchingDescriptorsForPartialListOfProperties(descriptors, entityTokens(1), properties(2));

        // then
        assertEquals(asSet(expected), descriptors);
    }

    @Test
    void shouldLookupSingleKeyAndSharedCompositeKeyDescriptors() {
        // given
        var set = new SchemaDescriptorLookupSet<>();
        var expected1 = of(SchemaDescriptors.forLabel(1, 2));
        var expected2 = of(SchemaDescriptors.forLabel(1, 2, 3));
        set.add(expected1);
        set.add(expected2);

        // when
        var descriptors = new HashSet<SchemaDescriptorSupplier>();
        set.matchingDescriptorsForPartialListOfProperties(descriptors, entityTokens(1), properties(2));

        // then
        assertEquals(asSet(expected1, expected2), descriptors);
    }

    @Test
    void shouldLookupCompositeKeyDescriptor() {
        // given
        var set = new SchemaDescriptorLookupSet<>();
        var descriptor1 = of(SchemaDescriptors.forLabel(1, 2, 3));
        var descriptor2 = of(SchemaDescriptors.forLabel(1, 2, 4));
        var descriptor3 = of(SchemaDescriptors.forLabel(1, 2, 5, 6));
        set.add(descriptor1);
        set.add(descriptor2);
        set.add(descriptor3);

        // when
        var descriptors = new HashSet<SchemaDescriptorSupplier>();
        set.matchingDescriptorsForCompleteListOfProperties(descriptors, entityTokens(1), properties(2, 5, 6));

        // then
        assertEquals(asSet(descriptor3), descriptors);
    }

    @Test
    void shouldLookupAllByEntityToken() {
        // given
        var set = new SchemaDescriptorLookupSet<>();
        var descriptor1 = of(SchemaDescriptors.forLabel(1, 2, 3));
        var descriptor2 = of(SchemaDescriptors.forLabel(1, 2, 4));
        var descriptor3 = of(SchemaDescriptors.forLabel(1, 2, 5, 6));
        var descriptor4 = of(SchemaDescriptors.forLabel(2, 2, 3));
        var descriptor5 = of(SchemaDescriptors.forLabel(3, 2, 5, 6));
        set.add(descriptor1);
        set.add(descriptor2);
        set.add(descriptor3);
        set.add(descriptor4);
        set.add(descriptor5);

        // when
        var descriptors = new HashSet<SchemaDescriptorSupplier>();
        set.matchingDescriptors(descriptors, entityTokens(1));

        // then
        assertEquals(asSet(descriptor1, descriptor2, descriptor3), descriptors);
    }

    @Test
    void shouldAddRemoveAndLookupRandomDescriptorsNoIdempotentOperations() {
        shouldAddRemoveAndLookupRandomDescriptors(false);
    }

    @Test
    void shouldAddRemoveAndLookupRandomDescriptorsWithIdempotentOperations() {
        shouldAddRemoveAndLookupRandomDescriptors(true);
    }

    private void shouldAddRemoveAndLookupRandomDescriptors(boolean includeIdempotentAddsAndRemoves) {
        // given
        var all = new ArrayList<SchemaDescriptorSupplier>();
        var set = new SchemaDescriptorLookupSet<>();
        int highEntityKeyId = 8;
        int highPropertyKeyId = 8;
        int maxNumberOfEntityKeys = 3;
        int maxNumberOfPropertyKeys = 3;

        // when/then
        for (int i = 0; i < 100 /*just a couple of rounds adding, removing and looking up descriptors*/; i++) {
            // add some
            int countToAdd = random.nextInt(1, 5);
            for (int a = 0; a < countToAdd; a++) {
                var descriptor = of(randomSchemaDescriptor(
                        highEntityKeyId, highPropertyKeyId, maxNumberOfEntityKeys, maxNumberOfPropertyKeys));
                if (!includeIdempotentAddsAndRemoves && all.indexOf(descriptor) != -1) {
                    // Oops, we randomly generated a descriptor that already exists
                    continue;
                }

                set.add(descriptor);
                all.add(descriptor);
            }

            // remove some
            int countToRemove = random.nextInt(0, 2);
            for (int r = 0; r < countToRemove && !all.isEmpty(); r++) {
                var descriptor = all.remove(random.nextInt(all.size()));
                set.remove(descriptor);
                if (includeIdempotentAddsAndRemoves) {
                    set.remove(descriptor);
                    while (all.remove(descriptor)) {
                        // Just continue removing duplicates until all are done
                    }
                }
            }

            // lookup
            int countToLookup = 20;
            for (int l = 0; l < countToLookup; l++) {
                int[] entityTokenIdsInts = randomUniqueSortedIntArray(highEntityKeyId, random.nextInt(1, 3));
                int[] propertyKeyIds =
                        randomUniqueSortedIntArray(highPropertyKeyId, random.nextInt(1, maxNumberOfPropertyKeys));
                var actual = new HashSet<SchemaDescriptorSupplier>();

                // lookup by only entity tokens
                actual.clear();
                set.matchingDescriptors(actual, entityTokenIdsInts);
                assertEquals(expectedDescriptors(all, filterByEntity(entityTokenIdsInts)), actual);

                // lookup by partial property list
                actual.clear();
                set.matchingDescriptorsForPartialListOfProperties(actual, entityTokenIdsInts, propertyKeyIds);
                assertEquals(
                        expectedDescriptors(all, filterByEntityAndPropertyPartial(entityTokenIdsInts, propertyKeyIds)),
                        actual);

                // lookup by complete property list
                actual.clear();
                set.matchingDescriptorsForCompleteListOfProperties(actual, entityTokenIdsInts, propertyKeyIds);
                assertEquals(
                        expectedDescriptors(all, filterByEntityAndPropertyComplete(entityTokenIdsInts, propertyKeyIds)),
                        actual);
            }
        }
    }

    private static Predicate<SchemaDescriptorSupplier> filterByEntityAndPropertyComplete(
            int[] entityTokenIds, int[] propertyKeyIds) {
        return holder -> {
            IntPredicate propertyKeyPredicate = indexPropertyId -> contains(propertyKeyIds, indexPropertyId);
            boolean propertiesAccepted = holder.schema().schemaPatternMatchingType() == COMPLETE_ALL_TOKENS
                    // For typical indexes (COMPLETE_ALL_TOKENS) all must match
                    ? stream(holder.schema().getPropertyIds()).allMatch(propertyKeyPredicate)
                    // For multi-token (e.g. full-text) descriptors any property key match is to be considered a match
                    : stream(holder.schema().getPropertyIds()).anyMatch(propertyKeyPredicate);
            return stream(holder.schema().getEntityTokenIds())
                            .anyMatch(indexEntityId -> contains(entityTokenIds, indexEntityId))
                    && propertiesAccepted;
        };
    }

    private static Predicate<SchemaDescriptorSupplier> filterByEntityAndPropertyPartial(
            int[] entityTokenIds, int[] propertyKeyIds) {
        return holder -> stream(holder.schema().getEntityTokenIds())
                        .anyMatch(indexEntityId -> contains(entityTokenIds, indexEntityId))
                && stream(holder.schema().getPropertyIds())
                        .anyMatch(indexPropertyId -> contains(propertyKeyIds, indexPropertyId));
    }

    private static Predicate<SchemaDescriptorSupplier> filterByEntity(int[] entityTokenIds) {
        return holder -> stream(holder.schema().getEntityTokenIds())
                .anyMatch(indexEntityId -> contains(entityTokenIds, indexEntityId));
    }

    private static Set<SchemaDescriptorSupplier> expectedDescriptors(
            List<SchemaDescriptorSupplier> all, Predicate<SchemaDescriptorSupplier> filter) {
        return asSet(Iterators.filter(filter, all.iterator()));
    }

    private SchemaDescriptor randomSchemaDescriptor(
            int highEntityKeyId, int highPropertyKeyId, int maxNumberOfEntityKeys, int maxNumberOfPropertyKeys) {
        int numberOfEntityKeys = random.nextInt(1, maxNumberOfEntityKeys);
        int[] entityKeys = randomUniqueUnsortedIntArray(highEntityKeyId, numberOfEntityKeys);
        int numberOfPropertyKeys = random.nextInt(1, maxNumberOfPropertyKeys);
        int[] propertyKeys = randomUniqueUnsortedIntArray(highPropertyKeyId, numberOfPropertyKeys);
        return entityKeys.length > 1
                ? SchemaDescriptors.fulltext(EntityType.NODE, entityKeys, propertyKeys)
                : SchemaDescriptors.forLabel(entityKeys[0], propertyKeys);
    }

    private int[] randomUniqueUnsortedIntArray(int maxValue, int length) {
        int[] array = new int[length];
        MutableIntSet seen = IntSets.mutable.empty();
        for (int i = 0; i < length; i++) {
            int candidate;
            do {
                candidate = random.nextInt(maxValue);
            } while (!seen.add(candidate));
            array[i] = candidate;
        }
        return array;
    }

    private int[] randomUniqueSortedIntArray(int maxValue, int length) {
        int[] array = randomUniqueUnsortedIntArray(maxValue, length);
        Arrays.sort(array);
        return array;
    }

    private static int[] properties(int... properties) {
        return properties;
    }

    private static int[] entityTokens(int... labels) {
        return labels;
    }

    private static SchemaDescriptorSupplier of(SchemaDescriptor schema) {
        return new TestSchemaDescriptorSupplier(schema);
    }

    private static class TestSchemaDescriptorSupplier implements SchemaDescriptorSupplier {
        private final SchemaDescriptor schema;

        TestSchemaDescriptorSupplier(SchemaDescriptor schema) {
            this.schema = schema;
        }

        @Override
        public SchemaDescriptor schema() {
            return schema;
        }

        @Override
        public String userDescription(TokenNameLookup tokenNameLookup) {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            return schema.equals(((TestSchemaDescriptorSupplier) o).schema);
        }

        @Override
        public int hashCode() {
            return schema.hashCode();
        }
    }
}
