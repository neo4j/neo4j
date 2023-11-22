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

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.schema.SchemaCache.NO_LOGICAL_KEYS;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;
import static org.neo4j.internal.schema.SchemaDescriptors.fulltext;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.keyForSchema;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.test.Race;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.ValueCategory;

class SchemaCacheTest {

    private final SchemaRule hans = newIndexRule(1, 0, 5);
    private final SchemaRule witch = nodePropertyExistenceConstraint(2, 3, 6);
    private final SchemaRule gretel = newIndexRule(3, 0, 7);
    private final ConstraintDescriptor robot = relPropertyExistenceConstraint(7L, 8, 9);
    private final IndexConfigCompleter indexConfigCompleter = (index, indexingBehaviour) -> index;
    private static final int[] noEntityToken = EMPTY_INT_ARRAY;

    // For "related to" tests
    private final IndexDescriptor schema3_4 = newIndexRule(10, 3, 4);
    private final IndexDescriptor schema5_6_7 = newIndexRule(11, 5, 6, 7);
    private final IndexDescriptor schema5_8 = newIndexRule(12, 5, 8);
    private final IndexDescriptor node35_8 = IndexPrototype.forSchema(fulltext(NODE, new int[] {3, 5}, new int[] {8}))
            .withName("index_13")
            .materialise(13);
    private final IndexDescriptor rel35_8 = IndexPrototype.forSchema(
                    fulltext(RELATIONSHIP, new int[] {3, 5}, new int[] {8}))
            .withName("index_14")
            .materialise(14);

    @Test
    void shouldConstructSchemaCache() {
        // GIVEN
        SchemaCache cache = newSchemaCache(hans, witch, gretel, robot);

        // THEN
        assertEquals(asSet(hans, gretel), Iterables.asSet(cache.indexes()));
        assertEquals(asSet(witch, robot), Iterables.asSet(cache.constraints()));
    }

    @Test
    void addRemoveIndexes() {
        SchemaCache cache = newSchemaCache(hans, witch, gretel, robot);

        IndexDescriptor rule1 = newIndexRule(10, 11, 12);
        IndexDescriptor rule2 = newIndexRule(13, 14, 15);
        cache.addSchemaRule(rule1);
        cache.addSchemaRule(rule2);

        cache.removeSchemaRule(hans.getId());
        cache.removeSchemaRule(witch.getId());

        assertEquals(asSet(gretel, rule1, rule2), Iterables.asSet(cache.indexes()));
        assertEquals(asSet(robot), Iterables.asSet(cache.constraints()));
    }

    @Test
    void addSchemaRules() {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        // WHEN
        cache.addSchemaRule(hans);
        cache.addSchemaRule(gretel);
        cache.addSchemaRule(witch);
        cache.addSchemaRule(robot);

        // THEN
        assertEquals(asSet(hans, gretel), Iterables.asSet(cache.indexes()));
        assertEquals(asSet(witch, robot), Iterables.asSet(cache.constraints()));
    }

    @Test
    void shouldListConstraints() {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        // WHEN
        cache.addSchemaRule(uniquenessConstraint(0L, 1, 2, 133L));
        cache.addSchemaRule(uniquenessConstraint(1L, 3, 4, 133L));
        cache.addSchemaRule(relPropertyExistenceConstraint(2L, 5, 6));
        cache.addSchemaRule(nodePropertyExistenceConstraint(3L, 7, 8));

        // THEN
        ConstraintDescriptor unique1 = uniqueForLabel(1, 2);
        ConstraintDescriptor unique2 = uniqueForLabel(3, 4);
        ConstraintDescriptor existsRel = ConstraintDescriptorFactory.existsForRelType(5, 6);
        ConstraintDescriptor existsNode = existsForLabel(7, 8);

        assertEquals(asSet(unique1, unique2, existsRel, existsNode), Iterables.asSet(cache.constraints()));

        assertEquals(asSet(unique1), asSet(cache.constraintsForLabel(1)));

        assertEquals(asSet(unique1), asSet(cache.constraintsForSchema(unique1.schema())));

        assertEquals(asSet(), asSet(cache.constraintsForSchema(forLabel(1, 3))));

        assertEquals(asSet(existsRel), asSet(cache.constraintsForRelationshipType(5)));
    }

    @Test
    void shouldRemoveConstraints() {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule(uniquenessConstraint(0L, 1, 2, 133L));
        cache.addSchemaRule(uniquenessConstraint(1L, 3, 4, 133L));

        // WHEN
        cache.removeSchemaRule(0L);

        // THEN
        ConstraintDescriptor dropped = uniqueForLabel(1, 1);
        ConstraintDescriptor unique = uniqueForLabel(3, 4);
        assertEquals(asSet(unique), Iterables.asSet(cache.constraints()));

        assertEquals(asSet(), asSet(cache.constraintsForLabel(1)));

        assertEquals(asSet(), asSet(cache.constraintsForSchema(dropped.schema())));
    }

    @Test
    void addingConstraintsShouldBeIdempotent() {
        // given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule(uniquenessConstraint(0L, 1, 2, 133L));

        // when
        cache.addSchemaRule(uniquenessConstraint(0L, 1, 2, 133L));

        // then
        assertEquals(Collections.singletonList(uniqueForLabel(1, 2)), Iterables.asList(cache.constraints()));
    }

    @Test
    void shouldResolveIndexDescriptor() {
        // Given
        SchemaCache cache = newSchemaCache();

        IndexDescriptor expected;
        cache.addSchemaRule(newIndexRule(1L, 1, 2));
        cache.addSchemaRule(expected = newIndexRule(2L, 1, 3));
        cache.addSchemaRule(newIndexRule(3L, 2, 2));

        // When
        IndexDescriptor actual = single(cache.indexesForSchema(forLabel(1, 3)));

        // Then
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void shouldResolveIndexDescriptorBySchemaAndType() {
        // Given
        SchemaCache cache = newSchemaCache();

        IndexDescriptor expected;
        cache.addSchemaRule(newIndexRule(1L, 1, 2));
        cache.addSchemaRule(expected = newIndexRule(2L, IndexType.TEXT, 1, 3));
        cache.addSchemaRule(newIndexRule(3L, 2, 2));

        // When
        IndexDescriptor actual = cache.indexForSchemaAndType(forLabel(1, 3), IndexType.TEXT);
        IndexDescriptor wrongType = cache.indexForSchemaAndType(forLabel(1, 3), IndexType.RANGE);

        // Then
        assertThat(actual).isEqualTo(expected);
        assertThat(wrongType).isNull();
    }

    @Test
    void shouldResolveMultipleIndexDescriptorsForSameSchema() {
        // Given
        SchemaCache cache = newSchemaCache();

        IndexDescriptor expected;
        IndexDescriptor expected2;
        cache.addSchemaRule(newIndexRule(1L, 1, 2));
        cache.addSchemaRule(expected = newIndexRule(2L, IndexType.RANGE, 1, 3));
        cache.addSchemaRule(expected2 = newIndexRule(3L, IndexType.TEXT, 1, 3));
        cache.addSchemaRule(newIndexRule(4L, 2, 2));

        // When
        var actual = cache.indexForSchemaAndType(forLabel(1, 3), IndexType.RANGE);
        var actual2 = cache.indexForSchemaAndType(forLabel(1, 3), IndexType.TEXT);

        // Then
        assertThat(actual).isEqualTo(expected);
        assertThat(actual2).isEqualTo(expected2);
    }

    @Test
    void shouldStoreMultipleIndexDescriptorsForSameSchema() {
        // Given
        SchemaCache cache = newSchemaCache();

        IndexDescriptor expected;
        IndexDescriptor expected2;
        cache.addSchemaRule(newIndexRule(1L, 1, 2));
        cache.addSchemaRule(expected = newIndexRule(2L, IndexType.RANGE, 1, 3));
        cache.addSchemaRule(expected2 = newIndexRule(3L, IndexType.TEXT, 1, 3));
        cache.addSchemaRule(newIndexRule(4L, 2, 2));

        // When
        var actual = cache.indexesForSchema(forLabel(1, 3));

        // Then
        assertThat(actual).toIterable().containsExactlyInAnyOrder(expected, expected2);
    }

    @Test
    void shouldStoreMultipleConstraintDescriptorsForSameSchema() {
        // Given
        SchemaCache cache = newSchemaCache();

        ConstraintDescriptor expected;
        ConstraintDescriptor expected2;
        cache.addSchemaRule(uniquenessConstraint(1L, 1, 2, 3));
        cache.addSchemaRule(expected = uniquenessConstraint(2L, 1, 3, 4, IndexType.TEXT));
        cache.addSchemaRule(expected2 = uniquenessConstraint(3L, 1, 3, 5, IndexType.RANGE));
        cache.addSchemaRule(uniquenessConstraint(4L, 1, 2, 6));

        // When
        var actual = cache.constraintsForSchema(forLabel(1, 3));

        // Then
        assertThat(actual).toIterable().containsExactlyInAnyOrder(expected, expected2);
    }

    @Test
    void shouldReturnImmutableIteratorFromIndexesForSchema() {
        // Given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule(newIndexRule(2L, IndexType.RANGE, 1, 3));
        cache.addSchemaRule(newIndexRule(3L, IndexType.TEXT, 1, 3));

        // When
        var actual = cache.indexesForSchema(forLabel(1, 3));
        actual.next();

        // Then
        assertThatThrownBy(actual::remove).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldRelpaceEquialentIndex() {
        // Given
        SchemaCache cache = newSchemaCache();

        var schema = forLabel(1, 3);
        var index = IndexPrototype.uniqueForSchema(schema)
                .withName("index_id")
                .withIndexType(IndexType.TEXT)
                .materialise(2L);
        cache.addSchemaRule(index);
        var updated = index.withOwningConstraintId(6); // same index, different owing constraint id should be replacable
        cache.addSchemaRule(updated);

        assertThat(cache.indexesForSchema(schema)).toIterable().containsOnly(updated);
        assertThat(cache.indexForSchemaAndType(schema, IndexType.TEXT)).isEqualTo(updated);
    }

    @Test
    void shouldRemoveWhenMultipeIndexDescriptorsForSameSchema() {
        // Given
        final var label = 2;
        SchemaCache cache = newSchemaCache();

        IndexDescriptor expected;
        IndexDescriptor expected2;
        cache.addSchemaRule(newIndexRule(1L, label, 2));
        cache.addSchemaRule(expected = newIndexRule(2L, IndexType.RANGE, label, 3));
        cache.addSchemaRule(expected2 = newIndexRule(3L, IndexType.TEXT, label, 3));
        cache.addSchemaRule(newIndexRule(4L, label + 1, 2));

        cache.removeSchemaRule(expected.getId());
        // When

        // Then
        var schema = forLabel(label, 3);
        assertThat(cache.indexesForSchema(schema)).toIterable().containsExactlyInAnyOrder(expected2);
        assertThat(cache.indexForSchemaAndType(schema, IndexType.TEXT)).isEqualTo(expected2);
    }

    @Test
    void schemaCacheSnapshotsShouldBeReadOnly() {
        // Given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule(newIndexRule(1L, 1, 2));
        cache.addSchemaRule(newIndexRule(2L, 2, 3));

        SchemaCache snapshot = cache.snapshot();

        cache.addSchemaRule(newIndexRule(3L, 1, 4));

        // When
        Set<IndexDescriptor> indexes = asSet(snapshot.indexesForLabel(1));

        // Then
        Set<IndexDescriptor> expected = asSet(newIndexRule(1L, 1, 2));
        assertEquals(expected, indexes);

        assertThatThrownBy(() -> snapshot.addSchemaRule(newIndexRule(3L, 1, 4)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Schema cache snapshots are read-only");
    }

    @Test
    void shouldReturnEmptyWhenNoIndexExists() {
        // Given
        SchemaCache schemaCache = newSchemaCache();

        // When
        Iterator<IndexDescriptor> iterator = schemaCache.indexesForSchema(forLabel(1, 1));

        // Then
        assertFalse(iterator.hasNext());
    }

    @Test
    void shouldListConstraintsForLabel() {
        // Given
        ConstraintDescriptor rule1 = uniquenessConstraint(0, 1, 1, 0);
        ConstraintDescriptor rule2 = uniquenessConstraint(1, 2, 1, 0);
        ConstraintDescriptor rule3 = nodePropertyExistenceConstraint(2, 1, 2);

        SchemaCache cache = newSchemaCache();
        cache.addSchemaRule(rule1);
        cache.addSchemaRule(rule2);
        cache.addSchemaRule(rule3);

        // When
        Set<ConstraintDescriptor> listed = asSet(cache.constraintsForLabel(1));

        // Then
        Set<ConstraintDescriptor> expected = asSet(rule1, rule3);
        assertEquals(expected, listed);
    }

    @Test
    void shouldListConstraintsForSchema() {
        // Given
        ConstraintDescriptor rule1 = uniquenessConstraint(0, 1, 1, 0);
        ConstraintDescriptor rule2 = uniquenessConstraint(1, 2, 1, 0);
        ConstraintDescriptor rule3 = nodePropertyExistenceConstraint(2, 1, 2);

        SchemaCache cache = newSchemaCache();
        cache.addSchemaRule(rule1);
        cache.addSchemaRule(rule2);
        cache.addSchemaRule(rule3);

        // When
        Set<ConstraintDescriptor> listed = asSet(cache.constraintsForSchema(rule3.schema()));

        // Then
        assertEquals(singleton(rule3), listed);
    }

    @Test
    void shouldListConstraintsForRelationshipType() {
        // Given
        ConstraintDescriptor rule1 = relPropertyExistenceConstraint(0, 1, 1);
        ConstraintDescriptor rule2 = relPropertyExistenceConstraint(1, 2, 1);
        ConstraintDescriptor rule3 = relPropertyExistenceConstraint(2, 1, 2);

        SchemaCache cache = newSchemaCache();
        cache.addSchemaRule(rule1);
        cache.addSchemaRule(rule2);
        cache.addSchemaRule(rule3);

        // When
        Set<ConstraintDescriptor> listed = asSet(cache.constraintsForRelationshipType(1));

        // Then
        Set<ConstraintDescriptor> expected = asSet(rule1, rule3);
        assertEquals(expected, listed);
    }

    @Test
    void concurrentSchemaRuleAdd() throws Throwable {
        SchemaCache cache = newSchemaCache();
        Race race = new Race();
        int indexNumber = 10;
        for (int i = 0; i < indexNumber; i++) {
            int id = i;
            race.addContestant(() -> cache.addSchemaRule(newIndexRule(id, id, id)));
        }
        race.go();

        assertEquals(indexNumber, Iterables.count(cache.indexes()));
        for (int labelId = 0; labelId < indexNumber; labelId++) {
            assertEquals(1, Iterators.count(cache.indexesForLabel(labelId)));
        }
    }

    @Test
    void concurrentSchemaRuleRemove() throws Throwable {
        SchemaCache cache = newSchemaCache();
        int indexNumber = 20;
        for (int i = 0; i < indexNumber; i++) {
            cache.addSchemaRule(newIndexRule(i, i, i));
        }
        Race race = new Race();
        int numberOfDeletions = 10;
        for (int i = 0; i < numberOfDeletions; i++) {
            int indexId = i;
            race.addContestant(() -> cache.removeSchemaRule(indexId));
        }
        race.go();

        assertEquals(indexNumber - numberOfDeletions, Iterables.count(cache.indexes()));
        for (int labelId = numberOfDeletions; labelId < indexNumber; labelId++) {
            assertEquals(1, Iterators.count(cache.indexesForLabel(labelId)));
        }
    }

    @Test
    void removeSchemaWithRepeatedLabel() {
        final SchemaCache cache = newSchemaCache();

        final int id = 1;
        final int[] repeatedLabels = {0, 1, 0};
        final FulltextSchemaDescriptor schema = fulltext(NODE, repeatedLabels, new int[] {1});
        IndexDescriptor index = newIndexRule(schema, id);
        cache.addSchemaRule(index);
        cache.removeSchemaRule(id);
    }

    @Test
    void removeSchemaWithRepeatedRelType() {
        final SchemaCache cache = newSchemaCache();

        final int id = 1;
        final int[] repeatedRelTypes = {0, 1, 0};
        final FulltextSchemaDescriptor schema = fulltext(RELATIONSHIP, repeatedRelTypes, new int[] {1});
        IndexDescriptor index = newIndexRule(schema, id);
        cache.addSchemaRule(index);
        cache.removeSchemaRule(id);
    }

    @Test
    void shouldGetRelatedIndexForLabel() {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(cache.getValueIndexesRelatedTo(entityTokens(3), noEntityToken, properties(), false, NODE))
                .contains(schema3_4, node35_8);
    }

    @Test
    void shouldGetRelatedIndexForProperty() {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(cache.getValueIndexesRelatedTo(noEntityToken, entityTokens(3, 4, 5), properties(4), false, NODE))
                .contains(schema3_4);
    }

    @Test
    void shouldGetRelatedIndexesForLabel() {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(cache.getValueIndexesRelatedTo(entityTokens(5), entityTokens(3, 4), properties(), false, NODE))
                .contains(schema5_6_7, schema5_8, node35_8);
    }

    @Test
    void shouldGetRelatedIndexes() {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(cache.getValueIndexesRelatedTo(entityTokens(3), entityTokens(4, 5), properties(7), false, NODE))
                .contains(schema3_4, schema5_6_7, node35_8);
    }

    @Test
    void shouldGetRelatedIndexOnce() {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(cache.getValueIndexesRelatedTo(entityTokens(3), noEntityToken, properties(4), false, NODE))
                .contains(schema3_4, node35_8);

        assertThat(cache.getValueIndexesRelatedTo(noEntityToken, entityTokens(5), properties(6, 7), false, NODE))
                .contains(schema5_6_7);
    }

    @Test
    void shouldHandleUnrelated() {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(cache.getValueIndexesRelatedTo(noEntityToken, noEntityToken, properties(), false, NODE))
                .isEmpty();

        assertTrue(cache.getValueIndexesRelatedTo(entityTokens(2), noEntityToken, properties(), false, NODE)
                .isEmpty());

        assertThat(cache.getValueIndexesRelatedTo(noEntityToken, entityTokens(2), properties(1), false, NODE))
                .isEmpty();

        assertTrue(cache.getValueIndexesRelatedTo(entityTokens(2), entityTokens(2), properties(1), false, NODE)
                .isEmpty());
    }

    @Test
    void shouldGetMultiLabelForAnyOfTheLabels() {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(cache.getValueIndexesRelatedTo(entityTokens(3), noEntityToken, properties(), false, NODE))
                .contains(schema3_4, node35_8);

        assertThat(cache.getValueIndexesRelatedTo(entityTokens(5), noEntityToken, properties(), false, NODE))
                .contains(schema5_8, schema5_6_7, node35_8);
    }

    @Test
    void shouldOnlyGetRelIndexesForRelUpdates() {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(cache.getValueIndexesRelatedTo(entityTokens(3), noEntityToken, properties(), false, RELATIONSHIP))
                .contains(rel35_8);

        assertThat(cache.getValueIndexesRelatedTo(entityTokens(5), noEntityToken, properties(), false, RELATIONSHIP))
                .contains(rel35_8);
    }

    @Test
    void removalsShouldOnlyRemoveCorrectProxy() {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        cache.removeSchemaRule(node35_8.getId());
        assertThat(cache.getValueIndexesRelatedTo(entityTokens(3), noEntityToken, properties(), false, NODE))
                .contains(schema3_4);
        assertThat(cache.getValueIndexesRelatedTo(entityTokens(3), noEntityToken, properties(), false, RELATIONSHIP))
                .contains(rel35_8);

        cache.removeSchemaRule(7);
        assertThat(cache.getValueIndexesRelatedTo(entityTokens(5), noEntityToken, properties(), false, NODE))
                .contains(schema5_8, schema5_6_7);
        assertThat(cache.getValueIndexesRelatedTo(entityTokens(5), noEntityToken, properties(), false, RELATIONSHIP))
                .contains(rel35_8);
    }

    @Test
    void shouldGetRelatedNodeConstraints() {
        // given
        SchemaCache cache =
                new SchemaCache(new ConstraintSemantics(), indexConfigCompleter, StorageEngineIndexingBehaviour.EMPTY);
        ConstraintDescriptor constraint1 =
                ConstraintDescriptorFactory.uniqueForLabel(1, 5, 6).withId(1);
        ConstraintDescriptor constraint2 =
                ConstraintDescriptorFactory.uniqueForLabel(1, 5).withId(2);
        ConstraintDescriptor constraint3 =
                ConstraintDescriptorFactory.uniqueForLabel(2, 5).withId(3);
        cache.addSchemaRule(constraint1);
        cache.addSchemaRule(constraint2);
        cache.addSchemaRule(constraint3);

        // when/then
        assertEquals(
                asSet(constraint2),
                cache.getUniquenessConstraintsRelatedTo(entityTokens(1), entityTokens(), properties(5), true, NODE));
        assertEquals(
                asSet(constraint1, constraint2),
                cache.getUniquenessConstraintsRelatedTo(entityTokens(1), entityTokens(), properties(5), false, NODE));
        assertEquals(
                asSet(constraint1, constraint2),
                cache.getUniquenessConstraintsRelatedTo(entityTokens(1), entityTokens(), properties(5, 6), true, NODE));
        assertEquals(
                asSet(constraint1, constraint2),
                cache.getUniquenessConstraintsRelatedTo(entityTokens(), entityTokens(1), properties(5), false, NODE));
        assertEquals(
                asSet(constraint1, constraint2, constraint3),
                cache.getUniquenessConstraintsRelatedTo(entityTokens(1, 2), entityTokens(), properties(), false, NODE));
    }

    @Test
    void shouldRemoveNodeConstraints() {
        // given
        SchemaCache cache =
                new SchemaCache(new ConstraintSemantics(), indexConfigCompleter, StorageEngineIndexingBehaviour.EMPTY);
        ConstraintDescriptor constraint1 =
                ConstraintDescriptorFactory.uniqueForLabel(1, 5, 6).withId(1);
        ConstraintDescriptor constraint2 =
                ConstraintDescriptorFactory.uniqueForLabel(1, 5).withId(2);
        ConstraintDescriptor constraint3 =
                ConstraintDescriptorFactory.uniqueForLabel(2, 5).withId(3);
        cache.addSchemaRule(constraint1);
        cache.addSchemaRule(constraint2);
        cache.addSchemaRule(constraint3);
        assertEquals(
                asSet(constraint2),
                cache.getUniquenessConstraintsRelatedTo(entityTokens(1), entityTokens(), properties(5), true, NODE));

        // and when
        cache.removeSchemaRule(constraint1.getId());
        cache.removeSchemaRule(constraint2.getId());
        cache.removeSchemaRule(constraint3.getId());

        // then
        assertTrue(cache.getUniquenessConstraintsRelatedTo(entityTokens(1), entityTokens(), properties(5), true, NODE)
                .isEmpty());
    }

    @Test
    void shouldCompleteConfigurationOfIndexesAddedToCache() {
        IndexCapability capability = new IndexCapability() {
            @Override
            public boolean supportsOrdering() {
                return false;
            }

            @Override
            public boolean supportsReturningValues() {
                return false;
            }

            @Override
            public boolean areValueCategoriesAccepted(ValueCategory... valueCategories) {
                Preconditions.requireNonEmpty(valueCategories);
                Preconditions.requireNoNullElements(valueCategories);
                return true;
            }

            @Override
            public boolean isQuerySupported(IndexQueryType queryType, ValueCategory valueCategory) {
                return true;
            }

            @Override
            public double getCostMultiplier(IndexQueryType... queryTypes) {
                return 1.0;
            }

            @Override
            public boolean supportPartitionedScan(IndexQuery... queries) {
                Preconditions.requireNonEmpty(queries);
                Preconditions.requireNoNullElements(queries);
                return false;
            }
        };
        List<IndexDescriptor> completed = new ArrayList<>();
        IndexConfigCompleter completer = (index, indexingBehaviour) -> {
            completed.add(index);
            return index.withIndexCapability(capability);
        };
        SchemaCache cache = new SchemaCache(new ConstraintSemantics(), completer, StorageEngineIndexingBehaviour.EMPTY);

        IndexDescriptor index1 = newIndexRule(1, 2, 3);
        ConstraintDescriptor constraint1 = uniquenessConstraint(2, 2, 3, 1);
        IndexDescriptor index2 = newIndexRule(3, 4, 5);
        ConstraintDescriptor constraint2 = uniquenessConstraint(4, 4, 5, 3);
        IndexDescriptor index3 = newIndexRule(5, 5, 5);

        cache.load(asList(index1, constraint1));
        cache.addSchemaRule(index2);
        cache.addSchemaRule(constraint2);
        cache.addSchemaRule(index3);

        assertEquals(List.of(index1, index2, index3), completed);
        assertEquals(capability, cache.getIndex(index1.getId()).getCapability());
        assertEquals(capability, cache.getIndex(index2.getId()).getCapability());
        assertEquals(capability, cache.getIndex(index3.getId()).getCapability());
    }

    @Test
    void shouldHaveAddedConstraintsAndIndexes() {
        long constraintId = 1;
        long indexId = 4;
        IndexDescriptor index = newIndexRule(indexId, 2, 3);
        ConstraintDescriptor constraint = uniquenessConstraint(constraintId, 2, 3, indexId);
        SchemaCache cache = newSchemaCache(index, constraint);
        assertTrue(cache.hasConstraintRule(constraintId));
        assertTrue(cache.hasConstraintRule(constraint));
        assertFalse(cache.hasConstraintRule(indexId));
        assertTrue(cache.hasIndex(index));
    }

    @Test
    void hasConstraintRuleShouldMatchBySchemaAndTypeAndIndexType() {
        ConstraintDescriptor existing = uniquenessConstraint(1, 2, 3, 4, IndexType.RANGE);
        // Different rule id, but same type, schema and index type.
        ConstraintDescriptor checked = uniquenessConstraint(0, 2, 3, 4, IndexType.RANGE);
        SchemaCache cache = newSchemaCache(existing);
        assertTrue(cache.hasConstraintRule(checked));
    }

    @Test
    void hasConstraintRuleShouldNotMatchOnDifferentIndexType() {
        ConstraintDescriptor existing = uniquenessConstraint(1, 2, 4, 5, IndexType.TEXT);
        // Different index type.
        ConstraintDescriptor checked = uniquenessConstraint(0, 2, 4, 5, IndexType.RANGE);
        SchemaCache cache = newSchemaCache(existing);
        assertFalse(cache.hasConstraintRule(checked));
    }

    @Test
    void shouldCacheDependentState() {
        SchemaCache cache = newSchemaCache();
        MutableInt mint = cache.getOrCreateDependantState(MutableInt.class, MutableInt::new, 1);
        assertEquals(1, mint.getValue());
        mint.setValue(2);
        mint = cache.getOrCreateDependantState(MutableInt.class, MutableInt::new, 1);
        assertEquals(2, mint.getValue());
    }

    @Test
    void shouldFindIndexDescriptorsByRelationshipType() {
        IndexDescriptor first =
                IndexPrototype.forSchema(forRelType(2, 3)).withName("index_1").materialise(1);
        IndexDescriptor second =
                IndexPrototype.forSchema(forLabel(2, 3)).withName("index_2").materialise(2);
        SchemaCache cache = newSchemaCache(first, second);
        assertEquals(first, single(cache.indexesForRelationshipType(2)));
        assertEquals(first.getId(), single(cache.indexesForRelationshipType(2)).getId());
    }

    @Test
    void shouldFindIndexDescriptorsByIndexName() {
        IndexDescriptor index =
                IndexPrototype.forSchema(forLabel(2, 3)).withName("index name").materialise(1);
        SchemaCache cache = newSchemaCache(index);
        assertEquals(index, cache.indexForName("index name"));
        cache.removeSchemaRule(index.getId());
        assertNull(cache.indexForName("index name"));
    }

    @Test
    void shouldFindConstraintByName() {
        ConstraintDescriptor constraint =
                nodePropertyExistenceConstraint(1, 2, 3).withName("constraint name");
        SchemaCache cache = newSchemaCache(constraint);
        assertEquals(constraint, cache.constraintForName("constraint name"));
        cache.removeSchemaRule(constraint.getId());
        assertNull(cache.constraintForName("constraint name"));
    }

    @Test
    void shouldFindConstraintAndIndexByName() {
        IndexDescriptor index = IndexPrototype.uniqueForSchema(forLabel(2, 3))
                .withName("schema name")
                .materialise(1);
        ConstraintDescriptor constraint = uniquenessConstraint(4, 2, 3, 1).withName("schema name");
        SchemaCache cache = newSchemaCache(index, constraint);
        assertEquals(index, cache.indexForName("schema name"));
        assertEquals(constraint, cache.constraintForName("schema name"));
    }

    @Test
    void logicalKeyConstraintsForNodeKey() {
        final var schemaName = "schema name";
        final var labelId = 2;
        final var propertyId = 3;
        final var schema = forLabel(labelId, propertyId);
        final var index =
                IndexPrototype.uniqueForSchema(schema).withName(schemaName).materialise(1);
        final var constraint =
                keyForSchema(schema).withId(42).withOwnedIndexId(index.getId()).withName(schemaName);
        final var cache = newSchemaCache(index, constraint);
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(labelId, NODE))
                .as("should find the property that makes up the logical key for the node/label")
                .isEqualTo(logicalKeys(propertyId));
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(labelId + 1, NODE))
                .as("should NOT find any logical key for the node/label")
                .isEqualTo(NO_LOGICAL_KEYS);
    }

    @Test
    void logicalKeysWithMultipleConstraintsForNodeKey() {
        final var labelId = 2;
        final var propertyId1 = 3;
        final var propertyId2 = 4;
        final var propertyId3 = 5;
        final var schema1 = forLabel(labelId, propertyId1);
        final var schema2 = forLabel(labelId, propertyId2, propertyId3);

        final var index1 =
                IndexPrototype.uniqueForSchema(schema1).withName("i1").materialise(1);
        final var index2 =
                IndexPrototype.uniqueForSchema(schema2).withName("i2").materialise(2);

        final var c1 = keyForSchema(schema1).withName("c1").withId(42).withOwnedIndexId(index1.getId());
        final var c2 = keyForSchema(schema2).withName("c2").withId(43).withOwnedIndexId(index2.getId());

        final var expected = new org.eclipse.collections.api.set.primitive.ImmutableIntSet[] {
            IntSets.immutable.of(propertyId2, propertyId3), IntSets.immutable.of(propertyId1)
        };

        assertThat(newSchemaCache(index1, c1, c2).constraintsGetPropertyTokensForLogicalKey(labelId, NODE))
                .as("should find the properties that makes up the logical keys for the node/label")
                .isEqualTo(expected);
    }

    @Test
    void logicalKeyConstraintsForNodeKeyWithMultipleProperties() {
        final var schemaName = "schema name";
        final var labelId = 2;
        final var propertyId1 = 3;
        final var propertyId2 = 4;
        final var schema = forLabel(labelId, propertyId1, propertyId2);
        final var index =
                IndexPrototype.uniqueForSchema(schema).withName(schemaName).materialise(1);
        final var constraint =
                keyForSchema(schema).withId(42).withOwnedIndexId(index.getId()).withName(schemaName);
        final var cache = newSchemaCache(index, constraint);
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(labelId, NODE))
                .as("should find the properties that makes up the logical key for the node/label")
                .isEqualTo(logicalKeys(propertyId1, propertyId2));
    }

    @Test
    void logicalKeyConstraintsForRelationshipKey() {
        final var schemaName = "schema name";
        final var relType = 2;
        final var propertyId = 3;
        final var schema = forRelType(relType, propertyId);
        final var index =
                IndexPrototype.uniqueForSchema(schema).withName(schemaName).materialise(1);
        final var constraint =
                keyForSchema(schema).withId(42).withOwnedIndexId(index.getId()).withName(schemaName);

        final var cache = newSchemaCache(index, constraint);
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(relType, RELATIONSHIP))
                .as("should find the property that makes up the logical key for the relationship")
                .isEqualTo(logicalKeys(propertyId));
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(relType + 1, RELATIONSHIP))
                .as("should NOT find any logical key for the relationship")
                .isEqualTo(NO_LOGICAL_KEYS);
    }

    @Test
    void logicalKeysWithMultipleConstraintsForRelationshipKey() {
        final var relType = 2;
        final var propertyId1 = 3;
        final var propertyId2 = 4;
        final var propertyId3 = 5;
        final var schema1 = forRelType(relType, propertyId1);
        final var schema2 = forRelType(relType, propertyId2, propertyId3);

        final var index1 =
                IndexPrototype.uniqueForSchema(schema1).withName("i1").materialise(1);
        final var index2 =
                IndexPrototype.uniqueForSchema(schema2).withName("i2").materialise(2);

        final var c1 = keyForSchema(schema1).withName("c1").withId(42).withOwnedIndexId(index1.getId());
        final var c2 = keyForSchema(schema2).withName("c2").withId(43).withOwnedIndexId(index2.getId());

        final var expected = new org.eclipse.collections.api.set.primitive.ImmutableIntSet[] {
            IntSets.immutable.of(propertyId2, propertyId3), IntSets.immutable.of(propertyId1)
        };

        assertThat(newSchemaCache(index1, c1, c2).constraintsGetPropertyTokensForLogicalKey(relType, RELATIONSHIP))
                .as("should find the properties that makes up the logical keys for the relationship")
                .isEqualTo(expected);
    }

    @Test
    void logicalKeyConstraintsForRelationshipKeyWithMultipleProperties() {
        final var schemaName = "schema name";
        final var relType = 2;
        final var propertyId1 = 3;
        final var propertyId2 = 4;
        final var schema = forRelType(relType, propertyId1, propertyId2);
        final var index =
                IndexPrototype.uniqueForSchema(schema).withName(schemaName).materialise(1);
        final var constraint =
                keyForSchema(schema).withId(42).withOwnedIndexId(index.getId()).withName(schemaName);

        final var cache = newSchemaCache(index, constraint);
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(relType, RELATIONSHIP))
                .as("should find the property that makes up the logical key for the relationship")
                .isEqualTo(logicalKeys(propertyId1, propertyId2));
    }

    @Test
    void logicalKeyConstraintsForNodeInSeparateSchemaRules() {
        final var schemaName = "schema name";
        final var labelId = 2;
        final var propertyId1 = 3;
        final var propertyId2 = 4;

        final var schema = forLabel(labelId, propertyId1, propertyId2);
        final var index =
                IndexPrototype.uniqueForSchema(schema).withName(schemaName).materialise(1);

        final var uniquenessConstraint = uniqueForSchema(schema).withId(42).withOwnedIndexId(index.getId());

        final var cache = newSchemaCache(index, uniquenessConstraint);
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(labelId, NODE))
                .as("should not find the logical key for the node/label yet")
                .isEqualTo(NO_LOGICAL_KEYS);

        final var existenceConstraint = nodePropertyExistenceConstraint(43, labelId, propertyId1);
        cache.addSchemaRule(existenceConstraint);
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(labelId, NODE))
                .as("not all properties exist to make up the logical key for the node/label")
                .isEqualTo(NO_LOGICAL_KEYS);

        cache.addSchemaRule(nodePropertyExistenceConstraint(44, labelId, propertyId2));
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(labelId, NODE))
                .as("All properties exist that make up the logical key for the node/label")
                .isEqualTo(logicalKeys(propertyId1, propertyId2));

        cache.removeSchemaRule(existenceConstraint.getId());
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(labelId, NODE))
                .as("logical key requires both the uniqueness and existence constraints")
                .isEqualTo(NO_LOGICAL_KEYS);
    }

    @Test
    void logicalKeyConstraintsForRelationshipInSeparateSchemaRules() {
        final var schemaName = "schema name";
        final var relType = 2;
        final var propertyId1 = 3;
        final var propertyId2 = 4;

        final var index = IndexPrototype.uniqueForSchema(forRelType(relType, propertyId1, propertyId2))
                .withName(schemaName)
                .materialise(1);
        final var uniquenessConstraint = uniqueForSchema(forRelType(relType, propertyId1))
                .withId(42)
                .withOwnedIndexId(index.getId())
                .withName(schemaName);

        final var cache = newSchemaCache(index, uniquenessConstraint);
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(relType, RELATIONSHIP))
                .as("should not find the logical key for the relationship yet")
                .isEqualTo(NO_LOGICAL_KEYS);

        final var existenceConstraint = relPropertyExistenceConstraint(43, relType, propertyId1);
        cache.addSchemaRule(existenceConstraint);
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(relType, RELATIONSHIP))
                .as("should now find the property that makes up the logical key for the relationship")
                .isEqualTo(logicalKeys(propertyId1));

        cache.addSchemaRule(uniquenessConstraint(44, relType, propertyId2, index.getId()));
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(relType, RELATIONSHIP))
                .as("should still be the one property that makes up the logical key for the relationship")
                .isEqualTo(logicalKeys(propertyId1));

        cache.removeSchemaRule(existenceConstraint.getId());
        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(relType, RELATIONSHIP))
                .as("logical key requires both the uniqueness and existence constraints")
                .isEqualTo(NO_LOGICAL_KEYS);
    }

    @ParameterizedTest
    @MethodSource("descriptorPositions")
    void logicalKeyConstraintsForNodeKeyAndExistenceInAnyOrderSchemaRules(
            int constraint1, int constraint2, int constraint3) {
        final var schemaName = "schema name";
        final var labelId = 2;
        final var propertyId1 = 3;
        final var propertyId2 = 4;

        final var schema = forLabel(labelId, propertyId1, propertyId2);
        final var index =
                IndexPrototype.uniqueForSchema(schema).withName(schemaName).materialise(1);

        final var descriptors = Lists.immutable.of(
                keyForSchema(schema).withId(42).withOwnedIndexId(index.getId()).withName(schemaName),
                nodePropertyExistenceConstraint(43, labelId, propertyId1),
                nodePropertyExistenceConstraint(44, labelId, propertyId2));

        final var cache = newSchemaCache(index, descriptors.get(constraint1));
        cache.addSchemaRule(descriptors.get(constraint2));
        cache.addSchemaRule(descriptors.get(constraint3));

        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(labelId, NODE))
                .as("should find the properties that makes up the logical key for the node/label")
                .isEqualTo(logicalKeys(propertyId1, propertyId2));
    }

    @ParameterizedTest
    @MethodSource("descriptorPositions")
    void logicalKeyConstraintsForRelationshipKeyAndExistenceInAnyOrderSchemaRules(
            int constraint1, int constraint2, int constraint3) {
        final var schemaName = "schema name";
        final var relType = 2;
        final var propertyId1 = 3;
        final var propertyId2 = 4;

        final var schema = forRelType(relType, propertyId1, propertyId2);
        final var index =
                IndexPrototype.uniqueForSchema(schema).withName(schemaName).materialise(1);

        final var descriptors = Lists.immutable.of(
                keyForSchema(schema).withId(42).withOwnedIndexId(index.getId()).withName(schemaName),
                relPropertyExistenceConstraint(43, relType, propertyId1),
                relPropertyExistenceConstraint(44, relType, propertyId2));

        final var cache = newSchemaCache(index, descriptors.get(constraint1));
        cache.addSchemaRule(descriptors.get(constraint2));
        cache.addSchemaRule(descriptors.get(constraint3));

        assertThat(cache.constraintsGetPropertyTokensForLogicalKey(relType, RELATIONSHIP))
                .as("should find the properties that makes up the logical key for the rel/type")
                .isEqualTo(logicalKeys(propertyId1, propertyId2));
    }

    // HELPERS

    private static Stream<Arguments> descriptorPositions() {
        return Stream.of(
                Arguments.of(0, 1, 2),
                Arguments.of(0, 2, 1),
                Arguments.of(1, 0, 2),
                Arguments.of(1, 2, 0),
                Arguments.of(2, 1, 0),
                Arguments.of(2, 0, 1));
    }

    private static int[] entityTokens(int... entityTokenIds) {
        return entityTokenIds;
    }

    private static int[] properties(int... propertyIds) {
        return propertyIds;
    }

    private static IndexDescriptor newIndexRule(long id, IndexType type, int label, int... propertyKeys) {
        return IndexPrototype.forSchema(forLabel(label, propertyKeys))
                .withName("index_id")
                .withIndexType(type)
                .materialise(id);
    }

    private static IndexDescriptor newIndexRule(long id, int label, int... propertyKeys) {
        return newIndexRule(forLabel(label, propertyKeys), id);
    }

    private static IndexDescriptor newIndexRule(SchemaDescriptor schema, long id) {
        return IndexPrototype.forSchema(schema).withName("index_id").materialise(id);
    }

    private static ConstraintDescriptor nodePropertyExistenceConstraint(long ruleId, int labelId, int propertyId) {
        return existsForLabel(labelId, propertyId).withId(ruleId);
    }

    private static ConstraintDescriptor relPropertyExistenceConstraint(long ruleId, int relTypeId, int propertyId) {
        return ConstraintDescriptorFactory.existsForRelType(relTypeId, propertyId)
                .withId(ruleId);
    }

    private static ConstraintDescriptor uniquenessConstraint(long ruleId, int labelId, int propertyId, long indexId) {
        return uniqueForLabel(labelId, propertyId).withId(ruleId).withOwnedIndexId(indexId);
    }

    private static ConstraintDescriptor uniquenessConstraint(
            long ruleId, int labelId, int propertyId, long indexId, IndexType type) {
        return uniqueForLabel(type, labelId, propertyId).withId(ruleId).withOwnedIndexId(indexId);
    }

    private SchemaCache newSchemaCache(SchemaRule... rules) {
        SchemaCache cache =
                new SchemaCache(new ConstraintSemantics(), indexConfigCompleter, StorageEngineIndexingBehaviour.EMPTY);
        cache.load((rules == null || rules.length == 0) ? Collections.emptyList() : asList(rules));
        return cache;
    }

    private SchemaCache newSchemaCacheWithRulesForRelatedToCalls() {
        return newSchemaCache(schema3_4, schema5_6_7, schema5_8, node35_8, rel35_8);
    }

    private static IntSet[] logicalKeys(int... props) {
        return new IntSet[] {IntSets.immutable.of(props)};
    }

    private static class ConstraintSemantics extends StandardConstraintRuleAccessor {
        @Override
        public ConstraintDescriptor readConstraint(ConstraintDescriptor constraint) {
            if ((constraint.type() == ConstraintType.EXISTS || constraint.type() == ConstraintType.UNIQUE_EXISTS)
                    && !constraint.enforcesPropertyExistence()) {
                throw new IllegalStateException("Unsupported constraint type: " + constraint);
            }
            return super.readConstraint(constraint);
        }
    }
}
