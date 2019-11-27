/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexValueCapability;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.test.Race;
import org.neo4j.values.storable.ValueCategory;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptor.forRelType;
import static org.neo4j.internal.schema.SchemaDescriptor.fulltext;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForLabel;

class SchemaCacheTest
{
    private final SchemaRule hans = newIndexRule( 1, 0, 5 );
    private final SchemaRule witch = nodePropertyExistenceConstraint( 2, 3, 6 );
    private final SchemaRule gretel = newIndexRule( 3, 0, 7 );
    private final ConstraintDescriptor robot = relPropertyExistenceConstraint( 7L, 8, 9 );
    private IndexConfigCompleter indexConfigCompleter = index -> index;

    private static final long[] noEntityToken = {};

    // For "related to" tests
    private IndexDescriptor schema3_4 = newIndexRule( 10, 3, 4 );
    private IndexDescriptor schema5_6_7 = newIndexRule( 11, 5, 6, 7 );
    private IndexDescriptor schema5_8 = newIndexRule( 12, 5, 8 );
    private IndexDescriptor node35_8 = IndexPrototype.forSchema( fulltext( NODE, new int[]{3, 5}, new int[]{8} ) )
            .withName( "index_13" ).materialise( 13 );
    private IndexDescriptor rel35_8 = IndexPrototype.forSchema( fulltext( RELATIONSHIP, new int[]{3, 5}, new int[]{8} ) )
            .withName( "index_14" ).materialise( 14 );

    @Test
    void shouldConstructSchemaCache()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache( hans, witch, gretel, robot );

        // THEN
        assertEquals( asSet( hans, gretel ), Iterables.asSet( cache.indexes() ) );
        assertEquals( asSet( witch, robot ), Iterables.asSet( cache.constraints() ) );
    }

    @Test
    void addRemoveIndexes()
    {
        SchemaCache cache = newSchemaCache( hans, witch, gretel, robot );

        IndexDescriptor rule1 = newIndexRule( 10, 11, 12 );
        IndexDescriptor rule2 = newIndexRule( 13, 14, 15 );
        cache.addSchemaRule( rule1 );
        cache.addSchemaRule( rule2 );

        cache.removeSchemaRule( hans.getId() );
        cache.removeSchemaRule( witch.getId() );

        assertEquals( asSet( gretel, rule1, rule2 ), Iterables.asSet( cache.indexes() ) );
        assertEquals( asSet( robot ), Iterables.asSet( cache.constraints() ) );
    }

    @Test
    void addSchemaRules()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        // WHEN
        cache.addSchemaRule( hans );
        cache.addSchemaRule( gretel );
        cache.addSchemaRule( witch );
        cache.addSchemaRule( robot );

        // THEN
        assertEquals( asSet( hans, gretel ), Iterables.asSet( cache.indexes() ) );
        assertEquals( asSet( witch, robot ), Iterables.asSet( cache.constraints() ) );
    }

    @Test
    void shouldListConstraints()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        // WHEN
        cache.addSchemaRule( uniquenessConstraint( 0L, 1, 2, 133L ) );
        cache.addSchemaRule( uniquenessConstraint( 1L, 3, 4, 133L ) );
        cache.addSchemaRule( relPropertyExistenceConstraint( 2L, 5, 6 ) );
        cache.addSchemaRule( nodePropertyExistenceConstraint( 3L, 7, 8 ) );

        // THEN
        ConstraintDescriptor unique1 = uniqueForLabel( 1, 2 );
        ConstraintDescriptor unique2 = uniqueForLabel( 3, 4 );
        ConstraintDescriptor existsRel = ConstraintDescriptorFactory.existsForRelType( 5, 6 );
        ConstraintDescriptor existsNode = ConstraintDescriptorFactory.existsForLabel( 7, 8 );

        assertEquals(
                asSet( unique1, unique2, existsRel, existsNode ),
                Iterables.asSet( cache.constraints() ) );

        assertEquals(
                asSet( unique1 ),
                asSet( cache.constraintsForLabel( 1 ) ) );

        assertEquals(
                asSet( unique1 ),
                asSet( cache.constraintsForSchema( unique1.schema() ) ) );

        assertEquals(
                asSet(),
                asSet( cache.constraintsForSchema( forLabel( 1, 3 ) ) ) );

        assertEquals(
                asSet( existsRel ),
                asSet( cache.constraintsForRelationshipType( 5 ) ) );
    }

    @Test
    void shouldRemoveConstraints()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( uniquenessConstraint( 0L, 1, 2, 133L ) );
        cache.addSchemaRule( uniquenessConstraint( 1L, 3, 4, 133L ) );

        // WHEN
        cache.removeSchemaRule( 0L );

        // THEN
        ConstraintDescriptor dropped = uniqueForLabel( 1, 1 );
        ConstraintDescriptor unique = uniqueForLabel( 3, 4 );
        assertEquals(
                asSet( unique ),
                Iterables.asSet( cache.constraints() ) );

        assertEquals(
                asSet(),
                asSet( cache.constraintsForLabel( 1 ) ) );

        assertEquals(
                asSet(),
                asSet( cache.constraintsForSchema( dropped.schema() ) ) );
    }

    @Test
    void addingConstraintsShouldBeIdempotent()
    {
        // given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( uniquenessConstraint( 0L, 1, 2, 133L ) );

        // when
        cache.addSchemaRule( uniquenessConstraint( 0L, 1, 2, 133L ) );

        // then
        assertEquals( Collections.singletonList( uniqueForLabel( 1, 2 ) ),
                Iterables.asList( cache.constraints() ) );
    }

    @Test
    void shouldResolveIndexDescriptor()
    {
        // Given
        SchemaCache cache = newSchemaCache();

        IndexDescriptor expected;
        cache.addSchemaRule( newIndexRule( 1L, 1, 2 ) );
        cache.addSchemaRule( expected = newIndexRule( 2L, 1, 3 ) );
        cache.addSchemaRule( newIndexRule( 3L, 2, 2 ) );

        // When
        IndexDescriptor actual = single( cache.indexesForSchema( forLabel( 1, 3 ) ) );

        // Then
        assertThat( actual ).isEqualTo( expected );
    }

    @Test
    void schemaCacheSnapshotsShouldBeReadOnly()
    {
        // Given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( newIndexRule( 1L, 1, 2 ) );
        cache.addSchemaRule( newIndexRule( 2L, 2, 3 ) );

        SchemaCache snapshot = cache.snapshot();

        cache.addSchemaRule( newIndexRule( 3L, 1, 2 ) );

        // When
        Set<IndexDescriptor> indexes = asSet( snapshot.indexesForLabel( 1 ) );

        // Then
        Set<IndexDescriptor> expected = asSet( newIndexRule( 1L, 1, 2 ) );
        assertEquals( expected, indexes );

        assertThrows( IllegalStateException.class, () -> snapshot.addSchemaRule( newIndexRule( 3L, 1, 2 ) ) );
    }

    @Test
    void shouldReturnNullWhenNoIndexExists()
    {
        // Given
        SchemaCache schemaCache = newSchemaCache();

        // When
        Iterator<IndexDescriptor> iterator = schemaCache.indexesForSchema( forLabel( 1, 1 ) );

        // Then
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldListConstraintsForLabel()
    {
        // Given
        ConstraintDescriptor rule1 = uniquenessConstraint( 0, 1, 1, 0 );
        ConstraintDescriptor rule2 = uniquenessConstraint( 1, 2, 1, 0 );
        ConstraintDescriptor rule3 = nodePropertyExistenceConstraint( 2, 1, 2 );

        SchemaCache cache = newSchemaCache();
        cache.addSchemaRule( rule1 );
        cache.addSchemaRule( rule2 );
        cache.addSchemaRule( rule3 );

        // When
        Set<ConstraintDescriptor> listed = asSet( cache.constraintsForLabel( 1 ) );

        // Then
        Set<ConstraintDescriptor> expected = asSet( rule1, rule3 );
        assertEquals( expected, listed );
    }

    @Test
    void shouldListConstraintsForSchema()
    {
        // Given
        ConstraintDescriptor rule1 = uniquenessConstraint( 0, 1, 1, 0 );
        ConstraintDescriptor rule2 = uniquenessConstraint( 1, 2, 1, 0 );
        ConstraintDescriptor rule3 = nodePropertyExistenceConstraint( 2, 1, 2 );

        SchemaCache cache = newSchemaCache();
        cache.addSchemaRule( rule1 );
        cache.addSchemaRule( rule2 );
        cache.addSchemaRule( rule3 );

        // When
        Set<ConstraintDescriptor> listed = asSet( cache.constraintsForSchema( rule3.schema() ) );

        // Then
        assertEquals( singleton( rule3 ), listed );
    }

    @Test
    void shouldListConstraintsForRelationshipType()
    {
        // Given
        ConstraintDescriptor rule1 = relPropertyExistenceConstraint( 0, 1, 1 );
        ConstraintDescriptor rule2 = relPropertyExistenceConstraint( 1, 2, 1 );
        ConstraintDescriptor rule3 = relPropertyExistenceConstraint( 2, 1, 2 );

        SchemaCache cache = newSchemaCache();
        cache.addSchemaRule( rule1 );
        cache.addSchemaRule( rule2 );
        cache.addSchemaRule( rule3 );

        // When
        Set<ConstraintDescriptor> listed = asSet( cache.constraintsForRelationshipType( 1 ) );

        // Then
        Set<ConstraintDescriptor> expected = asSet( rule1, rule3 );
        assertEquals( expected, listed );
    }

    @Test
    void concurrentSchemaRuleAdd() throws Throwable
    {
        SchemaCache cache = newSchemaCache();
        Race race = new Race();
        int indexNumber = 10;
        for ( int i = 0; i < indexNumber; i++ )
        {
            int id = i;
            race.addContestant( () -> cache.addSchemaRule( newIndexRule( id, id, id ) ) );
        }
        race.go();

        assertEquals( indexNumber, Iterables.count( cache.indexes() ) );
        for ( int labelId = 0; labelId < indexNumber; labelId++ )
        {
            assertEquals( 1, Iterators.count( cache.indexesForLabel( labelId ) ) );
        }
    }

    @Test
    void concurrentSchemaRuleRemove() throws Throwable
    {
        SchemaCache cache = newSchemaCache();
        int indexNumber = 20;
        for ( int i = 0; i < indexNumber; i++ )
        {
            cache.addSchemaRule( newIndexRule( i, i, i ) );
        }
        Race race = new Race();
        int numberOfDeletions = 10;
        for ( int i = 0; i < numberOfDeletions; i++ )
        {
            int indexId = i;
            race.addContestant( () -> cache.removeSchemaRule( indexId ) );
        }
        race.go();

        assertEquals( indexNumber - numberOfDeletions, Iterables.count( cache.indexes() ) );
        for ( int labelId = numberOfDeletions; labelId < indexNumber; labelId++ )
        {
            assertEquals( 1, Iterators.count( cache.indexesForLabel( labelId ) ) );
        }
    }

    @Test
    void removeSchemaWithRepeatedLabel()
    {
        final SchemaCache cache = newSchemaCache();

        final int id = 1;
        final int[] repeatedLabels = {0, 1, 0};
        final FulltextSchemaDescriptor schema = fulltext( NODE, repeatedLabels, new int[]{1} );
        IndexDescriptor index = newIndexRule( schema, id );
        cache.addSchemaRule( index );
        cache.removeSchemaRule( id );
    }

    @Test
    void removeSchemaWithRepeatedRelType()
    {
        final SchemaCache cache = newSchemaCache();

        final int id = 1;
        final int[] repeatedRelTypes = {0, 1, 0};
        final FulltextSchemaDescriptor schema = fulltext( RELATIONSHIP, repeatedRelTypes, new int[]{1} );
        IndexDescriptor index = newIndexRule( schema, id );
        cache.addSchemaRule( index );
        cache.removeSchemaRule( id );
    }

    @Test
    void shouldGetRelatedIndexForLabel()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, properties(), false, NODE ) ).contains( schema3_4, node35_8 );
    }

    @Test
    void shouldGetRelatedIndexForProperty()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( noEntityToken, entityTokens( 3, 4, 5 ), properties( 4 ), false, NODE ) ).contains( schema3_4 );
    }

    @Test
    void shouldGetRelatedIndexesForLabel()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( entityTokens( 5 ), entityTokens( 3, 4 ), properties(), false, NODE ) ).contains( schema5_6_7, schema5_8,
                node35_8 );
    }

    @Test
    void shouldGetRelatedIndexes()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), entityTokens( 4, 5 ), properties( 7 ), false, NODE ) ).contains( schema3_4, schema5_6_7,
                node35_8 );
    }

    @Test
    void shouldGetRelatedIndexOnce()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, properties( 4 ), false, NODE ) ).contains( schema3_4, node35_8 );

        assertThat( cache.getIndexesRelatedTo( noEntityToken, entityTokens( 5 ), properties( 6, 7 ), false, NODE ) ).contains( schema5_6_7 );
    }

    @Test
    void shouldHandleUnrelated()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( noEntityToken, noEntityToken, properties(), false, NODE ) ).isEmpty();

        assertTrue( cache.getIndexesRelatedTo( entityTokens( 2 ), noEntityToken, properties(), false, NODE ).isEmpty() );

        assertThat( cache.getIndexesRelatedTo( noEntityToken, entityTokens( 2 ), properties( 1 ), false, NODE ) ).isEmpty();

        assertTrue( cache.getIndexesRelatedTo( entityTokens( 2 ), entityTokens( 2 ), properties( 1 ), false, NODE ).isEmpty() );
    }

    @Test
    void shouldGetMultiLabelForAnyOfTheLabels()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, properties(), false, NODE ) ).contains( schema3_4, node35_8 );

        assertThat( cache.getIndexesRelatedTo( entityTokens( 5 ), noEntityToken, properties(), false, NODE ) ).contains( schema5_8, schema5_6_7, node35_8 );
    }

    @Test
    void shouldOnlyGetRelIndexesForRelUpdates()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, properties(), false, RELATIONSHIP ) ).contains( rel35_8 );

        assertThat( cache.getIndexesRelatedTo( entityTokens( 5 ), noEntityToken, properties(), false, RELATIONSHIP ) ).contains( rel35_8 );
    }

    @Test
    void removalsShouldOnlyRemoveCorrectProxy()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        cache.removeSchemaRule( node35_8.getId() );
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, properties(), false, NODE ) ).contains( schema3_4 );
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, properties(), false, RELATIONSHIP ) ).contains( rel35_8 );

        cache.removeSchemaRule( 7 );
        assertThat( cache.getIndexesRelatedTo( entityTokens( 5 ), noEntityToken, properties(), false, NODE ) ).contains( schema5_8, schema5_6_7 );
        assertThat( cache.getIndexesRelatedTo( entityTokens( 5 ), noEntityToken, properties(), false, RELATIONSHIP ) ).contains( rel35_8 );

    }

    @Test
    void shouldGetRelatedNodeConstraints()
    {
        // given
        SchemaCache cache = new SchemaCache( new ConstraintSemantics(), indexConfigCompleter );
        ConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForLabel( 1, 5, 6 ).withId( 1 );
        ConstraintDescriptor constraint2 = ConstraintDescriptorFactory.uniqueForLabel( 1, 5 ).withId( 2 );
        ConstraintDescriptor constraint3 = ConstraintDescriptorFactory.uniqueForLabel( 2, 5 ).withId( 3 );
        cache.addSchemaRule( constraint1 );
        cache.addSchemaRule( constraint2 );
        cache.addSchemaRule( constraint3 );

        // when/then
        assertEquals(
                asSet( constraint2 ),
                cache.getUniquenessConstraintsRelatedTo( entityTokens( 1 ), entityTokens(), properties( 5 ), true, NODE ) );
        assertEquals(
                asSet( constraint1, constraint2 ),
                cache.getUniquenessConstraintsRelatedTo( entityTokens( 1 ), entityTokens(), properties( 5 ), false, NODE ) );
        assertEquals(
                asSet( constraint1, constraint2 ),
                cache.getUniquenessConstraintsRelatedTo( entityTokens( 1 ), entityTokens(), properties( 5, 6 ), true, NODE ) );
        assertEquals(
                asSet( constraint1, constraint2 ),
                cache.getUniquenessConstraintsRelatedTo( entityTokens(), entityTokens( 1 ), properties( 5 ), false, NODE ) );
        assertEquals(
                asSet( constraint1, constraint2, constraint3 ),
                cache.getUniquenessConstraintsRelatedTo( entityTokens( 1, 2 ), entityTokens(), properties(), false, NODE ) );
    }

    @Test
    void shouldRemoveNodeConstraints()
    {
        // given
        SchemaCache cache = new SchemaCache( new ConstraintSemantics(), indexConfigCompleter );
        ConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForLabel( 1, 5, 6 ).withId( 1 );
        ConstraintDescriptor constraint2 = ConstraintDescriptorFactory.uniqueForLabel( 1, 5 ).withId( 2 );
        ConstraintDescriptor constraint3 = ConstraintDescriptorFactory.uniqueForLabel( 2, 5 ).withId( 3 );
        cache.addSchemaRule( constraint1 );
        cache.addSchemaRule( constraint2 );
        cache.addSchemaRule( constraint3 );
        assertEquals(
                asSet( constraint2 ),
                cache.getUniquenessConstraintsRelatedTo( entityTokens( 1 ), entityTokens(), properties( 5 ), true, NODE ) );

        // and when
        cache.removeSchemaRule( constraint1.getId() );
        cache.removeSchemaRule( constraint2.getId() );
        cache.removeSchemaRule( constraint3.getId() );

        // then
        assertTrue( cache.getUniquenessConstraintsRelatedTo( entityTokens( 1 ), entityTokens(), properties( 5 ), true, NODE ).isEmpty() );
    }

    @Test
    void shouldCompleteConfigurationOfIndexesAddedToCache()
    {
        IndexCapability capability = new IndexCapability()
        {
            @Override
            public IndexOrder[] orderCapability( ValueCategory... valueCategories )
            {
                return new IndexOrder[0];
            }

            @Override
            public IndexValueCapability valueCapability( ValueCategory... valueCategories )
            {
                return IndexValueCapability.NO;
            }
        };
        ArrayList<IndexDescriptor> completed = new ArrayList<>();
        IndexConfigCompleter completer = index ->
        {
            completed.add( index );
            return index.withIndexCapability( capability );
        };
        SchemaCache cache = new SchemaCache( new ConstraintSemantics(), completer );

        IndexDescriptor index1 = newIndexRule( 1, 2, 3 );
        ConstraintDescriptor constraint1 = uniquenessConstraint( 2, 2, 3, 1 );
        IndexDescriptor index2 = newIndexRule( 3, 4, 5 );
        ConstraintDescriptor constraint2 = uniquenessConstraint( 4, 4, 5, 3 );
        IndexDescriptor index3 = newIndexRule( 5, 5, 5 );

        cache.load( asList( index1, constraint1 ) );
        cache.addSchemaRule( index2 );
        cache.addSchemaRule( constraint2 );
        cache.addSchemaRule( index3 );

        assertEquals( List.of( index1, index2, index3 ), completed );
        assertEquals( capability, cache.getIndex( index1.getId() ).getCapability() );
        assertEquals( capability, cache.getIndex( index2.getId() ).getCapability() );
        assertEquals( capability, cache.getIndex( index3.getId() ).getCapability() );
    }

    @Test
    void shouldHaveAddedConstraintsAndIndexes()
    {
        long constraintId = 1;
        long indexId = 4;
        IndexDescriptor index = newIndexRule( indexId, 2, 3 );
        ConstraintDescriptor constraint = uniquenessConstraint( constraintId, 2, 3, indexId );
        SchemaCache cache = newSchemaCache( index, constraint );
        assertTrue( cache.hasConstraintRule( constraintId ) );
        assertTrue( cache.hasConstraintRule( constraint ) );
        assertFalse( cache.hasConstraintRule( indexId ) );
        assertTrue( cache.hasIndex( index ) );
    }

    @Test
    void hasConstraintRuleShouldMatchBySchemaAndType()
    {
        ConstraintDescriptor existing = uniquenessConstraint( 1, 2, 3, 4 );
        ConstraintDescriptor checked = uniquenessConstraint( 0, 2, 3, 4 ); // Different rule id, but same type and schema.
        SchemaCache cache = newSchemaCache( existing );
        assertTrue( cache.hasConstraintRule( checked ) );
    }

    @Test
    void shouldCacheDependentState()
    {
        SchemaCache cache = newSchemaCache();
        MutableInt mint = cache.getOrCreateDependantState( MutableInt.class, MutableInt::new, 1 );
        assertEquals( 1, mint.getValue() );
        mint.setValue( 2 );
        mint = cache.getOrCreateDependantState( MutableInt.class, MutableInt::new, 1 );
        assertEquals( 2, mint.getValue() );
    }

    @Test
    void shouldFindIndexDescriptorsByRelationshipType()
    {
        IndexDescriptor first = IndexPrototype.forSchema( forRelType( 2, 3 ) ).withName( "index_1" ).materialise( 1 );
        IndexDescriptor second = IndexPrototype.forSchema( forLabel( 2, 3 ) ).withName( "index_2" ).materialise( 2 );
        SchemaCache cache = newSchemaCache( first, second );
        assertEquals( first, single( cache.indexesForRelationshipType( 2 ) ) );
        assertEquals( first.getId(), single( cache.indexesForRelationshipType( 2 ) ).getId() );
    }

    @Test
    void shouldFindIndexDescriptorsByIndexName()
    {
        IndexDescriptor index = IndexPrototype.forSchema( forLabel( 2, 3 ) ).withName( "index name" ).materialise( 1 );
        SchemaCache cache = newSchemaCache( index );
        assertEquals( index, cache.indexForName( "index name" ) );
        cache.removeSchemaRule( index.getId() );
        assertNull( cache.indexForName( "index name" ) );
    }

    @Test
    void shouldFindConstraintByName()
    {
        ConstraintDescriptor constraint = nodePropertyExistenceConstraint( 1, 2, 3 ).withName( "constraint name" );
        SchemaCache cache = newSchemaCache( constraint );
        assertEquals( constraint, cache.constraintForName( "constraint name" ) );
        cache.removeSchemaRule( constraint.getId() );
        assertNull( cache.constraintForName( "constraint name" ) );
    }

    @Test
    void shouldFindConstraintAndIndexByName()
    {
        IndexDescriptor index = IndexPrototype.uniqueForSchema( forLabel( 2, 3 ) ).withName( "schema name" ).materialise( 1 );
        ConstraintDescriptor constraint = uniquenessConstraint( 4, 2, 3, 1 ).withName( "schema name" );
        SchemaCache cache = newSchemaCache( index, constraint );
        assertEquals( index, cache.indexForName( "schema name" ) );
        assertEquals( constraint, cache.constraintForName( "schema name" ) );
    }

    // HELPERS

    private static long[] entityTokens( long... entityTokenIds )
    {
        return entityTokenIds;
    }

    private static int[] properties( int... propertyIds )
    {
        return propertyIds;
    }

    private static IndexDescriptor newIndexRule( long id, int label, int... propertyKeys )
    {
        return newIndexRule( forLabel( label, propertyKeys ), id );
    }

    private static IndexDescriptor newIndexRule( SchemaDescriptor schema, long id )
    {
        return IndexPrototype.forSchema( schema ).withName( "index_id" ).materialise( id );
    }

    private static ConstraintDescriptor nodePropertyExistenceConstraint( long ruleId, int labelId, int propertyId )
    {
        return ConstraintDescriptorFactory.existsForLabel( labelId, propertyId ).withId( ruleId );
    }

    private static ConstraintDescriptor relPropertyExistenceConstraint( long ruleId, int relTypeId, int propertyId )
    {
        return ConstraintDescriptorFactory.existsForRelType( relTypeId, propertyId ).withId( ruleId );
    }

    private static ConstraintDescriptor uniquenessConstraint( long ruleId, int labelId, int propertyId, long indexId )
    {
        return uniqueForLabel( labelId, propertyId ).withId( ruleId ).withOwnedIndexId( indexId );
    }

    private SchemaCache newSchemaCache( SchemaRule... rules )
    {
        SchemaCache cache = new SchemaCache( new ConstraintSemantics(), indexConfigCompleter );
        cache.load( (rules == null || rules.length == 0) ? Collections.emptyList() : asList( rules ) );
        return cache;
    }

    private SchemaCache newSchemaCacheWithRulesForRelatedToCalls()
    {
        return newSchemaCache( schema3_4, schema5_6_7, schema5_8, node35_8, rel35_8 );
    }

    private static class ConstraintSemantics extends StandardConstraintRuleAccessor
    {
        @Override
        public ConstraintDescriptor readConstraint( ConstraintDescriptor constraint )
        {
            if ( (constraint.type() == ConstraintType.EXISTS || constraint.type() == ConstraintType.UNIQUE_EXISTS) && !constraint.enforcesPropertyExistence() )
            {
                throw new IllegalStateException( "Unsupported constraint type: " + constraint );
            }
            return super.readConstraint( constraint );
        }
    }
}
