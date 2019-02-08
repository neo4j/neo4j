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

import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.LabelSchemaDescriptor;
import org.neo4j.test.Race;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.function.Function.identity;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterableOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.impl.store.record.ConstraintRule.constraintRule;

public class SchemaCacheTest
{
    private final SchemaRule hans = newIndexRule( 1, 0, 5 );
    private final SchemaRule witch = nodePropertyExistenceConstraintRule( 2, 3, 6 );
    private final SchemaRule gretel = newIndexRule( 3, 0, 7 );
    private final ConstraintRule robot = relPropertyExistenceConstraintRule( 7L, 8, 9 );

    private static final long[] noEntityToken = {};

    // For "related to" tests
    private SchemaRule schema3_4 = newIndexRule( 10, 3, 4 );
    private SchemaRule schema5_6_7 = newIndexRule( 11, 5, 6, 7 );
    private SchemaRule schema5_8 = newIndexRule( 12, 5, 8 );
    private SchemaRule node35_8 = IndexDescriptorFactory.forSchema( SchemaDescriptorFactory.multiToken( new int[] {3,5}, NODE, 8 ) ).withId( 13 );
    private SchemaRule rel35_8 = IndexDescriptorFactory.forSchema( SchemaDescriptorFactory.multiToken( new int[] {3,5}, RELATIONSHIP, 8 ) ).withId( 14 );

    @Test
    public void should_construct_schema_cache()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache( hans, witch, gretel, robot );

        // THEN
        assertEquals( asSet( hans, gretel ), Iterables.asSet( cache.indexDescriptors() ) );
        assertEquals( asSet( witch, robot ), Iterables.asSet( cache.constraintRules() ) );
    }

    @Test
    public void addRemoveIndexes()
    {
        SchemaCache cache = newSchemaCache( hans, witch, gretel, robot );

        StoreIndexDescriptor rule1 = newIndexRule( 10, 11, 12 );
        StoreIndexDescriptor rule2 = newIndexRule( 13, 14, 15 );
        cache.addSchemaRule( rule1 );
        cache.addSchemaRule( rule2 );

        cache.removeSchemaRule( hans.getId() );
        cache.removeSchemaRule( witch.getId() );

        assertEquals( asSet( gretel, rule1, rule2 ), Iterables.asSet( cache.indexDescriptors() ) );
        assertEquals( asSet( robot ), Iterables.asSet( cache.constraintRules() ) );
    }

    @Test
    public void addSchemaRules()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        // WHEN
        cache.addSchemaRule( hans );
        cache.addSchemaRule( gretel );
        cache.addSchemaRule( witch );
        cache.addSchemaRule( robot );

        // THEN
        assertEquals( asSet( hans, gretel ), Iterables.asSet( cache.indexDescriptors() ) );
        assertEquals( asSet( witch, robot ), Iterables.asSet( cache.constraintRules() ) );
    }

    @Test
    public void should_list_constraints()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        // WHEN
        cache.addSchemaRule( uniquenessConstraintRule( 0L, 1, 2, 133L ) );
        cache.addSchemaRule( uniquenessConstraintRule( 1L, 3, 4, 133L ) );
        cache.addSchemaRule( relPropertyExistenceConstraintRule( 2L, 5, 6 ) );
        cache.addSchemaRule( nodePropertyExistenceConstraintRule( 3L, 7, 8 ) );

        // THEN
        ConstraintDescriptor unique1 = uniqueForLabel( 1, 2 );
        ConstraintDescriptor unique2 = uniqueForLabel( 3, 4 );
        ConstraintDescriptor existsRel = ConstraintDescriptorFactory.existsForRelType( 5, 6 );
        ConstraintDescriptor existsNode = ConstraintDescriptorFactory.existsForLabel( 7, 8 );

        assertEquals(
                asSet( unique1, unique2, existsRel, existsNode ),
                asSet( cache.constraints() ) );

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
    public void should_remove_constraints()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( uniquenessConstraintRule( 0L, 1, 2, 133L ) );
        cache.addSchemaRule( uniquenessConstraintRule( 1L, 3, 4, 133L ) );

        // WHEN
        cache.removeSchemaRule( 0L );

        // THEN
        ConstraintDescriptor dropped = uniqueForLabel( 1, 1 );
        ConstraintDescriptor unique = uniqueForLabel( 3, 4 );
        assertEquals(
                asSet( unique ),
                asSet( cache.constraints() ) );

        assertEquals(
                asSet(),
                asSet( cache.constraintsForLabel( 1 ) ) );

        assertEquals(
                asSet(),
                asSet( cache.constraintsForSchema( dropped.schema() ) ) );
    }

    @Test
    public void adding_constraints_should_be_idempotent()
    {
        // given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( uniquenessConstraintRule( 0L, 1, 2, 133L ) );

        // when
        cache.addSchemaRule( uniquenessConstraintRule( 0L, 1, 2, 133L ) );

        // then
        assertEquals(
                asList( uniqueForLabel( 1, 2 ) ),
                Iterators.asList( cache.constraints() ) );
    }

    @Test
    public void shouldResolveIndexDescriptor()
    {
        // Given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( newIndexRule( 1L, 1, 2 ) );
        cache.addSchemaRule( newIndexRule( 2L, 1, 3 ) );
        cache.addSchemaRule( newIndexRule( 3L, 2, 2 ) );

        // When
        LabelSchemaDescriptor schema = forLabel( 1, 3 );
        StorageIndexReference descriptor = cache.indexDescriptor( schema );

        // Then
        assertThat( descriptor.schema(), equalTo( schema ) );
    }

    @Test
    public void shouldReturnNullWhenNoIndexExists()
    {
        // Given
        SchemaCache schemaCache = newSchemaCache();

        // When
        StorageIndexReference schemaIndexDescriptor = schemaCache.indexDescriptor( forLabel( 1, 1 ) );

        // Then
        assertNull( schemaIndexDescriptor );
    }

    @Test
    public void shouldListConstraintsForLabel()
    {
        // Given
        ConstraintRule rule1 = uniquenessConstraintRule( 0, 1, 1, 0 );
        ConstraintRule rule2 = uniquenessConstraintRule( 1, 2, 1, 0 );
        ConstraintRule rule3 = nodePropertyExistenceConstraintRule( 2, 1, 2 );

        SchemaCache cache = newSchemaCache();
        cache.addSchemaRule( rule1 );
        cache.addSchemaRule( rule2 );
        cache.addSchemaRule( rule3 );

        // When
        Set<ConstraintDescriptor> listed = asSet( cache.constraintsForLabel( 1 ) );

        // Then
        Set<ConstraintDescriptor> expected = asSet( rule1.getConstraintDescriptor(), rule3.getConstraintDescriptor() );
        assertEquals( expected, listed );
    }

    @Test
    public void shouldListConstraintsForSchema()
    {
        // Given
        ConstraintRule rule1 = uniquenessConstraintRule( 0, 1, 1, 0 );
        ConstraintRule rule2 = uniquenessConstraintRule( 1, 2, 1, 0 );
        ConstraintRule rule3 = nodePropertyExistenceConstraintRule( 2, 1, 2 );

        SchemaCache cache = newSchemaCache();
        cache.addSchemaRule( rule1 );
        cache.addSchemaRule( rule2 );
        cache.addSchemaRule( rule3 );

        // When
        Set<ConstraintDescriptor> listed = asSet( cache.constraintsForSchema( rule3.schema() ) );

        // Then
        assertEquals( singleton( rule3.getConstraintDescriptor() ), listed );
    }

    @Test
    public void shouldListConstraintsForRelationshipType()
    {
        // Given
        ConstraintRule rule1 = relPropertyExistenceConstraintRule( 0, 1, 1 );
        ConstraintRule rule2 = relPropertyExistenceConstraintRule( 0, 2, 1 );
        ConstraintRule rule3 = relPropertyExistenceConstraintRule( 0, 1, 2 );

        SchemaCache cache = newSchemaCache();
        cache.addSchemaRule( rule1 );
        cache.addSchemaRule( rule2 );
        cache.addSchemaRule( rule3 );

        // When
        Set<ConstraintDescriptor> listed = asSet( cache.constraintsForRelationshipType( 1 ) );

        // Then
        Set<ConstraintDescriptor> expected = asSet( rule1.getConstraintDescriptor(), rule3.getConstraintDescriptor() );
        assertEquals( expected, listed );
    }

    @Test
    public void concurrentSchemaRuleAdd() throws Throwable
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

        assertEquals( indexNumber, Iterables.count( cache.indexDescriptors() ) );
        for ( int labelId = 0; labelId < indexNumber; labelId++ )
        {
            assertEquals( 1, Iterators.count( cache.indexDescriptorsForLabel( labelId ) ) );
        }
        for ( int propertyId = 0; propertyId < indexNumber; propertyId++ )
        {
            assertEquals( 1, Iterators.count( cache.indexesByNodeProperty( propertyId ) ) );
        }
    }

    @Test
    public void concurrentSchemaRuleRemove() throws Throwable
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

        assertEquals( indexNumber - numberOfDeletions, Iterables.count( cache.indexDescriptors() ) );
        for ( int labelId = numberOfDeletions; labelId < indexNumber; labelId++ )
        {
            assertEquals( 1, Iterators.count( cache.indexDescriptorsForLabel( labelId ) ) );
        }
        for ( int propertyId = numberOfDeletions; propertyId < indexNumber; propertyId++ )
        {
            assertEquals( 1, Iterators.count( cache.indexesByNodeProperty( propertyId ) ) );
        }
    }

    @Test
    public void shouldGetRelatedIndexForLabel()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, IntSets.immutable.empty(), NODE, identity() ),
                containsInAnyOrder( schema3_4, node35_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexForProperty()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(
                cache.getIndexesRelatedTo( noEntityToken, entityTokens( 3, 4, 5 ), properties( 4 ), NODE, identity() ),
                containsInAnyOrder( schema3_4 ) );
    }

    @Test
    public void shouldGetRelatedIndexesForLabel()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( entityTokens( 5 ), entityTokens( 3, 4 ), IntSets.immutable.empty(), NODE, identity() ),
                containsInAnyOrder( schema5_6_7, schema5_8, node35_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexes()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(
                cache.getIndexesRelatedTo( entityTokens( 3 ), entityTokens( 4, 5 ), properties( 7 ), NODE, identity() ),
                containsInAnyOrder( schema3_4, schema5_6_7, node35_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexOnce()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat(
                cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, properties( 4 ), NODE, identity() ),
                containsInAnyOrder( schema3_4, node35_8 ) );

        assertThat(
                cache.getIndexesRelatedTo( noEntityToken, entityTokens( 5 ), properties( 6, 7 ), NODE, identity() ),
                containsInAnyOrder( schema5_6_7 ) );
    }

    @Test
    public void shouldHandleUnrelated()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( noEntityToken, noEntityToken, IntSets.immutable.empty(), NODE, identity() ),
                emptyIterableOf( StorageIndexReference.class ) );

        assertTrue( cache.getIndexesRelatedTo( entityTokens( 2 ), noEntityToken, IntSets.immutable.empty(), NODE, identity() ).isEmpty() );

        assertThat(
                cache.getIndexesRelatedTo( noEntityToken, entityTokens( 2 ), properties( 1 ), NODE, identity() ),
                emptyIterableOf( StorageIndexReference.class ) );

        assertTrue( cache.getIndexesRelatedTo( entityTokens( 2 ), entityTokens( 2 ), properties( 1 ), NODE, identity() ).isEmpty() );
    }

    @Test
    public void shouldGetMultiLabelForAnyOfTheLabels()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, IntSets.immutable.empty(), NODE, identity() ),
                containsInAnyOrder( schema3_4, node35_8 ) );

        assertThat( cache.getIndexesRelatedTo( entityTokens( 5 ), noEntityToken, IntSets.immutable.empty(), NODE, identity() ),
                containsInAnyOrder( schema5_8, schema5_6_7, node35_8 ) );
    }

    @Test
    public void shouldOnlyGetRelIndexesForRelUpdates()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, IntSets.immutable.empty(), RELATIONSHIP, identity() ),
                containsInAnyOrder( rel35_8 ) );

        assertThat( cache.getIndexesRelatedTo( entityTokens( 5 ), noEntityToken, IntSets.immutable.empty(), RELATIONSHIP, identity() ),
                containsInAnyOrder( rel35_8 ) );
    }

    @Test
    public void removalsShouldOnlyRemoveCorrectProxy()
    {
        SchemaCache cache = newSchemaCacheWithRulesForRelatedToCalls();
        cache.removeSchemaRule( node35_8.getId() );
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, IntSets.immutable.empty(), NODE, identity() ),
                containsInAnyOrder( schema3_4 ) );
        assertThat( cache.getIndexesRelatedTo( entityTokens( 3 ), noEntityToken, IntSets.immutable.empty(), RELATIONSHIP, identity() ),
                containsInAnyOrder( rel35_8 ) );

        cache.removeSchemaRule( 7 );
        assertThat( cache.getIndexesRelatedTo( entityTokens( 5 ), noEntityToken, IntSets.immutable.empty(), NODE, identity() ),
                containsInAnyOrder( schema5_8, schema5_6_7 ) );
        assertThat( cache.getIndexesRelatedTo( entityTokens( 5 ), noEntityToken, IntSets.immutable.empty(), RELATIONSHIP, identity() ),
                containsInAnyOrder( rel35_8 ) );

    }

    // HELPERS

    private long[] entityTokens( long... entityTokenIds )
    {
        return entityTokenIds;
    }

    private IntSet properties( int... propertyIds )
    {
        return new IntHashSet( propertyIds );
    }

    private StoreIndexDescriptor newIndexRule( long id, int label, int... propertyKeys )
    {
        return TestIndexDescriptorFactory.forLabel( label, propertyKeys ).withId( id );
    }

    private ConstraintRule nodePropertyExistenceConstraintRule( long ruleId, int labelId, int propertyId )
    {
        return constraintRule( ruleId, ConstraintDescriptorFactory.existsForLabel( labelId, propertyId ) );
    }

    private ConstraintRule relPropertyExistenceConstraintRule( long ruleId, int relTypeId, int propertyId )
    {
        return constraintRule( ruleId, ConstraintDescriptorFactory.existsForRelType( relTypeId, propertyId ) );
    }

    private ConstraintRule uniquenessConstraintRule( long ruleId, int labelId, int propertyId, long indexId )
    {
        return constraintRule( ruleId, uniqueForLabel( labelId, propertyId ), indexId );
    }

    private static SchemaCache newSchemaCache( SchemaRule... rules )
    {
        SchemaRuleAccess access = new StubSchemaRuleAccess()
        {
            @Override
            public Iterable<SchemaRule> getAll()
            {
                return (rules == null || rules.length == 0) ? Collections.emptyList() : asList( rules );
            }
        };
        SchemaCache cache = new SchemaCache( new ConstraintSemantics(), access );
        cache.loadAllRules();
        return cache;
    }

    private SchemaCache newSchemaCacheWithRulesForRelatedToCalls()
    {
        return newSchemaCache( schema3_4, schema5_6_7, schema5_8, node35_8, rel35_8 );
    }

    private static class ConstraintSemantics extends StandardConstraintSemantics
    {
        @Override
        protected ConstraintDescriptor readNonStandardConstraint( ConstraintRule rule, String errorMessage )
        {
            if ( !rule.getConstraintDescriptor().enforcesPropertyExistence() )
            {
                throw new IllegalStateException( "Unsupported constraint type: " + rule );
            }
            return rule.getConstraintDescriptor();
        }
    }
}
