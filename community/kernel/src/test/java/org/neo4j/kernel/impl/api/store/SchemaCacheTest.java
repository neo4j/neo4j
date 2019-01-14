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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.test.Race;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.store.record.ConstraintRule.constraintRule;

public class SchemaCacheTest
{
    private final SchemaRule hans = newIndexRule( 1, 0, 5 );
    private final SchemaRule witch = nodePropertyExistenceConstraintRule( 2, 3, 6 );
    private final SchemaRule gretel = newIndexRule( 3, 0, 7 );
    private final ConstraintRule robot = relPropertyExistenceConstraintRule( 7L, 8, 9 );

    @Test
    public void should_construct_schema_cache()
    {
        // GIVEN
        Collection<SchemaRule> rules = asList( hans, witch, gretel, robot );
        SchemaCache cache = new SchemaCache( new ConstraintSemantics(), rules );

        // THEN
        assertEquals( asSet( hans, gretel ), Iterables.asSet( cache.indexRules() ) );
        assertEquals( asSet( witch, robot ), Iterables.asSet( cache.constraintRules() ) );
    }

    @Test
    public void addRemoveIndexes()
    {
        Collection<SchemaRule> rules = asList( hans, witch, gretel, robot );
        SchemaCache cache = new SchemaCache( new ConstraintSemantics(), rules );

        IndexRule rule1 = newIndexRule( 10, 11, 12 );
        IndexRule rule2 = newIndexRule( 13, 14, 15 );
        cache.addSchemaRule( rule1 );
        cache.addSchemaRule( rule2 );

        cache.removeSchemaRule( hans.getId() );
        cache.removeSchemaRule( witch.getId() );

        assertEquals( asSet( gretel, rule1, rule2 ), Iterables.asSet( cache.indexRules() ) );
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
        assertEquals( asSet( hans, gretel ), Iterables.asSet( cache.indexRules() ) );
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
        SchemaIndexDescriptor descriptor = cache.indexDescriptor( schema );

        // Then
        assertThat( descriptor.schema(), equalTo( schema ) );
    }

    @Test
    public void shouldReturnNullWhenNoIndexExists()
    {
        // Given
        SchemaCache schemaCache = newSchemaCache();

        // When
        SchemaIndexDescriptor schemaIndexDescriptor = schemaCache.indexDescriptor( forLabel( 1, 1 ) );

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
        Set<ConstraintDescriptor> expected = asSet(
                rule1.getConstraintDescriptor(),
                rule3.getConstraintDescriptor() );
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

        assertEquals( indexNumber, Iterables.count( cache.indexRules() ) );
        for ( int labelId = 0; labelId < indexNumber; labelId++ )
        {
            assertEquals( 1, Iterators.count( cache.indexDescriptorsForLabel( labelId ) ) );
        }
        for ( int propertyId = 0; propertyId < indexNumber; propertyId++ )
        {
            assertEquals( 1, Iterators.count( cache.indexesByProperty( propertyId ) ) );
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

        assertEquals( indexNumber - numberOfDeletions, Iterables.count( cache.indexRules() ) );
        for ( int labelId = numberOfDeletions; labelId < indexNumber; labelId++ )
        {
            assertEquals( 1, Iterators.count( cache.indexDescriptorsForLabel( labelId ) ) );
        }
        for ( int propertyId = numberOfDeletions; propertyId < indexNumber; propertyId++ )
        {
            assertEquals( 1, Iterators.count( cache.indexesByProperty( propertyId ) ) );
        }
    }

    private IndexRule newIndexRule( long id, int label, int propertyKey )
    {
        return IndexRule.indexRule( id, SchemaIndexDescriptorFactory.forLabel( label, propertyKey ), PROVIDER_DESCRIPTOR );
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
        return new SchemaCache( new ConstraintSemantics(), (rules == null || rules.length == 0)
                                                           ? Collections.emptyList() : Arrays.asList( rules ) );
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
