/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

public class SchemaCacheTest
{
    final SchemaRule hans = newIndexRule( 1, 0, 5 );
    final SchemaRule witch = nodePropertyExistenceConstraintRule( 2, 3, 6 );
    final SchemaRule gretel = newIndexRule( 3, 0, 7 );

    @Test
    public void should_construct_schema_cache()
    {
        // GIVEN
        Collection<SchemaRule> rules = asList( hans, witch, gretel );
        SchemaCache cache = new SchemaCache( new ConstraintSemantics(), rules );

        // THEN
        assertEquals( asSet( hans, gretel ), Iterables.asSet( cache.indexRules() ) );
        assertEquals( asSet( witch ), Iterables.asSet( cache.constraintRules() ) );
    }

    @Test
    public void should_add_schema_rules()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        // WHEN
        cache.addSchemaRule( hans );
        cache.addSchemaRule( gretel );
        cache.addSchemaRule( witch );

        // THEN
        assertEquals( asSet( hans, gretel ), Iterables.asSet( cache.indexRules() ) );
        assertEquals( asSet( witch ), Iterables.asSet( cache.constraintRules() ) );
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
        ConstraintDescriptor unique1 = ConstraintDescriptorFactory.uniqueForLabel( 1, 2 );
        ConstraintDescriptor unique2 = ConstraintDescriptorFactory.uniqueForLabel( 3, 4 );
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
                asSet( cache.constraintsForSchema( SchemaDescriptorFactory.forLabel( 1, 3 ) ) ) );

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
        ConstraintDescriptor dropped = ConstraintDescriptorFactory.uniqueForLabel( 1, 1 );
        ConstraintDescriptor unique = ConstraintDescriptorFactory.uniqueForLabel( 3, 4 );
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
    public void adding_constraints_should_be_idempotent() throws Exception
    {
        // given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( uniquenessConstraintRule( 0L, 1, 2, 133L ) );

        // when
        cache.addSchemaRule( uniquenessConstraintRule( 0L, 1, 2, 133L ) );

        // then
        assertEquals(
                asList( ConstraintDescriptorFactory.uniqueForLabel( 1, 2 ) ),
                Iterators.asList( cache.constraints() ) );
    }

    @Test
    public void shouldResolveIndexDescriptor() throws Exception
    {
        // Given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( newIndexRule( 1L, 1, 2 ) );
        cache.addSchemaRule( newIndexRule( 2L, 1, 3 ) );
        cache.addSchemaRule( newIndexRule( 3L, 2, 2 ) );

        // When
        LabelSchemaDescriptor schema = SchemaDescriptorFactory.forLabel( 1, 3 );
        IndexDescriptor descriptor = cache.indexDescriptor( schema );

        // Then
        assertThat( descriptor.schema(), equalTo( schema ) );
    }

    @Test
    public void shouldReturnNullWhenNoIndexExists()
    {
        // Given
        SchemaCache schemaCache = newSchemaCache();

        // When
        IndexDescriptor indexDescriptor = schemaCache.indexDescriptor( SchemaDescriptorFactory.forLabel( 1, 1 ) );

        // Then
        assertNull( indexDescriptor );
    }

    @Test
    public void shouldListConstraintsForLabel()
    {
        // Given
        ConstraintRule rule1 = uniquenessConstraintRule( 0, 1, 1, 0 );
        ConstraintRule rule2 = uniquenessConstraintRule( 1, 2, 1, 0 );
        ConstraintRule rule3 = nodePropertyExistenceConstraintRule( 2, 1, 2 );

        SchemaCache cache = newSchemaCache( rule1, rule2, rule3 );

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

        SchemaCache cache = newSchemaCache( rule1, rule2, rule3 );

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

        SchemaCache cache = newSchemaCache( rule1, rule2, rule3 );

        // When
        Set<ConstraintDescriptor> listed = asSet( cache.constraintsForRelationshipType( 1 ) );

        // Then
        Set<ConstraintDescriptor> expected = asSet( rule1.getConstraintDescriptor(), rule3.getConstraintDescriptor() );
        assertEquals( expected, listed );
    }

    private IndexRule newIndexRule( long id, int label, int propertyKey )
    {
        return IndexRule.indexRule( id, IndexDescriptorFactory.forLabel( label, propertyKey ), PROVIDER_DESCRIPTOR );
    }

    private ConstraintRule nodePropertyExistenceConstraintRule( long ruleId, int labelId, int propertyId )
    {
        return ConstraintRule.constraintRule(
                ruleId, ConstraintDescriptorFactory.existsForLabel( labelId, propertyId ) );
    }

    private ConstraintRule relPropertyExistenceConstraintRule( long ruleId, int relTypeId, int propertyId )
    {
        return ConstraintRule.constraintRule(
                ruleId, ConstraintDescriptorFactory.existsForRelType( relTypeId, propertyId ) );
    }

    private ConstraintRule uniquenessConstraintRule( long ruleId, int labelId, int propertyId, long indexId )
    {
        return ConstraintRule.constraintRule(
                ruleId, ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyId ), indexId );
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
