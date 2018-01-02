/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodePropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.PropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.store.record.NodePropertyExistenceConstraintRule.nodePropertyExistenceConstraintRule;
import static org.neo4j.kernel.impl.store.record.RelationshipPropertyExistenceConstraintRule.relPropertyExistenceConstraintRule;
import static org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule.uniquenessConstraintRule;

public class SchemaCacheTest
{
    final SchemaRule hans = newIndexRule( 1, 0, 5 );
    final SchemaRule witch = newIndexRule( 2, 3, 6 );
    final SchemaRule gretel = newIndexRule( 3, 0, 7 );

    @Test
    public void should_construct_schema_cache()
    {
        // GIVEN
        Collection<SchemaRule> rules = asList( hans, witch, gretel );
        SchemaCache cache = new SchemaCache( new StandardConstraintSemantics(), rules );

        // THEN
        assertEquals( asSet( hans, gretel ), asSet( cache.schemaRulesForLabel( 0 ) ) );
        assertEquals( asSet( witch ), asSet( cache.schemaRulesForLabel( 3 ) ) );
        assertEquals( asSet( rules ), asSet( cache.schemaRules() ) );
    }

    @Test
    public void should_add_schema_rules_to_a_label()
    {
        // GIVEN
        Collection<SchemaRule> rules = Collections.emptyList();
        SchemaCache cache = new SchemaCache( new StandardConstraintSemantics(), rules );

        // WHEN
        cache.addSchemaRule( hans );
        cache.addSchemaRule( gretel );

        // THEN
        assertEquals( asSet( hans, gretel ), asSet( cache.schemaRulesForLabel( 0 ) ) );
    }

    @Test
    public void should_to_retrieve_all_schema_rules()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        // WHEN
        cache.addSchemaRule( hans );
        cache.addSchemaRule( gretel );

        // THEN
        assertEquals( asSet( hans, gretel ), asSet( cache.schemaRules() ) );
    }

    @Test
    public void should_list_constraints()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        // WHEN
        cache.addSchemaRule( uniquenessConstraintRule( 0l, 1, 2, 133l ) );
        cache.addSchemaRule( uniquenessConstraintRule( 1l, 3, 4, 133l ) );
        cache.addSchemaRule( relPropertyExistenceConstraintRule( 2l, 5, 6 ) );
        cache.addSchemaRule( nodePropertyExistenceConstraintRule( 3l, 7, 8 ) );

        // THEN
        assertEquals(
                asSet( new UniquenessConstraint( 1, 2 ), new UniquenessConstraint( 3, 4 ),
                        new RelationshipPropertyExistenceConstraint( 5, 6 ),
                        new NodePropertyExistenceConstraint( 7, 8 ) ),
                asSet( cache.constraints() ) );

        assertEquals(
                asSet( new UniquenessConstraint( 1, 2 ) ),
                asSet( cache.constraintsForLabel( 1 ) ) );

        assertEquals(
                asSet( new UniquenessConstraint( 1, 2 ) ),
                asSet( cache.constraintsForLabelAndProperty( 1, 2 ) ) );

        assertEquals(
                asSet(),
                asSet( cache.constraintsForLabelAndProperty( 1, 3 ) ) );

        assertEquals(
                asSet( new RelationshipPropertyExistenceConstraint( 5, 6 ) ),
                asSet( cache.constraintsForRelationshipType( 5 ) ) );

        assertEquals(
                asSet( new RelationshipPropertyExistenceConstraint( 5, 6 ) ),
                asSet( cache.constraintsForRelationshipTypeAndProperty( 5, 6 ) ) );
    }

    @Test
    public void should_remove_constraints()
    {
        // GIVEN
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( uniquenessConstraintRule( 0l, 1, 2, 133l ) );
        cache.addSchemaRule( uniquenessConstraintRule( 1l, 3, 4, 133l ) );

        // WHEN
        cache.removeSchemaRule( 0l );

        // THEN
        assertEquals(
                asSet( new UniquenessConstraint( 3, 4 ) ),
                asSet( cache.constraints() ) );

        assertEquals(
                asSet(),
                asSet( cache.constraintsForLabel( 1 ) ) );

        assertEquals(
                asSet(),
                asSet( cache.constraintsForLabelAndProperty( 1, 2 ) ) );
    }

    @Test
    public void adding_constraints_should_be_idempotent() throws Exception
    {
        // given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( uniquenessConstraintRule( 0l, 1, 2, 133l ) );

        // when
        cache.addSchemaRule( uniquenessConstraintRule( 0l, 1, 2, 133l ) );

        // then
        assertEquals(
                asList( new UniquenessConstraint( 1, 2 ) ),
                IteratorUtil.asList( cache.constraints() ) );
    }

    @Test
    public void shouldResolveIndexDescriptor() throws Exception
    {
        // Given
        SchemaCache cache = newSchemaCache();

        cache.addSchemaRule( newIndexRule( 1l, 1, 2 ) );
        cache.addSchemaRule( newIndexRule( 2l, 1, 3 ) );
        cache.addSchemaRule( newIndexRule( 3l, 2, 2 ) );

        // When
        IndexDescriptor descriptor = cache.indexDescriptor( 1, 3 );

        // Then
        assertEquals( 1, descriptor.getLabelId() );
        assertEquals( 3, descriptor.getPropertyKeyId() );
    }

    @Test
    public void shouldReturnNullWhenNoIndexExists()
    {
        // Given
        SchemaCache schemaCache = newSchemaCache();

        // When
        IndexDescriptor indexDescriptor = schemaCache.indexDescriptor( 1, 1 );

        // Then
        assertNull( indexDescriptor );
    }

    @Test
    public void shouldListConstraintsForLabel()
    {
        // Given
        UniquePropertyConstraintRule rule1 = uniquenessConstraintRule( 0, 1, 1, 0 );
        UniquePropertyConstraintRule rule2 = uniquenessConstraintRule( 1, 2, 1, 0 );
        NodePropertyExistenceConstraintRule rule3 = nodePropertyExistenceConstraintRule( 2, 1, 2 );

        SchemaCache cache = newSchemaCache( rule1, rule2, rule3 );

        // When
        Set<NodePropertyConstraint> listed = asSet( cache.constraintsForLabel( 1 ) );

        // Then
        Set<NodePropertyConstraint> expected = asSet( rule1.toConstraint(), rule3.toConstraint() );
        assertEquals( expected, listed );
    }

    @Test
    public void shouldListConstraintsForLabelAndProperty()
    {
        // Given
        UniquePropertyConstraintRule rule1 = uniquenessConstraintRule( 0, 1, 1, 0 );
        UniquePropertyConstraintRule rule2 = uniquenessConstraintRule( 1, 2, 1, 0 );
        NodePropertyExistenceConstraintRule rule3 = nodePropertyExistenceConstraintRule( 2, 1, 2 );

        SchemaCache cache = newSchemaCache( rule1, rule2, rule3 );

        // When
        Set<NodePropertyConstraint> listed = asSet( cache.constraintsForLabelAndProperty( 1, 2 ) );

        // Then
        assertEquals( singleton( rule3.toConstraint() ), listed );
    }

    @Test
    public void shouldListConstraintsForRelationshipType()
    {
        // Given
        RelationshipPropertyExistenceConstraintRule rule1 = relPropertyExistenceConstraintRule( 0, 1, 1 );
        RelationshipPropertyExistenceConstraintRule rule2 = relPropertyExistenceConstraintRule( 0, 2, 1 );
        RelationshipPropertyExistenceConstraintRule rule3 = relPropertyExistenceConstraintRule( 0, 1, 2 );

        SchemaCache cache = newSchemaCache( rule1, rule2, rule3 );

        // When
        Set<RelationshipPropertyConstraint> listed = asSet( cache.constraintsForRelationshipType( 1 ) );

        // Then
        Set<RelationshipPropertyConstraint> expected = asSet( rule1.toConstraint(), rule3.toConstraint() );
        assertEquals( expected, listed );
    }

    @Test
    public void shouldListConstraintsForRelationshipTypeAndProperty()
    {
        // Given
        RelationshipPropertyExistenceConstraintRule rule1 = relPropertyExistenceConstraintRule( 0, 1, 1 );
        RelationshipPropertyExistenceConstraintRule rule2 = relPropertyExistenceConstraintRule( 0, 2, 1 );
        RelationshipPropertyExistenceConstraintRule rule3 = relPropertyExistenceConstraintRule( 0, 1, 2 );

        SchemaCache cache = newSchemaCache( rule1, rule2, rule3 );

        // When
        Set<RelationshipPropertyConstraint> listed = asSet( cache.constraintsForRelationshipTypeAndProperty( 2, 1 ) );

        // Then
        assertEquals( singleton( rule2.toConstraint() ), listed );
    }

    private IndexRule newIndexRule( long id, int label, int propertyKey )
    {
        return IndexRule.indexRule( id, label, propertyKey, PROVIDER_DESCRIPTOR );
    }

    private static SchemaCache newSchemaCache( SchemaRule... rules )
    {
        return new SchemaCache( new ConstraintSemantics(), (rules == null || rules.length == 0)
                                ? Collections.<SchemaRule>emptyList() : Arrays.asList( rules ) );
    }

    private static class ConstraintSemantics extends StandardConstraintSemantics
    {
        @Override
        protected PropertyConstraint readNonStandardConstraint( PropertyConstraintRule rule )
        {
            if ( rule instanceof NodePropertyExistenceConstraintRule )
            {
                return ((NodePropertyExistenceConstraintRule) rule).toConstraint();
            }
            if ( rule instanceof RelationshipPropertyExistenceConstraintRule )
            {
                return ((RelationshipPropertyExistenceConstraintRule) rule).toConstraint();
            }
            return super.readNonStandardConstraint( rule );
        }
    }
}
