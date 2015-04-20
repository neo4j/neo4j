/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Collection;

import org.junit.Test;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.store.UniquenessConstraintRule.uniquenessConstraintRule;

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
        SchemaCache cache = new SchemaCache( rules );

        // THEN
        assertEquals( asSet( hans, gretel ), asSet( cache.schemaRulesForLabel( 0 ) ) );
        assertEquals( asSet( witch ), asSet( cache.schemaRulesForLabel( 3 ) ) );
        assertEquals( asSet( rules ), asSet( cache.schemaRules() ) );
    }

    @Test
    public void should_add_schema_rules_to_a_label() {
        // GIVEN
        Collection<SchemaRule> rules = asList();
        SchemaCache cache = new SchemaCache( rules );

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
        Collection<SchemaRule> rules = asList();
        SchemaCache cache = new SchemaCache( rules );

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
        Collection<SchemaRule> rules = asList();
        SchemaCache cache = new SchemaCache( rules );

        // WHEN
        cache.addSchemaRule( uniquenessConstraintRule( 0l, 1, 2, 133l ) );
        cache.addSchemaRule( uniquenessConstraintRule( 1l, 3, 4, 133l ) );

        // THEN
        assertEquals(
                asSet( new UniquenessConstraint( 1, 2 ), new UniquenessConstraint( 3, 4 ) ),
                asSet( cache.constraints() ) );

        assertEquals(
                asSet( new UniquenessConstraint( 1, 2 ) ),
                asSet( cache.constraintsForLabel( 1 ) ) );

        assertEquals(
                asSet( new UniquenessConstraint( 1, 2 ) ),
                asSet( cache.constraintsForLabelAndProperty( 1, 2 ) ) );

        assertEquals(
                asSet( ),
                asSet( cache.constraintsForLabelAndProperty( 1, 3 ) ) );
    }

    @Test
    public void should_remove_constraints()
    {
        // GIVEN
        Collection<SchemaRule> rules = asList();
        SchemaCache cache = new SchemaCache( rules );

        cache.addSchemaRule( uniquenessConstraintRule( 0l, 1, 2, 133l ) );
        cache.addSchemaRule( uniquenessConstraintRule( 1l, 3, 4, 133l ) );

        // WHEN
        cache.removeSchemaRule( 0l );

        // THEN
        assertEquals(
                asSet( new UniquenessConstraint( 3, 4 ) ),
                asSet( cache.constraints() ) );

        assertEquals(
                asSet(  ),
                asSet( cache.constraintsForLabel( 1 )) );

        assertEquals(
                asSet(  ),
                asSet( cache.constraintsForLabelAndProperty( 1, 2 ) ) );
    }

    @Test
    public void adding_constraints_should_be_idempotent() throws Exception
    {
        // given
        Collection<SchemaRule> rules = asList();
        SchemaCache cache = new SchemaCache( rules );

        cache.addSchemaRule( uniquenessConstraintRule( 0l, 1, 2, 133l ) );

        // when
        cache.addSchemaRule( uniquenessConstraintRule( 0l, 1, 2, 133l ) );

        // then
        assertEquals(
                asList( new UniquenessConstraint( 1, 2 ) ),
                IteratorUtil.asList( cache.constraints() ) );
    }

    @Test
    public void shouldResolveIndexId() throws Exception
    {
        // Given
        Collection<SchemaRule> rules = asList();
        SchemaCache cache = new SchemaCache( rules );

        cache.addSchemaRule( newIndexRule( 1l, 1, 2 ) );
        cache.addSchemaRule( newIndexRule( 2l, 1, 3 ) );
        cache.addSchemaRule( newIndexRule( 3l, 2, 2 ) );

        // When
        long indexId = cache.indexId( new IndexDescriptor( 1, 3 ) );

        // Then
        assertThat(indexId, equalTo(2l));
    }

    @Test
    public void shouldResolveIndexDescriptor() throws Exception
    {
        // Given
        Collection<SchemaRule> rules = asList();
        SchemaCache cache = new SchemaCache( rules );

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
        SchemaCache schemaCache = new SchemaCache( Iterables.<SchemaRule>empty() );

        // When
        IndexDescriptor indexDescriptor = schemaCache.indexDescriptor( 1, 1 );

        // Then
        assertNull( indexDescriptor );
    }

    private IndexRule newIndexRule( long id, int label, int propertyKey )
    {
        return IndexRule.indexRule( id, label, propertyKey, PROVIDER_DESCRIPTOR );
    }
}
