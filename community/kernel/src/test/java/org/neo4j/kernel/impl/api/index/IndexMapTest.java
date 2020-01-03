/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.api.index;

import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterableOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.storageengine.api.EntityType.NODE;
import static org.neo4j.storageengine.api.EntityType.RELATIONSHIP;
import static org.neo4j.storageengine.api.schema.IndexDescriptorFactory.forSchema;

public class IndexMapTest
{
    private static final long[] noEntityToken = {};
    private IndexMap indexMap;

    private LabelSchemaDescriptor schema3_4 = SchemaDescriptorFactory.forLabel( 3, 4 );
    private LabelSchemaDescriptor schema5_6_7 = SchemaDescriptorFactory.forLabel( 5, 6, 7 );
    private LabelSchemaDescriptor schema5_8 = SchemaDescriptorFactory.forLabel( 5, 8 );
    private SchemaDescriptor node35_8 = SchemaDescriptorFactory.multiToken( new int[] {3,5}, NODE, 8 );
    private SchemaDescriptor rel35_8 = SchemaDescriptorFactory.multiToken( new int[] {3,5}, RELATIONSHIP, 8 );

    @Before
    public void setup()
    {
        indexMap = new IndexMap();
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( schema3_4 ).withId( 1 ).withoutCapabilities() ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( schema5_6_7 ).withId( 2 ).withoutCapabilities() ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( schema5_8 ).withId( 3 ).withoutCapabilities() ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( node35_8 ).withId( 4 ).withoutCapabilities() ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( rel35_8 ).withId( 5 ).withoutCapabilities() ) );
    }

    @Test
    public void shouldGetRelatedIndexForLabel()
    {
        assertThat( getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties(), false, NODE ),
                containsInAnyOrder( schema3_4, node35_8 ) );
    }

    private Set<SchemaDescriptor> getRelatedIndexes( long[] changedEntityTokens, long[] unchangedEntityTokens, int[] properties, boolean propertyListIsComplete,
            EntityType type )
    {
        return indexMap.getRelatedIndexes( changedEntityTokens, unchangedEntityTokens, properties, propertyListIsComplete, type ).stream().map(
                SchemaDescriptorSupplier::schema ).collect( toSet() );
    }

    @Test
    public void shouldGetRelatedIndexForProperty()
    {
        assertThat(
                getRelatedIndexes( noEntityToken, entityTokens( 3, 4, 5 ), properties( 4 ), false, NODE ),
                containsInAnyOrder( schema3_4 ) );
    }

    @Test
    public void shouldGetRelatedIndexesForLabel()
    {
        assertThat( getRelatedIndexes( entityTokens( 5 ), entityTokens( 3, 4 ), properties(), false, NODE ),
                containsInAnyOrder( schema5_6_7, schema5_8, node35_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexes()
    {
        assertThat(
                getRelatedIndexes( entityTokens( 3 ), entityTokens( 4, 5 ), properties( 7 ), false, NODE ),
                containsInAnyOrder( schema3_4, schema5_6_7, node35_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexOnce()
    {
        assertThat(
                getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties( 4 ), false, NODE ),
                containsInAnyOrder( schema3_4, node35_8 ) );

        assertThat(
                getRelatedIndexes( noEntityToken, entityTokens( 5 ), properties( 6, 7 ), false, NODE ),
                containsInAnyOrder( schema5_6_7 ) );
    }

    @Test
    public void shouldHandleUnrelated()
    {
        assertThat( getRelatedIndexes( noEntityToken, noEntityToken, properties(), false, NODE ),
                emptyIterableOf( SchemaDescriptor.class ) );

        assertTrue( getRelatedIndexes( entityTokens( 2 ), noEntityToken, properties(), false, NODE ).isEmpty() );

        assertThat(
                getRelatedIndexes( noEntityToken, entityTokens( 2 ), properties( 1 ), false, NODE ),
                emptyIterableOf( SchemaDescriptor.class ) );

        assertTrue( getRelatedIndexes( entityTokens( 2 ), entityTokens( 2 ), properties( 1 ), false, NODE ).isEmpty() );
    }

    @Test
    public void shouldGetMultiLabelForAnyOfTheLabels()
    {
        assertThat( getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties(), false, NODE ),
                containsInAnyOrder( schema3_4, node35_8 ) );

        assertThat( getRelatedIndexes( entityTokens( 5 ), noEntityToken, properties(), false, NODE ),
                containsInAnyOrder( schema5_8, schema5_6_7, node35_8 ) );
    }

    @Test
    public void shouldOnlyGetRelIndexesForRelUpdates()
    {
        assertThat( getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties(), false, RELATIONSHIP ),
                containsInAnyOrder( rel35_8 ) );

        assertThat( getRelatedIndexes( entityTokens( 5 ), noEntityToken, properties(), false, RELATIONSHIP ),
                containsInAnyOrder( rel35_8 ) );
    }

    @Test
    public void removalsShouldOnlyRemoveCorrectProxy()
    {
        indexMap.removeIndexProxy( 4 );
        assertThat( getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties(), false, NODE ),
                containsInAnyOrder( schema3_4 ) );
        assertThat( getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties(), false, RELATIONSHIP ),
                containsInAnyOrder( rel35_8 ) );

        indexMap.removeIndexProxy( 7 );
        assertThat( getRelatedIndexes( entityTokens( 5 ), noEntityToken, properties(), false, NODE ),
                containsInAnyOrder( schema5_8, schema5_6_7 ) );
        assertThat( getRelatedIndexes( entityTokens( 5 ), noEntityToken, properties(), false, RELATIONSHIP ),
                containsInAnyOrder( rel35_8 ) );
    }

    @Test
    public void shouldGetRelatedNodeConstraints()
    {
        // given
        ConstraintRule constraint1 = ConstraintRule.constraintRule( 1L, ConstraintDescriptorFactory.uniqueForLabel( 1, 5, 6 ), null );
        ConstraintRule constraint2 = ConstraintRule.constraintRule( 2L, ConstraintDescriptorFactory.uniqueForLabel( 1, 5 ), null );
        ConstraintRule constraint3 = ConstraintRule.constraintRule( 3L, ConstraintDescriptorFactory.uniqueForLabel( 2, 5 ), null );
        indexMap.putUniquenessConstraint( constraint1 );
        indexMap.putUniquenessConstraint( constraint2 );
        indexMap.putUniquenessConstraint( constraint3 );

        // when/then
        assertEquals(
                asSet( constraint2.getConstraintDescriptor() ),
                indexMap.getRelatedConstraints( entityTokens( 1 ), entityTokens(), properties( 5 ), true, NODE ) );
        assertEquals(
                asSet( constraint1.getConstraintDescriptor(), constraint2.getConstraintDescriptor() ),
                indexMap.getRelatedConstraints( entityTokens( 1 ), entityTokens(), properties( 5 ), false, NODE ) );
        assertEquals(
                asSet( constraint1.getConstraintDescriptor(), constraint2.getConstraintDescriptor() ),
                indexMap.getRelatedConstraints( entityTokens( 1 ), entityTokens(), properties( 5, 6 ), true, NODE ) );
        assertEquals(
                asSet( constraint1.getConstraintDescriptor(), constraint2.getConstraintDescriptor() ),
                indexMap.getRelatedConstraints( entityTokens(), entityTokens( 1 ), properties( 5 ), false, NODE ) );
        assertEquals(
                asSet( constraint1.getConstraintDescriptor(), constraint2.getConstraintDescriptor(), constraint3.getConstraintDescriptor() ),
                indexMap.getRelatedConstraints( entityTokens( 1, 2 ), entityTokens(), properties(), false, NODE ) );
    }

    @Test
    public void shouldRemoveNodeConstraints()
    {
        // given
        ConstraintRule constraint1 = ConstraintRule.constraintRule( 1L, ConstraintDescriptorFactory.uniqueForLabel( 1, 5, 6 ), null );
        ConstraintRule constraint2 = ConstraintRule.constraintRule( 2L, ConstraintDescriptorFactory.uniqueForLabel( 1, 5 ), null );
        ConstraintRule constraint3 = ConstraintRule.constraintRule( 3L, ConstraintDescriptorFactory.uniqueForLabel( 2, 5 ), null );
        indexMap.putUniquenessConstraint( constraint1 );
        indexMap.putUniquenessConstraint( constraint2 );
        indexMap.putUniquenessConstraint( constraint3 );
        assertEquals(
                asSet( constraint2.getConstraintDescriptor() ),
                indexMap.getRelatedConstraints( entityTokens( 1 ), entityTokens(), properties( 5 ), true, NODE ) );

        // and when
        indexMap.removeUniquenessConstraint( constraint1.getId() );
        indexMap.removeUniquenessConstraint( constraint2.getId() );
        indexMap.removeUniquenessConstraint( constraint3.getId() );

        // then
        assertTrue( indexMap.getRelatedConstraints( entityTokens( 1 ), entityTokens(), properties( 5 ), true, NODE ).isEmpty() );
    }

    // HELPERS

    private long[] entityTokens( long... entityTokenIds )
    {
        return entityTokenIds;
    }

    private int[] properties( int... propertyIds )
    {
        return propertyIds;
    }

    private class TestIndexProxy extends IndexProxyAdapter
    {
        private final CapableIndexDescriptor descriptor;

        private TestIndexProxy( CapableIndexDescriptor descriptor )
        {
            this.descriptor = descriptor;
        }

        @Override
        public CapableIndexDescriptor getDescriptor()
        {
            return descriptor;
        }
    }
}
