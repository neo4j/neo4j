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
package org.neo4j.kernel.impl.api.index;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterableOf;
import static org.junit.Assert.assertTrue;
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
        assertThat( indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties(), false, NODE ),
                containsInAnyOrder( schema3_4, node35_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexForProperty()
    {
        assertThat(
                indexMap.getRelatedIndexes( noEntityToken, entityTokens( 3, 4, 5 ), properties( 4 ), false, NODE ),
                containsInAnyOrder( schema3_4 ) );
    }

    @Test
    public void shouldGetRelatedIndexesForLabel()
    {
        assertThat( indexMap.getRelatedIndexes( entityTokens( 5 ), entityTokens( 3, 4 ), properties(), false, NODE ),
                containsInAnyOrder( schema5_6_7, schema5_8, node35_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexes()
    {
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 3 ), entityTokens( 4, 5 ), properties( 7 ), false, NODE ),
                containsInAnyOrder( schema3_4, schema5_6_7, node35_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexOnce()
    {
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties( 4 ), false, NODE ),
                containsInAnyOrder( schema3_4, node35_8 ) );

        assertThat(
                indexMap.getRelatedIndexes( noEntityToken, entityTokens( 5 ), properties( 6, 7 ), false, NODE ),
                containsInAnyOrder( schema5_6_7 ) );
    }

    @Test
    public void shouldHandleUnrelated()
    {
        assertThat( indexMap.getRelatedIndexes( noEntityToken, noEntityToken, properties(), false, NODE ),
                emptyIterableOf( SchemaDescriptor.class ) );

        assertTrue( indexMap.getRelatedIndexes( entityTokens( 2 ), noEntityToken, properties(), false, NODE ).isEmpty() );

        assertThat(
                indexMap.getRelatedIndexes( noEntityToken, entityTokens( 2 ), properties( 1 ), false, NODE ),
                emptyIterableOf( SchemaDescriptor.class ) );

        assertTrue( indexMap.getRelatedIndexes( entityTokens( 2 ), entityTokens( 2 ), properties( 1 ), false, NODE ).isEmpty() );
    }

    @Test
    public void shouldGetMultiLabelForAnyOfTheLabels()
    {
        assertThat( indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties(), false, NODE ),
                containsInAnyOrder( schema3_4, node35_8 ) );

        assertThat( indexMap.getRelatedIndexes( entityTokens( 5 ), noEntityToken, properties(), false, NODE ),
                containsInAnyOrder( schema5_8, schema5_6_7, node35_8 ) );
    }

    @Test
    public void shouldOnlyGetRelIndexesForRelUpdates()
    {
        assertThat( indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties(), false, RELATIONSHIP ),
                containsInAnyOrder( rel35_8 ) );

        assertThat( indexMap.getRelatedIndexes( entityTokens( 5 ), noEntityToken, properties(), false, RELATIONSHIP ),
                containsInAnyOrder( rel35_8 ) );
    }

    @Test
    public void removalsShouldOnlyRemoveCorrectProxy()
    {
        indexMap.removeIndexProxy( 4 );
        assertThat( indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties(), false, NODE ),
                containsInAnyOrder( schema3_4 ) );
        assertThat( indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties(), false, RELATIONSHIP ),
                containsInAnyOrder( rel35_8 ) );

        indexMap.removeIndexProxy( 7 );
        assertThat( indexMap.getRelatedIndexes( entityTokens( 5 ), noEntityToken, properties(), false, NODE ),
                containsInAnyOrder( schema5_8, schema5_6_7 ) );
        assertThat( indexMap.getRelatedIndexes( entityTokens( 5 ), noEntityToken, properties(), false, RELATIONSHIP ),
                containsInAnyOrder( rel35_8 ) );
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
