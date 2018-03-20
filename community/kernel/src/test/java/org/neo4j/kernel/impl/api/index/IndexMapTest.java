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
package org.neo4j.kernel.impl.api.index;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterableOf;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.emptySet;
import static org.neo4j.storageengine.api.EntityType.NODE;
import static org.neo4j.storageengine.api.EntityType.RELATIONSHIP;

public class IndexMapTest
{

    private static final long[] noEntityToken = {};
    private IndexMap indexMap;

    private LabelSchemaDescriptor schema3_4 = SchemaDescriptorFactory.forLabel( 3, 4 );
    private LabelSchemaDescriptor schema5_6_7 = SchemaDescriptorFactory.forLabel( 5, 6, 7 );
    private LabelSchemaDescriptor schema5_8 = SchemaDescriptorFactory.forLabel( 5, 8 );
    private SchemaDescriptor node35_8 = SchemaDescriptorFactory.multiToken( new int[] {3,5}, NODE, 8 );
    private SchemaDescriptor rel35_8 = SchemaDescriptorFactory.multiToken( new int[] {3,5}, RELATIONSHIP, 8 );
    private SchemaDescriptor anynode_8 = SchemaDescriptorFactory.multiToken( new int[0], NODE, 8 );
    private SchemaDescriptor anyrel_8 = SchemaDescriptorFactory.multiToken( new int[0], RELATIONSHIP, 8 );

    @Before
    public void setup()
    {
        PrimitiveLongObjectMap<IndexProxy> map = Primitive.longObjectMap();
        map.put( 1L, new TestIndexProxy( schema3_4 ) );
        map.put( 2L, new TestIndexProxy( schema5_6_7 ) );
        map.put( 3L, new TestIndexProxy( schema5_8 ) );
        map.put( 4L, new TestIndexProxy( node35_8 ) );
        map.put( 5L, new TestIndexProxy( rel35_8 ) );
        map.put( 6L, new TestIndexProxy( anynode_8) );
        map.put( 7L, new TestIndexProxy( anyrel_8 ) );
        indexMap = new IndexMap( map );
    }

    @Test
    public void shouldGetRelatedIndexForLabel()
    {
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, emptySet(), NODE ),
                containsInAnyOrder( schema3_4, node35_8, anynode_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexForProperty()
    {
        assertThat(
                indexMap.getRelatedIndexes( noEntityToken, entityTokens( 3, 4, 5 ), properties( 4 ), NODE ),
                containsInAnyOrder( schema3_4 ) );
    }

    @Test
    public void shouldGetRelatedIndexesForLabel()
    {
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 5 ), entityTokens( 3, 4 ), emptySet(), NODE ),
                containsInAnyOrder( schema5_6_7, schema5_8, anynode_8, node35_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexes()
    {
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 3 ), entityTokens( 4, 5 ), properties( 7 ), NODE ),
                containsInAnyOrder( schema3_4, schema5_6_7, node35_8, anynode_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexOnce()
    {
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, properties( 4 ), NODE ),
                containsInAnyOrder( schema3_4, node35_8, anynode_8 ) );

        assertThat(
                indexMap.getRelatedIndexes( noEntityToken, entityTokens( 5 ), properties( 6, 7 ), NODE ),
                containsInAnyOrder( schema5_6_7 ) );
    }

    @Test
    public void shouldHandleUnrelated()
    {
        assertThat(
                indexMap.getRelatedIndexes( noEntityToken, noEntityToken, emptySet(), NODE ),
                emptyIterableOf( SchemaDescriptor.class ) );

        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 2 ), noEntityToken, emptySet(), NODE ),
                containsInAnyOrder( anynode_8 ) );

        assertThat(
                indexMap.getRelatedIndexes( noEntityToken, entityTokens( 2 ), properties( 1 ), NODE ),
                emptyIterableOf( SchemaDescriptor.class ) );

        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 2 ), entityTokens( 2 ), properties( 1 ), NODE ),
                containsInAnyOrder( anynode_8 ) );
    }

    @Test
    public void shouldGetMultillabelForAnyOfTheLabels()
    {
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, emptySet(), NODE ),
                containsInAnyOrder( schema3_4, node35_8, anynode_8 ) );

        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 5 ), noEntityToken, emptySet(), NODE ),
                containsInAnyOrder( schema5_8, schema5_6_7, node35_8, anynode_8 ) );
    }

    @Test
    public void shouldOnlyGetRelIndexesForRelUpdates()
    {
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, emptySet(), RELATIONSHIP ),
                containsInAnyOrder( rel35_8, anyrel_8 ) );

        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 5 ), noEntityToken, emptySet(), RELATIONSHIP ),
                containsInAnyOrder( rel35_8, anyrel_8 ) );
    }

    @Test
    public void shouldGetAnynodeForAnyNodetype()
    {
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 1 ), noEntityToken, emptySet(), NODE ),
                containsInAnyOrder( anynode_8 ) );

        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 13 ), noEntityToken, emptySet(), NODE ),
                containsInAnyOrder( anynode_8 ) );
    }

    @Test
    public void shouldGetAnyrelForAnyReltype()
    {
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 1 ), noEntityToken, emptySet(), RELATIONSHIP ),
                containsInAnyOrder( anyrel_8 ) );

        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 13 ), noEntityToken, emptySet(), RELATIONSHIP ),
                containsInAnyOrder( anyrel_8 ) );
    }

    @Test
    public void shouldGetAnynodeForRelevantProperty()
    {
        assertThat(
                indexMap.getRelatedIndexes( noEntityToken, noEntityToken, properties(8), NODE ),
                containsInAnyOrder( anynode_8 ) );
    }

    @Test
    public void shouldGetAnyrelForRelevantProperty()
    {
        assertThat(
                indexMap.getRelatedIndexes( noEntityToken, noEntityToken, properties(8), RELATIONSHIP ),
                containsInAnyOrder( anyrel_8 ) );
    }

    @Test
    public void removalsShouldOnlyRemoveCorrectProxy()
    {
        indexMap.removeIndexProxy( 4 );
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, emptySet(), NODE ),
                containsInAnyOrder( schema3_4, anynode_8 ) );
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 3 ), noEntityToken, emptySet(), RELATIONSHIP ),
                containsInAnyOrder( rel35_8, anyrel_8 ) );

        indexMap.removeIndexProxy( 7 );
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 5 ), noEntityToken, emptySet(), NODE ),
                containsInAnyOrder( schema5_8, schema5_6_7, anynode_8 ) );
        assertThat(
                indexMap.getRelatedIndexes( entityTokens( 5 ), noEntityToken, emptySet(), RELATIONSHIP ),
                containsInAnyOrder( rel35_8 ) );

    }

    // HELPERS

    private long[] entityTokens( long... entityTokenIds )
    {
        return entityTokenIds;
    }

    private PrimitiveIntSet properties( int... propertyIds )
    {
        return PrimitiveIntCollections.asSet( propertyIds );
    }

    private class TestIndexProxy extends IndexProxyAdapter
    {
        private final SchemaDescriptor schema;

        private TestIndexProxy( SchemaDescriptor schema )
        {
            this.schema = schema;
        }

        @Override
        public SchemaDescriptor schema()
        {
            return schema;
        }
    }
}
