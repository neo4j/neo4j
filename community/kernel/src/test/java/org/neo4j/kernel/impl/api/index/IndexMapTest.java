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
package org.neo4j.kernel.impl.api.index;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterableOf;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.emptySet;

public class IndexMapTest
{

    private IndexMap indexMap;

    private LabelSchemaDescriptor schema3_4 = SchemaDescriptorFactory.forLabel( 3, 4 );
    private LabelSchemaDescriptor schema5_6_7 = SchemaDescriptorFactory.forLabel( 5, 6, 7 );
    private LabelSchemaDescriptor schema5_8 = SchemaDescriptorFactory.forLabel( 5, 8 );

    @Before
    public void setup()
    {
        Map<Long, IndexProxy> map = new HashMap<>();
        map.put( 1L, new TestIndexProxy( schema3_4 ) );
        map.put( 2L, new TestIndexProxy( schema5_6_7 ) );
        map.put( 3L, new TestIndexProxy( schema5_8 ) );

        indexMap = new IndexMap( map );
    }

    @Test
    public void shouldGetRelatedIndexForLabel()
    {
        assertThat(
                indexMap.getRelatedIndexes( new long[]{3}, emptySet() ),
                containsInAnyOrder( schema3_4 ) );
    }

    @Test
    public void shouldGetRelatedIndexForProperty()
    {
        assertThat(
                indexMap.getRelatedIndexes( new long[]{}, properties( 4 ) ),
                containsInAnyOrder( schema3_4 ) );
    }

    @Test
    public void shouldGetRelatedIndexesForLabel()
    {
        assertThat(
                indexMap.getRelatedIndexes( new long[]{5}, emptySet() ),
                containsInAnyOrder( schema5_6_7, schema5_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexes()
    {
        assertThat(
                indexMap.getRelatedIndexes( new long[]{3}, properties( 7 ) ),
                containsInAnyOrder( schema3_4, schema5_6_7 ) );
    }

    @Test
    public void shouldGetRelatedIndexOnce()
    {
        assertThat(
                indexMap.getRelatedIndexes( new long[]{3}, properties( 4 ) ),
                containsInAnyOrder( schema3_4 ) );

        assertThat(
                indexMap.getRelatedIndexes( new long[]{}, properties( 6, 7 ) ),
                containsInAnyOrder( schema5_6_7 ) );
    }

    @Test
    public void shouldHandleUnrelated()
    {
        assertThat(
                indexMap.getRelatedIndexes( new long[]{}, emptySet() ),
                emptyIterableOf( LabelSchemaDescriptor.class ) );

        assertThat(
                indexMap.getRelatedIndexes( new long[]{2}, emptySet() ),
                emptyIterableOf( LabelSchemaDescriptor.class ) );

        assertThat(
                indexMap.getRelatedIndexes( new long[]{}, properties( 1 ) ),
                emptyIterableOf( LabelSchemaDescriptor.class ) );

        assertThat(
                indexMap.getRelatedIndexes( new long[]{2}, properties( 1 ) ),
                emptyIterableOf( LabelSchemaDescriptor.class ) );
    }

    private PrimitiveIntSet properties( int... propertyIds )
    {
        return PrimitiveIntCollections.asSet( propertyIds );
    }

    private class TestIndexProxy extends IndexProxyAdapter
    {
        private final LabelSchemaDescriptor schema;

        private TestIndexProxy( LabelSchemaDescriptor schema )
        {
            this.schema = schema;
        }

        @Override
        public LabelSchemaDescriptor schema()
        {
            return schema;
        }
    }
}
