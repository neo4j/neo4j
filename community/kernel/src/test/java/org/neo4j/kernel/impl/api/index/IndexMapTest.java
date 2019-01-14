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

public class IndexMapTest
{

    private static final long[] noLabel = {};
    private IndexMap indexMap;

    private LabelSchemaDescriptor schema3_4 = SchemaDescriptorFactory.forLabel( 3, 4 );
    private LabelSchemaDescriptor schema5_6_7 = SchemaDescriptorFactory.forLabel( 5, 6, 7 );
    private LabelSchemaDescriptor schema5_8 = SchemaDescriptorFactory.forLabel( 5, 8 );

    @Before
    public void setup()
    {
        PrimitiveLongObjectMap<IndexProxy> map = Primitive.longObjectMap();
        map.put( 1L, new TestIndexProxy( schema3_4 ) );
        map.put( 2L, new TestIndexProxy( schema5_6_7 ) );
        map.put( 3L, new TestIndexProxy( schema5_8 ) );
        indexMap = new IndexMap( map );
    }

    @Test
    public void shouldGetRelatedIndexForLabel()
    {
        assertThat(
                indexMap.getRelatedIndexes( label( 3 ), noLabel, emptySet() ),
                containsInAnyOrder( schema3_4 ) );
    }

    @Test
    public void shouldGetRelatedIndexForProperty()
    {
        assertThat(
                indexMap.getRelatedIndexes( noLabel, label( 3, 4, 5 ), properties( 4 ) ),
                containsInAnyOrder( schema3_4 ) );
    }

    @Test
    public void shouldGetRelatedIndexesForLabel()
    {
        assertThat(
                indexMap.getRelatedIndexes( label( 5 ), label( 3, 4 ), emptySet() ),
                containsInAnyOrder( schema5_6_7, schema5_8 ) );
    }

    @Test
    public void shouldGetRelatedIndexes()
    {
        assertThat(
                indexMap.getRelatedIndexes( label( 3 ), label( 4, 5 ), properties( 7 ) ),
                containsInAnyOrder( schema3_4, schema5_6_7 ) );
    }

    @Test
    public void shouldGetRelatedIndexOnce()
    {
        assertThat(
                indexMap.getRelatedIndexes( label( 3 ), noLabel, properties( 4 ) ),
                containsInAnyOrder( schema3_4 ) );

        assertThat(
                indexMap.getRelatedIndexes( noLabel, label( 5 ), properties( 6, 7 ) ),
                containsInAnyOrder( schema5_6_7 ) );
    }

    @Test
    public void shouldHandleUnrelated()
    {
        assertThat(
                indexMap.getRelatedIndexes( noLabel, noLabel, emptySet() ),
                emptyIterableOf( SchemaDescriptor.class ) );

        assertThat(
                indexMap.getRelatedIndexes( label( 2 ), noLabel, emptySet() ),
                emptyIterableOf( SchemaDescriptor.class ) );

        assertThat(
                indexMap.getRelatedIndexes( noLabel, label( 2 ), properties( 1 ) ),
                emptyIterableOf( SchemaDescriptor.class ) );

        assertThat(
                indexMap.getRelatedIndexes( label( 2 ), label( 2 ), properties( 1 ) ),
                emptyIterableOf( SchemaDescriptor.class ) );
    }

    // HELPERS

    private long[] label( long... labels )
    {
        return labels;
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
