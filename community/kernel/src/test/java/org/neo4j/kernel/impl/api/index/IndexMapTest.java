/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.CapableIndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterableOf;

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
        MutableLongObjectMap<IndexProxy> map = new LongObjectHashMap<>();
        map.put( 1L, new TestIndexProxy( IndexDescriptorFactory.forSchema( schema3_4 ).withId( 1 ).withoutCapabilities() ) );
        map.put( 2L, new TestIndexProxy( IndexDescriptorFactory.forSchema( schema5_6_7 ).withId( 2 ).withoutCapabilities() ) );
        map.put( 3L, new TestIndexProxy( IndexDescriptorFactory.forSchema( schema5_8 ).withId( 3 ).withoutCapabilities() ) );
        indexMap = new IndexMap( map );
    }

    @Test
    public void shouldGetRelatedIndexForLabel()
    {
        assertThat(
                indexMap.getRelatedIndexes( label( 3 ), noLabel, IntSets.immutable.empty() ),
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
                indexMap.getRelatedIndexes( label( 5 ), label( 3, 4 ), IntSets.immutable.empty() ),
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
                indexMap.getRelatedIndexes( noLabel, noLabel, IntSets.immutable.empty() ),
                emptyIterableOf( SchemaDescriptor.class ) );

        assertThat(
                indexMap.getRelatedIndexes( label( 2 ), noLabel, IntSets.immutable.empty() ),
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

    private IntSet properties( int... propertyIds )
    {
        return new IntHashSet( propertyIds );
    }

    private class TestIndexProxy extends IndexProxyAdapter
    {
        private final CapableIndexDescriptor schema;

        private TestIndexProxy( CapableIndexDescriptor schema )
        {
            this.schema = schema;
        }

        @Override
        public CapableIndexDescriptor getDescriptor()
        {
            return schema;
        }
    }
}
