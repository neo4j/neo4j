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
package org.neo4j.kernel.impl.index.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class LayoutTestUtil<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
{
    private static final Comparator<IndexEntryUpdate<IndexDescriptor>> UPDATE_COMPARATOR = ( u1, u2 ) ->
            Values.COMPARATOR.compare( u1.values()[0], u2.values()[0] );

    final StoreIndexDescriptor indexDescriptor;

    LayoutTestUtil( IndexDescriptor indexDescriptor )
    {
        this.indexDescriptor = indexDescriptor.withId( 0 );
    }

    abstract Layout<KEY,VALUE> createLayout();

    abstract IndexEntryUpdate<IndexDescriptor>[] someUpdates();

    protected double fractionDuplicates()
    {
        return 0.1;
    }

    abstract IndexQuery rangeQuery( Value from, boolean fromInclusive, Value to, boolean toInclusive );

    abstract int compareIndexedPropertyValue( KEY key1, KEY key2 );

    StoreIndexDescriptor indexDescriptor()
    {
        return indexDescriptor;
    }

    void copyValue( VALUE value, VALUE intoValue )
    {
    }

    Iterator<IndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator( RandomRule random )
    {
        double fractionDuplicates = fractionDuplicates();
        return new PrefetchingIterator<IndexEntryUpdate<IndexDescriptor>>()
        {
            private final Set<Object> uniqueCompareValues = new HashSet<>();
            private final List<Value> uniqueValues = new ArrayList<>();
            private long currentEntityId;

            @Override
            protected IndexEntryUpdate<IndexDescriptor> fetchNextOrNull()
            {
                Value value;
                if ( fractionDuplicates > 0 && !uniqueValues.isEmpty() &&
                     random.nextFloat() < fractionDuplicates )
                {
                    value = existingNonUniqueValue( random );
                }
                else
                {
                    value = newUniqueValue( random.randomValues(), uniqueCompareValues, uniqueValues );
                }

                return add( currentEntityId++, value );
            }

            private Value existingNonUniqueValue( RandomRule randomRule )
            {
                return uniqueValues.get( randomRule.nextInt( uniqueValues.size() ) );
            }
        };
    }

    abstract Value newUniqueValue( RandomValues random, Set<Object> uniqueCompareValues, List<Value> uniqueValues );

    Value[] extractValuesFromUpdates( IndexEntryUpdate<IndexDescriptor>[] updates )
    {
        Value[] values = new Value[updates.length];
        for ( int i = 0; i < updates.length; i++ )
        {
            if ( updates[i].values().length > 1 )
            {
                throw new UnsupportedOperationException( "This method does not support composite entries" );
            }
            values[i] = updates[i].values()[0];
        }
        return values;
    }

    abstract IndexEntryUpdate<IndexDescriptor>[] someUpdatesNoDuplicateValues();

    abstract IndexEntryUpdate<IndexDescriptor>[] someUpdatesWithDuplicateValues();

    IndexEntryUpdate<IndexDescriptor>[] generateAddUpdatesFor( Object[] values )
    {
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] indexEntryUpdates = new IndexEntryUpdate[values.length];
        for ( int i = 0; i < indexEntryUpdates.length; i++ )
        {
            indexEntryUpdates[i] = add( i, Values.of( values[i] ) );
        }
        return indexEntryUpdates;
    }

    protected IndexEntryUpdate<IndexDescriptor> add( long nodeId, Value value )
    {
        return IndexEntryUpdate.add( nodeId, indexDescriptor, value );
    }

    static int countUniqueValues( IndexEntryUpdate<IndexDescriptor>[] updates )
    {
        return Stream.of( updates ).map( update -> update.values()[0] ).collect( Collectors.toSet() ).size();
    }

    static int countUniqueValues( Object[] updates )
    {
        return Stream.of( updates ).collect( Collectors.toSet() ).size();
    }

    void sort( IndexEntryUpdate<IndexDescriptor>[] updates )
    {
        Arrays.sort( updates, UPDATE_COMPARATOR );
    }
}
