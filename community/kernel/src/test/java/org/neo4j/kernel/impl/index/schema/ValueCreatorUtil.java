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
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class ValueCreatorUtil<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
{
    private static final Comparator<IndexEntryUpdate<IndexDescriptor>> UPDATE_COMPARATOR = ( u1, u2 ) ->
            Values.COMPARATOR.compare( u1.values()[0], u2.values()[0] );
    private static final int N_VALUES = 10;

    final StoreIndexDescriptor indexDescriptor;
    private RandomValues randomValues;

    /**
     * Don't forget to {@link #setRandom(RandomValues)}. Done separately to make it easier to reset random while debugging.
     */
    ValueCreatorUtil( StoreIndexDescriptor indexDescriptor )
    {
        this.indexDescriptor = indexDescriptor;
    }

    void setRandom( RandomValues randomValues )
    {
        this.randomValues = randomValues;
    }

    IndexEntryUpdate<IndexDescriptor>[] someUpdates()
    {
        Iterator<IndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator = randomUpdateGenerator();
        //noinspection unchecked
        IndexEntryUpdate<IndexDescriptor>[] result = new IndexEntryUpdate[N_VALUES];
        for ( int i = 0; i < N_VALUES; i++ )
        {
            result[i] = randomUpdateGenerator.next();
        }
        return result;
    }

    abstract RandomValues.Type[] supportedTypes();

    protected double fractionDuplicates()
    {
        return 0.1;
    }

    IndexQuery rangeQuery( Value from, boolean fromInclusive, Value to, boolean toInclusive )
    {
        return IndexQuery.range( 0, from, fromInclusive, to, toInclusive );
    }

    abstract int compareIndexedPropertyValue( KEY key1, KEY key2 );

    StoreIndexDescriptor indexDescriptor()
    {
        return indexDescriptor;
    }

    void copyValue( VALUE value, VALUE intoValue )
    {   // no-op until we decide to use value for something
    }

    Iterator<IndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator()
    {
        double fractionDuplicates = fractionDuplicates();
        return new PrefetchingIterator<IndexEntryUpdate<IndexDescriptor>>()
        {
            private final Set<Value> uniqueCompareValues = new HashSet<>();
            private final List<Value> uniqueValues = new ArrayList<>();
            private long currentEntityId;

            @Override
            protected IndexEntryUpdate<IndexDescriptor> fetchNextOrNull()
            {
                Value value;
                if ( fractionDuplicates > 0 && !uniqueValues.isEmpty() &&
                     randomValues.nextFloat() < fractionDuplicates )
                {
                    value = randomValues.among( uniqueValues );
                }
                else
                {
                    value = newUniqueValue( randomValues, uniqueCompareValues, uniqueValues );
                }

                return add( currentEntityId++, value );
            }

        };
    }

    Value newUniqueValue( RandomValues random, Set<Value> uniqueCompareValues, List<Value> uniqueValues )
    {
        Value value;
        do
        {
            value = random.nextValueOfTypes( supportedTypes() );
        }
        while ( !uniqueCompareValues.add( value ) );
        uniqueValues.add( value );
        return value;
    }

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
        return generateAddUpdatesFor( Arrays.stream( values )
                .map( Values::of )
                .toArray( Value[]::new ) );
    }

    IndexEntryUpdate<IndexDescriptor>[] generateAddUpdatesFor( Value[] values )
    {
        //noinspection unchecked
        IndexEntryUpdate<IndexDescriptor>[] indexEntryUpdates = new IndexEntryUpdate[values.length];
        for ( int i = 0; i < indexEntryUpdates.length; i++ )
        {
            indexEntryUpdates[i] = add( i, values[i] );
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
