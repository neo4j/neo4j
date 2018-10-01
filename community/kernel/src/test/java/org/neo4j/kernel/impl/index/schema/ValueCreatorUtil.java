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

import org.apache.commons.lang3.ArrayUtils;

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
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class ValueCreatorUtil<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
{
    static final double FRACTION_DUPLICATE_UNIQUE = 0;
    static final double FRACTION_DUPLICATE_NON_UNIQUE = 0.1;
    private static final Comparator<IndexEntryUpdate<IndexDescriptor>> UPDATE_COMPARATOR = ( u1, u2 ) ->
            Values.COMPARATOR.compare( u1.values()[0], u2.values()[0] );
    private static final int N_VALUES = 10;

    final StoreIndexDescriptor indexDescriptor;
    private final RandomValues.Type[] supportedTypes;
    private final double fractionDuplication;

    ValueCreatorUtil( ValueCreatorUtil delegate )
    {
        this( delegate.indexDescriptor, delegate.supportedTypes, delegate.fractionDuplication );
    }

    ValueCreatorUtil( StoreIndexDescriptor indexDescriptor, RandomValues.Type[] supportedTypes, double fractionDuplication )
    {
        this.indexDescriptor = indexDescriptor;
        this.supportedTypes = supportedTypes;
        this.fractionDuplication = fractionDuplication;
    }

    int compareIndexedPropertyValue( KEY key1, KEY key2 )
    {
        return Values.COMPARATOR.compare( key1.asValues()[0], key2.asValues()[0] );
    }

    RandomValues.Type[] supportedTypes()
    {
        return supportedTypes;
    }

    double fractionDuplicates()
    {
        return fractionDuplication;
    }

    IndexQuery rangeQuery( Value from, boolean fromInclusive, Value to, boolean toInclusive )
    {
        return IndexQuery.range( 0, from, fromInclusive, to, toInclusive );
    }

    StoreIndexDescriptor indexDescriptor()
    {
        return indexDescriptor;
    }

    IndexEntryUpdate<IndexDescriptor>[] someUpdates( RandomRule randomRule )
    {
        Iterator<IndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator = randomUpdateGenerator( randomRule );
        //noinspection unchecked
        IndexEntryUpdate<IndexDescriptor>[] result = new IndexEntryUpdate[N_VALUES];
        for ( int i = 0; i < N_VALUES; i++ )
        {
            result[i] = randomUpdateGenerator.next();
        }
        return result;
    }

    Iterator<IndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator( RandomRule randomRule )
    {
        Iterator<Value> valueIterator = randomValueGenerator( randomRule );
        return new RandomUpdateGenerator( valueIterator );
    }

    private Iterator<Value> randomValueGenerator( RandomRule randomRule )
    {
        RandomValues randomValues = randomRule.randomValues();
        double fractionDuplicates = fractionDuplicates();
        return new RandomValueGenerator( fractionDuplicates, randomValues );
    }

    IndexEntryUpdate<IndexDescriptor>[] someUpdatesWithDuplicateValues( RandomRule randomRule )
    {
        Iterator<Value> valueIterator = randomValueGenerator( randomRule );
        Value[] someValues = new Value[N_VALUES];
        for ( int i = 0; i < N_VALUES; i++ )
        {
            someValues[i] = valueIterator.next();
        }
        return generateAddUpdatesFor( ArrayUtils.addAll( someValues, someValues ) );
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

    void copyValue( VALUE value, VALUE intoValue )
    {   // no-op until we decide to use value for something
    }

    private class RandomValueGenerator extends PrefetchingIterator<Value>
    {
        private final Set<Value> uniqueCompareValues;
        private final List<Value> uniqueValues;
        private final double fractionDuplicates;
        private final RandomValues randomValues;

        RandomValueGenerator( double fractionDuplicates, RandomValues randomValues )
        {
            this.fractionDuplicates = fractionDuplicates;
            this.randomValues = randomValues;
            this.uniqueCompareValues = new HashSet<>();
            this.uniqueValues = new ArrayList<>();
        }

        @Override
        protected Value fetchNextOrNull()
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

            return value;
        }

        private Value newUniqueValue( RandomValues random, Set<Value> uniqueCompareValues, List<Value> uniqueValues )
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
    }

    private class RandomUpdateGenerator extends PrefetchingIterator<IndexEntryUpdate<IndexDescriptor>>
    {
        private final Iterator<Value> valueIterator;
        private long currentEntityId;

        RandomUpdateGenerator( Iterator<Value> valueIterator )
        {
            this.valueIterator = valueIterator;
        }

        @Override
        protected IndexEntryUpdate<IndexDescriptor> fetchNextOrNull()
        {
            Value value = valueIterator.next();
            return add( currentEntityId++, value );
        }
    }
}
