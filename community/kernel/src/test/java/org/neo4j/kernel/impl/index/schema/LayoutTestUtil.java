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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
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
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class LayoutTestUtil<KEY extends NativeSchemaKey, VALUE extends NativeSchemaValue>
{
    final SchemaIndexDescriptor schemaIndexDescriptor;

    LayoutTestUtil( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        this.schemaIndexDescriptor = schemaIndexDescriptor;
    }

    abstract Layout<KEY,VALUE> createLayout();

    abstract IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdates();

    protected abstract double fractionDuplicates();

    abstract IndexQuery rangeQuery( Number from, boolean fromInclusive, Number to, boolean toInclusive );

    abstract Value asValue( Number value );

    abstract int compareIndexedPropertyValue( KEY key1, KEY key2 );

    SchemaIndexDescriptor indexDescriptor()
    {
        return schemaIndexDescriptor;
    }

    void copyValue( VALUE value, VALUE intoValue )
    {
    }

    Iterator<IndexEntryUpdate<SchemaIndexDescriptor>> randomUpdateGenerator( RandomRule random )
    {
        double fractionDuplicates = fractionDuplicates();
        return new PrefetchingIterator<IndexEntryUpdate<SchemaIndexDescriptor>>()
        {
            private final Set<Double> uniqueCompareValues = new HashSet<>();
            private final List<Value> uniqueValues = new ArrayList<>();
            private long currentEntityId;

            @Override
            protected IndexEntryUpdate<SchemaIndexDescriptor> fetchNextOrNull()
            {
                Value value;
                if ( fractionDuplicates > 0 && !uniqueValues.isEmpty() &&
                        random.nextFloat() < fractionDuplicates )
                {
                    value = existingNonUniqueValue( random );
                }
                else
                {
                    value = newUniqueValue( random );
                }

                return add( currentEntityId++, value );
            }

            private Value newUniqueValue( RandomRule randomRule )
            {
                Number value;
                Double compareValue;
                do
                {
                    value = randomRule.numberPropertyValue();
                    compareValue = value.doubleValue();
                }
                while ( !uniqueCompareValues.add( compareValue ) );
                Value storableValue = asValue( value );
                uniqueValues.add( storableValue );
                return storableValue;
            }

            private Value existingNonUniqueValue( RandomRule randomRule )
            {
                return uniqueValues.get( randomRule.nextInt( uniqueValues.size() ) );
            }
        };
    }

    Value[] extractValuesFromUpdates( IndexEntryUpdate<SchemaIndexDescriptor>[] updates )
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

    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdatesNoDuplicateValues()
    {
        return generateAddUpdatesFor( ALL_EXTREME_VALUES );
    }

    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdatesWithDuplicateValues()
    {
        return generateAddUpdatesFor( ArrayUtils.addAll( ALL_EXTREME_VALUES, ALL_EXTREME_VALUES ) );
    }

    private IndexEntryUpdate<SchemaIndexDescriptor>[] generateAddUpdatesFor( Number[] values )
    {
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<SchemaIndexDescriptor>[] indexEntryUpdates = new IndexEntryUpdate[values.length];
        for ( int i = 0; i < indexEntryUpdates.length; i++ )
        {
            indexEntryUpdates[i] = add( i, Values.of( values[i] ) );
        }
        return indexEntryUpdates;
    }

    private static final Number[] ALL_EXTREME_VALUES = new Number[]
            {
                    Byte.MAX_VALUE,
                    Byte.MIN_VALUE,
                    Short.MAX_VALUE,
                    Short.MIN_VALUE,
                    Integer.MAX_VALUE,
                    Integer.MIN_VALUE,
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    Float.MAX_VALUE,
                    -Float.MAX_VALUE,
                    Double.MAX_VALUE,
                    -Double.MAX_VALUE,
                    Double.POSITIVE_INFINITY,
                    Double.NEGATIVE_INFINITY,
                    0,
                    // These two values below coerce to the same double
                    1234567890123456788L,
                    1234567890123456789L
            };

    protected IndexEntryUpdate<SchemaIndexDescriptor> add( long nodeId, Value value )
    {
        return IndexEntryUpdate.add( nodeId, schemaIndexDescriptor, value );
    }

    static int countUniqueValues( IndexEntryUpdate<SchemaIndexDescriptor>[] updates )
    {
        return Stream.of( updates ).map( update -> update.values()[0] ).collect( Collectors.toSet() ).size();
    }

    static int countUniqueValues( Number[] updates )
    {
        return Stream.of( updates ).collect( Collectors.toSet() ).size();
    }
}
