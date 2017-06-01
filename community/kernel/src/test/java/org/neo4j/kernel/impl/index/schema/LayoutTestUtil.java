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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.Values;

import static org.neo4j.kernel.impl.index.schema.NumberValue.DOUBLE;
import static org.neo4j.kernel.impl.index.schema.NumberValue.FLOAT;
import static org.neo4j.kernel.impl.index.schema.NumberValue.LONG;

abstract class LayoutTestUtil<KEY extends NumberKey, VALUE extends NumberValue>
{
    private final IndexDescriptor indexDescriptor;

    LayoutTestUtil( IndexDescriptor indexDescriptor )
    {
        this.indexDescriptor = indexDescriptor;
    }

    abstract Layout<KEY,VALUE> createLayout();

    abstract IndexEntryUpdate[] someUpdates();

    protected abstract double fractionDuplicates();

    void copyValue( VALUE value, VALUE intoValue )
    {
        intoValue.type = value.type;
        intoValue.rawValueBits = value.rawValueBits;
    }

    int compareValue( VALUE value1, VALUE value2 )
    {
        return compareIndexedPropertyValue( value1, value2 );
    }

    int compareIndexedPropertyValue( NumberValue value1, NumberValue value2 )
    {
        int typeCompare = Byte.compare( value1.type(), value2.type() );
        if ( typeCompare == 0 )
        {
            switch ( value1.type() )
            {
            case LONG:
                return Long.compare( value1.rawValueBits(), value2.rawValueBits() );
            case FLOAT:
                return Float.compare(
                        Float.intBitsToFloat( (int) value1.rawValueBits() ),
                        Float.intBitsToFloat( (int) value2.rawValueBits() ) );
            case DOUBLE:
                return Double.compare(
                        Double.longBitsToDouble( value1.rawValueBits() ),
                        Double.longBitsToDouble( value2.rawValueBits() ) );
            default:
                throw new IllegalArgumentException(
                        "Expected type to be LONG, FLOAT or DOUBLE (" + LONG + "," + FLOAT + "," + DOUBLE +
                                "). But was " + value1.type() );
            }
        }
        return typeCompare;
    }

    Iterator<IndexEntryUpdate<IndexDescriptor>> randomUniqueUpdateGenerator( RandomRule random )
    {
        double fractionDuplicates = fractionDuplicates();
        return new PrefetchingIterator<IndexEntryUpdate<IndexDescriptor>>()
        {
            private final Set<Double> uniqueCompareValues = new HashSet<>();
            private final List<Number> uniqueValues = new ArrayList<>();
            private long currentEntityId;

            @Override
            protected IndexEntryUpdate<IndexDescriptor> fetchNextOrNull()
            {
                Number value;
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

            private Number newUniqueValue( RandomRule randomRule )
            {
                Number value;
                Double compareValue;
                do
                {
                    value = randomRule.numberPropertyValue();
                    compareValue = value.doubleValue();
                }
                while ( !uniqueCompareValues.add( compareValue ) );
                uniqueValues.add( value );
                return value;
            }

            private Number existingNonUniqueValue( RandomRule randomRule )
            {
                return uniqueValues.get( randomRule.nextInt( uniqueValues.size() ) );
            }
        };
    }

    @SuppressWarnings( "rawtypes" )
    IndexEntryUpdate[] someUpdatesNoDuplicateValues()
    {
        return generateAddUpdatesFor( ALL_EXTREME_VALUES );
    }

    @SuppressWarnings( "rawtypes" )
    IndexEntryUpdate[] someUpdatesWithDuplicateValues()
    {
        return generateAddUpdatesFor( ArrayUtils.addAll( ALL_EXTREME_VALUES, ALL_EXTREME_VALUES ) );
    }

    private IndexEntryUpdate[] generateAddUpdatesFor( Number[] values )
    {
        IndexEntryUpdate[] indexEntryUpdates = new IndexEntryUpdate[values.length];
        for ( int i = 0; i < indexEntryUpdates.length; i++ )
        {
            indexEntryUpdates[i] = add( i, values[i] );
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
                    0
            };

    protected IndexEntryUpdate<IndexDescriptor> add( long nodeId, Object value )
    {
        return IndexEntryUpdate.add( nodeId, indexDescriptor, Values.of( value ) );
    }
}
