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
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class LayoutTestUtil<KEY extends NativeSchemaKey, VALUE extends NativeSchemaValue>
{
    final IndexDescriptor indexDescriptor;

    LayoutTestUtil( IndexDescriptor indexDescriptor )
    {
        this.indexDescriptor = indexDescriptor;
    }

    abstract Layout<KEY,VALUE> createLayout();

    abstract IndexEntryUpdate<IndexDescriptor>[] someUpdates();

    protected abstract double fractionDuplicates();

    abstract IndexQuery rangeQuery( Number from, boolean fromInclusive, Number to, boolean toInclusive );

    abstract Value asValue( Number value );

    abstract int compareIndexedPropertyValue( KEY key1, KEY key2 );

    IndexDescriptor indexDescriptor()
    {
        return indexDescriptor;
    }

    void copyValue( VALUE value, VALUE intoValue )
    {
    }

    abstract Iterator<IndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator( RandomRule random );

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

    IndexEntryUpdate<IndexDescriptor>[] someUpdatesNoDuplicateValues()
    {
        return generateAddUpdatesFor( ALL_EXTREME_VALUES );
    }

    IndexEntryUpdate<IndexDescriptor>[] someUpdatesWithDuplicateValues()
    {
        return generateAddUpdatesFor( ArrayUtils.addAll( ALL_EXTREME_VALUES, ALL_EXTREME_VALUES ) );
    }

    IndexEntryUpdate<IndexDescriptor>[] someSpatialUpdatesWithDuplicateValues()
    {
        return generateAddUpdatesFor( ArrayUtils.addAll( SOME_POINTS, SOME_POINTS ) );
    }

    private IndexEntryUpdate<IndexDescriptor>[] generateAddUpdatesFor( Object[] values )
    {
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] indexEntryUpdates = new IndexEntryUpdate[values.length];
        for ( int i = 0; i < indexEntryUpdates.length; i++ )
        {
            indexEntryUpdates[i] = add( i, Values.of( values[i] ) );
        }
        return indexEntryUpdates;
    }

    private static final PointValue[] SOME_POINTS = new PointValue[]
            {
                    Values.pointValue( CoordinateReferenceSystem.WGS84, 12.5, 56.8 ),
                    Values.pointValue( CoordinateReferenceSystem.WGS84, -38.5, 36.8 ),
                    Values.pointValue( CoordinateReferenceSystem.WGS84, 30.0, -40.0 ),
                    Values.pointValue( CoordinateReferenceSystem.WGS84, -50, -25 )
            };

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

    protected IndexEntryUpdate<IndexDescriptor> add( long nodeId, Value value )
    {
        return IndexEntryUpdate.add( nodeId, indexDescriptor, value );
    }

    static int countUniqueValues( IndexEntryUpdate<IndexDescriptor>[] updates )
    {
        return Stream.of( updates ).map( update -> update.values()[0] ).collect( Collectors.toSet() ).size();
    }

    static int countUniqueValues( Number[] updates )
    {
        return Stream.of( updates ).collect( Collectors.toSet() ).size();
    }
}
