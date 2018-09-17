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

import java.util.Arrays;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.impl.index.schema.GeometryType.assertHasCoordinates;
import static org.neo4j.kernel.impl.index.schema.GeometryType.dimensions;
import static org.neo4j.kernel.impl.index.schema.GeometryType.put;
import static org.neo4j.kernel.impl.index.schema.GeometryType.putCrs;
import static org.neo4j.kernel.impl.index.schema.GeometryType.readCrs;

class GeometryArrayType extends AbstractArrayType<PointValue>
{
    // Affected key state:
    // long0Array (rawValueBits)
    // long1 (coordinate reference system tableId)
    // long2 (coordinate reference system code)

    GeometryArrayType( byte typeId )
    {
        super( ValueGroup.GEOMETRY_ARRAY, typeId, ( o1, o2, i ) -> GeometryType.compare(
                        // intentional long1 and long2 - not the array versions
                        o1.long0Array[i], o1.long1, o1.long2,
                        o2.long0Array[i], o2.long1, o2.long2 ),
                null, null, null, null, null );
    }

    @Override
    int valueSize( GenericKeyState state )
    {
        return GenericKeyState.SIZE_GEOMETRY_HEADER +
                arrayKeySize( state, GenericKeyState.SIZE_GEOMETRY + dimensions( state ) * GenericKeyState.SIZE_GEOMETRY_COORDINATE );
    }

    @Override
    void copyValue( GenericKeyState to, GenericKeyState from, int length )
    {
        initializeArray( to, length, null );
        System.arraycopy( from.long0Array, 0, to.long0Array, 0, length );
        to.long1 = from.long1;
        to.long2 = from.long2;
        to.long3 = from.long3;
        int dimensions = dimensions( from );
        to.long1Array = ensureBigEnough( to.long1Array, dimensions * length );
        System.arraycopy( from.long1Array, 0, to.long1Array, 0, dimensions * length );
        to.spaceFillingCurve = from.spaceFillingCurve;
    }

    @Override
    void initializeArray( GenericKeyState key, int length, ValueWriter.ArrayType arrayType )
    {
        key.long0Array = ensureBigEnough( key.long0Array, length );

        // Since this method is called when serializing a PointValue into the key state, the CRS and number of dimensions
        // are unknown at this point. Instead key.long1Array will be initialized lazily upon observing the first array item,
        // because that's when we first will know that information.
        if ( length == 0 && key.long1Array == null )
        {
            // There's this special case where we're initializing an empty geometry array and so the long1Array
            // won't be initialized at all. Therefore we're preemptively making sure it's at least not null.
            key.long1Array = EMPTY_LONG_ARRAY;
        }
    }

    @Override
    Value asValue( GenericKeyState state )
    {
        assertHasCoordinates( state.long3, state.long1Array );
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( (int) state.long1, (int) state.long2 );
        Point[] points = new Point[state.arrayLength];
        int dimensions = dimensions( state );
        for ( int i = 0; i < points.length; i++ )
        {
            points[i] = GeometryType.asValue( state, crs, dimensions * i );
        }
        return Values.pointArray( points );
    }

    @Override
    void putValue( PageCursor cursor, GenericKeyState state )
    {
        putCrs( cursor, state.long1, state.long2, state.long3 );
        int dimensions = dimensions( state );
        putArray( cursor, state, ( c, k, i ) -> put( c, state.long0Array[i], state.long3, state.long1Array, i * dimensions ) );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKeyState into )
    {
        readCrs( cursor, into );
        return readArray( cursor, ValueWriter.ArrayType.POINT, GeometryArrayType::readGeometryArrayItem, into );
    }

    @Override
    String toString( GenericKeyState state )
    {
        return format( "GeometryArray[tableId:%d, code:%d, rawValues:%s]",
                state.long1, state.long2, Arrays.toString( Arrays.copyOf( state.long0Array, state.arrayLength ) ) );
    }

    private static boolean readGeometryArrayItem( PageCursor cursor, GenericKeyState into )
    {
        into.long0Array[into.currentArrayOffset] = cursor.getLong();
        int dimensions = dimensions( into );
        if ( into.currentArrayOffset == 0 )
        {
            // Initialize the coordinates array lazily because we don't know the dimension count
            // when initializeArray is called, only afterwards when the header have been read.
            into.long1Array = ensureBigEnough( into.long1Array, dimensions * into.arrayLength );
        }
        for ( int i = 0, offset = into.currentArrayOffset * dimensions; i < dimensions; i++ )
        {
            into.long1Array[offset + i] = cursor.getLong();
        }
        into.currentArrayOffset++;
        return true;
    }

    void write( GenericKeyState state, int offset, long derivedSpaceFillingCurveValue, double[] coordinates )
    {
        state.long0Array[offset] = derivedSpaceFillingCurveValue;
        if ( offset == 0 )
        {
            int dimensions = coordinates.length;
            state.long1Array = ensureBigEnough( state.long1Array, dimensions * state.arrayLength );
            state.long3 = dimensions;
        }
        for ( int i = 0, base = dimensions( state ) * offset; i < coordinates.length; i++ )
        {
            state.long1Array[base + i] = Double.doubleToLongBits( coordinates[i] );
        }
    }
}
