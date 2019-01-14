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
package org.neo4j.kernel.impl.index.schema;

import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.impl.index.schema.GeometryType.assertHasCoordinates;
import static org.neo4j.kernel.impl.index.schema.GeometryType.dimensions;
import static org.neo4j.kernel.impl.index.schema.GeometryType.hasCoordinates;
import static org.neo4j.kernel.impl.index.schema.GeometryType.putCrs;
import static org.neo4j.kernel.impl.index.schema.GeometryType.putPoint;
import static org.neo4j.kernel.impl.index.schema.GeometryType.readCrs;

/**
 * Handles {@link PointValue[]}.
 *
 * Note about lazy initialization of {@link GenericKey} data structures: a point type is special in that it contains a {@link CoordinateReferenceSystem},
 * which dictates how much space it will occupy. When serializing a {@link PointArray} into {@link GenericKey} (via the logic in this class)
 * the {@link CoordinateReferenceSystem} isn't known at initialization, where only the type and array length is known.
 * This is why some state is initialize lazily when observing the first point in the array.
 */
class GeometryArrayType extends AbstractArrayType<PointValue>
{
    // Affected key state:
    // long0Array (rawValueBits)
    // long1Array (coordinates)
    // long1 (coordinate reference system tableId)
    // long2 (coordinate reference system code)
    // long3 (dimensions)

    GeometryArrayType( byte typeId )
    {
        super( ValueGroup.GEOMETRY_ARRAY, typeId, ( o1, o2, i ) -> GeometryType.compare(
                        // intentional long1 and long2 - not the array versions
                o1.long0Array[i], o1.long1, o1.long2, o1.long3, o1.long1Array, (int) o1.long3 * i,
                o2.long0Array[i], o2.long1, o2.long2, o2.long3, o2.long1Array, (int) o2.long3 * i ),
                null, null, null, null, null );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return GenericKey.SIZE_GEOMETRY_HEADER +
                arrayKeySize( state, GenericKey.SIZE_GEOMETRY + dimensions( state ) * GenericKey.SIZE_GEOMETRY_COORDINATE );
    }

    @Override
    void copyValue( GenericKey to, GenericKey from, int length )
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
    void initializeArray( GenericKey key, int length, ValueWriter.ArrayType arrayType )
    {
        key.long0Array = ensureBigEnough( key.long0Array, length );

        // Since this method is called when serializing a PointValue into the key state, the CRS and number of dimensions
        // are unknown at this point. Read more about why lazy initialization is required in the class-level javadoc.
        if ( length == 0 && key.long1Array == null )
        {
            // There's this special case where we're initializing an empty geometry array and so the long1Array
            // won't be initialized at all. Therefore we're preemptively making sure it's at least not null.
            key.long1Array = EMPTY_LONG_ARRAY;
        }
    }

    @Override
    Value asValue( GenericKey state )
    {
        Point[] points = new Point[state.arrayLength];
        if ( points.length > 0 )
        {
            assertHasCoordinates( state );
            CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( (int) state.long1, (int) state.long2 );
            int dimensions = dimensions( state );
            for ( int i = 0; i < points.length; i++ )
            {
                points[i] = GeometryType.asValue( state, crs, dimensions * i );
            }
        }
        return Values.pointArray( points );
    }

    @Override
    void putValue( PageCursor cursor, GenericKey state )
    {
        putCrs( cursor, state.long1, state.long2, state.long3 );
        int dimensions = dimensions( state );
        putArray( cursor, state, ( c, k, i ) -> putPoint( c, k.long0Array[i], k.long3, k.long1Array, i * dimensions ) );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKey into )
    {
        readCrs( cursor, into );
        return readArray( cursor, ValueWriter.ArrayType.POINT, GeometryArrayType::readGeometryArrayItem, into );
    }

    @Override
    String toString( GenericKey state )
    {
        String asValueString = hasCoordinates( state ) ? asValue( state ).toString() : "NO_COORDINATES";
        return format( "GeometryArray[tableId:%d, code:%d, rawValues:%s, value:%s]",
                state.long1, state.long2, Arrays.toString( Arrays.copyOf( state.long0Array, state.arrayLength ) ), asValueString );
    }

    private static boolean readGeometryArrayItem( PageCursor cursor, GenericKey into )
    {
        into.long0Array[into.currentArrayOffset] = cursor.getLong();
        int dimensions = dimensions( into );
        if ( into.currentArrayOffset == 0 )
        {
            // Read more about why lazy initialization is required in the class-level javadoc.
            into.long1Array = ensureBigEnough( into.long1Array, dimensions * into.arrayLength );
        }
        for ( int i = 0, offset = into.currentArrayOffset * dimensions; i < dimensions; i++ )
        {
            into.long1Array[offset + i] = cursor.getLong();
        }
        into.currentArrayOffset++;
        return true;
    }

    void write( GenericKey state, int offset, long derivedSpaceFillingCurveValue, double[] coordinates )
    {
        state.long0Array[offset] = derivedSpaceFillingCurveValue;
        if ( offset == 0 )
        {
            // Read more about why lazy initialization is required in the class-level javadoc.
            int dimensions = coordinates.length;
            state.long1Array = ensureBigEnough( state.long1Array, dimensions * state.arrayLength );
            state.long3 = dimensions;
        }
        for ( int i = 0, base = dimensions( state ) * offset; i < coordinates.length; i++ )
        {
            state.long1Array[base + i] = Double.doubleToLongBits( coordinates[i] );
        }
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long1=" + state.long1 );
        joiner.add( "long2=" + state.long2 );
        joiner.add( "long3=" + state.long3 );
        joiner.add( "long0Array=" + Arrays.toString( state.long0Array ) );
        joiner.add( "long1Array=" + Arrays.toString( state.long1Array ) );
        super.addTypeSpecificDetails( joiner, state );
    }
}
