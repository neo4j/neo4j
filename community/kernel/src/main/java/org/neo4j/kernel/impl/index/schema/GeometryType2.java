/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;

class GeometryType2 extends Type
{
    // Affected key state:
    // long0 (coordinate reference system tableId)
    // long1 (coordinate reference system code)
    // long2 (dimensions)
    // long0Array (coordinates)

    // code+table for points (geometry) is 3B in total
    private static final int MASK_CODE =            0b00000011_11111111_11111111;
    private static final int MASK_DIMENSIONS_READ = 0b00011100_00000000_00000000;
    //                                                  ^ this bit is reserved for future expansion of number of dimensions
    private static final int MASK_TABLE_READ =      0b11000000_00000000_00000000;
    private static final int SHIFT_DIMENSIONS = Integer.bitCount( MASK_CODE );
    private static final int SHIFT_TABLE = SHIFT_DIMENSIONS + 1/*the reserved dimension bit*/ + Integer.bitCount( MASK_DIMENSIONS_READ );
    private static final int MASK_TABLE_PUT = MASK_TABLE_READ >>> SHIFT_TABLE;
    private static final int MASK_DIMENSIONS_PUT = MASK_DIMENSIONS_READ >>> SHIFT_DIMENSIONS;

    GeometryType2( byte typeId )
    {
        super( ValueGroup.GEOMETRY, typeId, PointValue.MIN_VALUE, PointValue.MAX_VALUE );
    }

    @Override
    int valueSize( GenericKey<?> state )
    {
        int coordinatesSize = dimensions( state ) * Types.SIZE_GEOMETRY_COORDINATE;
        return Types.SIZE_GEOMETRY_HEADER + coordinatesSize;
    }

    static int dimensions( GenericKey<?> state )
    {
        return toIntExact( state.long2 );
    }

    @Override
    void copyValue( GenericKey<?> to, GenericKey<?> from )
    {
        to.long0 = from.long0;
        to.long1 = from.long1;
        to.long2 = from.long2;
        int dimensions = dimensions( from );
        to.long0Array = ensureBigEnough( to.long0Array, dimensions );
        System.arraycopy( from.long0Array, 0, to.long0Array, 0, dimensions );
    }

    @Override
    Value asValue( GenericKey<?> state )
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( (int) state.long0, (int) state.long1 );
        return asValue( state, crs, 0 );
    }

    static PointValue asValue( GenericKey<?> state, CoordinateReferenceSystem crs, int offset )
    {
        double[] coordinates = new double[dimensions( state )];
        for ( int i = 0; i < coordinates.length; i++ )
        {
            coordinates[i] = Double.longBitsToDouble( state.long0Array[offset + i] );
        }
        return Values.pointValue( crs, coordinates );
    }

    @Override
    int compareValue( GenericKey<?> left, GenericKey<?> right )
    {
        return compare(
                left.long0, left.long1, left.long2, left.long0Array, 0,
                right.long0, right.long1, right.long2, right.long0Array, 0 );
    }

    @Override
    void putValue( PageCursor cursor, GenericKey<?> state )
    {
        putCrs( cursor, state.long0, state.long1, state.long2 );
        putPoint( cursor, state.long2, state.long0Array, 0 );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKey<?> into )
    {
        return readCrs( cursor, into ) && readPoint( cursor, into );
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey<?> state )
    {
        joiner.add( "long0=" + state.long0 );
        joiner.add( "long1=" + state.long1 );
        joiner.add( "long2=" + state.long2 );
        joiner.add( "long0Array=" + Arrays.toString( state.long0Array ) );
    }

    static int compare(
            long this_long0, long this_long1, long this_long2, long[] this_long0Array, int this_coordinates_offset,
            long that_long0, long that_long1, long that_long2, long[] that_long0Array, int that_coordinates_offset )
    {
        int tableIdComparison = Integer.compare( (int) this_long0, (int) that_long0 );
        if ( tableIdComparison != 0 )
        {
            return tableIdComparison;
        }

        int codeComparison = Integer.compare( (int) this_long1, (int) that_long1 );
        if ( codeComparison != 0 )
        {
            return codeComparison;
        }

        // Points with the same coordinate system will have the same dimensions (enforced in Values.pointValue) - using this.
        for ( int i = 0; i < (int) this_long2; i++ )
        {
            final double thisCoord = Double.longBitsToDouble( this_long0Array[this_coordinates_offset + i] );
            final double thatCoord = Double.longBitsToDouble( that_long0Array[that_coordinates_offset + i] );
            int coordinateComparison = Double.compare( thisCoord, thatCoord );
            if ( coordinateComparison != 0 )
            {
                return coordinateComparison;
            }
        }
        return 0;
    }

    static boolean readCrs( PageCursor cursor, GenericKey<?> into )
    {
        int header = read3BInt( cursor );
        into.long0 = (header & MASK_TABLE_READ) >>> SHIFT_TABLE;
        into.long1 = header & MASK_CODE;
        into.long2 = (header & MASK_DIMENSIONS_READ) >>> SHIFT_DIMENSIONS;
        return true;
    }

    private static boolean readPoint( PageCursor cursor, GenericKey<?> into )
    {
        // into.long2 have just been read by readCrs, before this method is called
        int dimensions = dimensions( into );
        into.long0Array = ensureBigEnough( into.long0Array, dimensions );
        for ( int i = 0; i < dimensions; i++ )
        {
            into.long0Array[i] = cursor.getLong();
        }
        return true;
    }

    private static int read3BInt( PageCursor cursor )
    {
        int low = cursor.getShort() & 0xFFFF;
        int high = cursor.getByte() & 0xFF;
        return high << Short.SIZE | low;
    }

    static void putCrs( PageCursor cursor, long tableId, long code, long dimensions )
    {
        assertValueWithin( tableId, MASK_TABLE_PUT, "tableId" );
        assertValueWithin( code, MASK_CODE, "code" );
        assertValueWithin( dimensions, MASK_DIMENSIONS_PUT, "dimensions" );
        int header = (int) ((tableId << SHIFT_TABLE) | (dimensions << SHIFT_DIMENSIONS) | code);
        put3BInt( cursor, header );
    }

    private static void assertValueWithin( long value, int maskAllowed, String name )
    {
        if ( (value & ~maskAllowed) != 0 )
        {
            throw new IllegalArgumentException( "Expected 0 < " + name + " <= " + maskAllowed + ", but was " + value );
        }
    }

    static void putPoint( PageCursor cursor, long dimensions, long[] coordinate, int long0ArrayOffset )
    {
        for ( int i = 0; i < dimensions; i++ )
        {
            cursor.putLong( coordinate[long0ArrayOffset + i] );
        }
    }

    static void write( RangeKey state, int tableId, int code, double[] coordinate )
    {
        state.long0 = tableId;
        state.long1 = code;
        state.long0Array = ensureBigEnough( state.long0Array, coordinate.length );
        for ( int i = 0; i < coordinate.length; i++ )
        {
            state.long0Array[i] = Double.doubleToLongBits( coordinate[i] );
        }
        state.long2 = coordinate.length;
    }

    private static void put3BInt( PageCursor cursor, int value )
    {
        cursor.putShort( (short) value );
        cursor.putByte( (byte) (value >>> Short.SIZE) );
    }
}
