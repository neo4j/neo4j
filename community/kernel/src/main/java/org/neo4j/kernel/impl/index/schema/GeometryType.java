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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.NO_VALUE;

class GeometryType extends Type
{
    // Affected key state:
    // long0 (rawValueBits)
    // long1 (coordinate reference system tableId)
    // long2 (coordinate reference system code)

    // code+table for points (geometry) is 3B in total
    private static final int MASK_CODE = 0x3FFFFF; // 22b
    private static final int SHIFT_TABLE = Integer.bitCount( MASK_CODE );
    private static final int MASK_TABLE_READ = 0xC00000; // 2b
    private static final int MASK_TABLE_PUT = MASK_TABLE_READ >>> SHIFT_TABLE;

    GeometryType( byte typeId )
    {
        super( ValueGroup.GEOMETRY, typeId, PointValue.MIN_VALUE, PointValue.MAX_VALUE );
    }

    @Override
    int valueSize( GenericKeyState state )
    {
        return GenericKeyState.SIZE_GEOMETRY_HEADER + GenericKeyState.SIZE_GEOMETRY;
    }

    @Override
    void copyValue( GenericKeyState to, GenericKeyState from )
    {
        to.long0 = from.long0;
        to.long1 = from.long1;
        to.long2 = from.long2;
        to.spaceFillingCurve = from.spaceFillingCurve;
    }

    @Override
    Value asValue( GenericKeyState state )
    {
        return NO_VALUE;
    }

    @Override
    int compareValue( GenericKeyState left, GenericKeyState right )
    {
        return compare(
                left.long0, left.long1, left.long2,
                right.long0, right.long1, right.long2 );
    }

    @Override
    void putValue( PageCursor cursor, GenericKeyState state )
    {
        putCrs( cursor, state.long1, state.long2 );
        put( cursor, state.long0 );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKeyState into )
    {
        return readCrs( cursor, into ) && read( cursor, into );
    }

    @Override
    String toString( GenericKeyState state )
    {
        return format( "Geometry[tableId:%d, code:%d, rawValue:%d]", state.long1, state.long2, state.long0 );
    }

    /**
     * This method will compare along the curve, which is not a spatial comparison, but is correct
     * for comparison within the space filling index as long as the original spatial range has already
     * been decomposed into a collection of 1D curve ranges before calling down into the GPTree.
     */
    static int compare(
            long this_long0, long this_long1, long this_long2,
            long that_long0, long that_long1, long that_long2 )
    {
        int tableIdComparison = Integer.compare( (int) this_long1, (int) that_long1 );
        if ( tableIdComparison != 0 )
        {
            return tableIdComparison;
        }

        int codeComparison = Integer.compare( (int) this_long2, (int) that_long2 );
        if ( codeComparison != 0 )
        {
            return codeComparison;
        }

        return Long.compare( this_long0, that_long0 );
    }

    static void putCrs( PageCursor cursor, long long1, long long2 )
    {
        if ( (long1 & ~MASK_TABLE_PUT) != 0 )
        {
            throw new IllegalArgumentException( "Table id must be 0 < tableId <= " + MASK_TABLE_PUT + ", but was " + long1 );
        }
        if ( (long2 & ~MASK_CODE) != 0 )
        {
            throw new IllegalArgumentException( "Code must be 0 < code <= " + MASK_CODE + ", but was " + long1 );
        }
        int tableAndCode = (int) ((long1 << SHIFT_TABLE) | long2);
        put3BInt( cursor, tableAndCode );
    }

    static void put( PageCursor cursor, long long0 )
    {
        cursor.putLong( long0 );
    }

    private static void put3BInt( PageCursor cursor, int value )
    {
        cursor.putShort( (short) value );
        cursor.putByte( (byte) (value >>> Short.SIZE) );
    }

    static boolean readCrs( PageCursor cursor, GenericKeyState into )
    {
        int tableAndCode = read3BInt( cursor );
        into.long1 = (tableAndCode & MASK_TABLE_READ) >>> SHIFT_TABLE;
        into.long2 = tableAndCode & MASK_CODE;
        return true;
    }

    static boolean read( PageCursor cursor, GenericKeyState into )
    {
        into.long0 = cursor.getLong();
        return true;
    }

    private static int read3BInt( PageCursor cursor )
    {
        int low = cursor.getShort() & 0xFFFF;
        int high = cursor.getByte() & 0xFF;
        int i = high << Short.SIZE | low;
        return i;
    }

    void write( GenericKeyState state, long derivedSpaceFillingCurveValue )
    {
        state.long0 = derivedSpaceFillingCurveValue;
    }
}
