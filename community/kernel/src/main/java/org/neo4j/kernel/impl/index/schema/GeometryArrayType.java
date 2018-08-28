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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.toNonNegativeShortExact;
import static org.neo4j.kernel.impl.index.schema.GeometryType.put;
import static org.neo4j.kernel.impl.index.schema.GeometryType.putCrs;
import static org.neo4j.kernel.impl.index.schema.GeometryType.readCrs;
import static org.neo4j.values.storable.Values.NO_VALUE;

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
        return GenericKeyState.SIZE_GEOMETRY_HEADER + arrayKeySize( state, GenericKeyState.SIZE_GEOMETRY );
    }

    @Override
    void copyValue( GenericKeyState to, GenericKeyState from, int length )
    {
        initializeArray( to, length, null );
        System.arraycopy( from.long0Array, 0, to.long0Array, 0, length );
        to.long1 = from.long1;
        to.long2 = from.long2;
    }

    @Override
    void initializeArray( GenericKeyState key, int length, ValueWriter.ArrayType arrayType )
    {
        key.long0Array = ensureBigEnough( key.long0Array, length );
        // plain long1 for tableId
        // plain long2 for code
    }

    @Override
    Value asValue( GenericKeyState state )
    {
        return NO_VALUE;
    }

    @Override
    void putValue( PageCursor cursor, GenericKeyState state )
    {
        putCrs( cursor, state.long1, state.long2 );
        putArray( cursor, state, ( c, k, i ) -> put( c, state.long0Array[i] ) );
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
        return format( "Geometry[tableId:%d, code:%d, rawValues:%s]",
                state.long1, state.long2, Arrays.toString( Arrays.copyOf( state.long0Array, state.arrayLength ) ) );
    }

    private static boolean readGeometryArrayItem( PageCursor cursor, GenericKeyState into )
    {
        into.long0Array[into.currentArrayOffset++] = cursor.getLong();
        return true;
    }

    void write( GenericKeyState state, int offset, long derivedSpaceFillingCurveValue )
    {
        state.long0Array[offset] = derivedSpaceFillingCurveValue;
    }
}
