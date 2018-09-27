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
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.GenericKeyState.FALSE;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_ARRAY_LENGTH;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.setCursorException;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.toNonNegativeShortExact;
import static org.neo4j.kernel.impl.index.schema.TextType.CHAR_TYPE_LENGTH_MARKER;
import static org.neo4j.kernel.impl.index.schema.TextType.isCharValueType;
import static org.neo4j.kernel.impl.index.schema.TextType.setCharType;
import static org.neo4j.kernel.impl.index.schema.TextType.textAsChar;

class TextArrayType extends AbstractArrayType<String>
{
    // Affected key state:
    // long0 (length)
    // long1 (bytesDereferenced)
    // long2 (ignoreLength|charValueType)
    // long3 (isHighest)
    // byteArrayArray

    TextArrayType( byte typeId )
    {
        super( ValueGroup.TEXT_ARRAY, typeId, ( o1, o2, i ) -> TextType.compare(
                        o1.byteArrayArray[i], o1.long0Array[i], o1.long2, o1.long3,
                        o2.byteArrayArray[i], o2.long0Array[i], o2.long2, o2.long3 ),
                ( k, i ) -> asValueRaw( k.byteArrayArray[i], k.long0Array[i] ),
                ( c, k, i ) -> TextType.put( c, k.byteArrayArray[i], k.long0Array[i], 0 ),
                null, String[]::new, ValueWriter.ArrayType.STRING );
    }

    @Override
    int valueSize( GenericKeyState state )
    {
        int stringArraySize = 0;
        for ( int i = 0; i < state.arrayLength; i++ )
        {
            stringArraySize += TextType.textKeySize( state.long0Array[i] );
        }
        return SIZE_ARRAY_LENGTH + stringArraySize;
    }

    @Override
    void copyValue( GenericKeyState to, GenericKeyState from, int length )
    {
        to.long1 = FALSE;
        to.long2 = from.long2;
        to.long3 = from.long3;
        initializeArray( to, length, null );
        System.arraycopy( from.long0Array, 0, to.long0Array, 0, length );
        for ( int i = 0; i < length; i++ )
        {
            short targetLength = (short) from.long0Array[i];
            to.byteArrayArray[i] = ensureBigEnough( to.byteArrayArray[i], targetLength );
            System.arraycopy( from.byteArrayArray[i], 0, to.byteArrayArray[i], 0, targetLength );
        }
    }

    @Override
    void initializeArray( GenericKeyState key, int length, ValueWriter.ArrayType arrayType )
    {
        key.long0Array = ensureBigEnough( key.long0Array, length );
        key.byteArrayArray = ensureBigEnough( key.byteArrayArray, length );
        // long1 (bytesDereferenced) - Not needed because we never leak bytes from string array
        // long2 (ignoreLength) - Not needed because kept on 'global' level for full array
        // long3 (isHighest) - Not needed because kept on 'global' level for full array
        setCharType( key, arrayType == ValueWriter.ArrayType.CHAR );
    }

    @Override
    void putValue( PageCursor cursor, GenericKeyState state )
    {
        short typeMarker = (short) (isCharValueType( state.long2 ) ? CHAR_TYPE_LENGTH_MARKER : 0);
        putArrayHeader( cursor, (short) (toNonNegativeShortExact( state.arrayLength ) | typeMarker) );
        putArrayItems( cursor, state, arrayElementWriter );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKeyState into )
    {
        short rawLength = cursor.getShort();
        boolean isCharType = (rawLength & CHAR_TYPE_LENGTH_MARKER) != 0;
        short length = (short) (rawLength & ~CHAR_TYPE_LENGTH_MARKER);
        if ( !setArrayLengthWhenReading( into, cursor, length ) )
        {
            return false;
        }
        into.beginArray( into.arrayLength, isCharType ? ValueWriter.ArrayType.CHAR : ValueWriter.ArrayType.STRING );
        for ( int i = 0; i < into.arrayLength; i++ )
        {
            short bytesLength = cursor.getShort();
            if ( bytesLength < 0 || bytesLength > size )
            {
                setCursorException( cursor, "non-valid bytes length, " + bytesLength );
                return false;
            }

            into.byteArrayArray[i] = ensureBigEnough( into.byteArrayArray[i], bytesLength );
            into.long0Array[i] = bytesLength;
            cursor.getBytes( into.byteArrayArray[i], 0, bytesLength );
        }
        into.endArray();
        return true;
    }

    @Override
    Value asValue( GenericKeyState state )
    {
        // no need to set bytes dereferenced because byte[][] owned by this class will be deserialized into String objects.
        if ( isCharValueType( state.long2 ) )
        {
            // this is a char[]
            return charArrayAsValue( state );
        }
        // this is a String[]
        return super.asValue( state );
    }

    private Value charArrayAsValue( GenericKeyState state )
    {
        char[] chars = new char[state.arrayLength];
        for ( int i = 0; i < state.arrayLength; i++ )
        {
            chars[i] = textAsChar( state.byteArrayArray[i] );
        }
        return Values.charArray( chars );
    }

    static String asValueRaw( byte[] byteArray, long long0 )
    {
        return byteArray == null ? null : UTF8.decode( byteArray, 0, (int) long0 );
    }

    void write( GenericKeyState state, int offset, byte[] bytes )
    {
        state.byteArrayArray[offset] = bytes;
        state.long0Array[offset] = bytes.length;
    }
}
