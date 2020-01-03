/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.values.storable.UTF8StringValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.GenericKey.FALSE;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_STRING_LENGTH;
import static org.neo4j.kernel.impl.index.schema.GenericKey.TRUE;
import static org.neo4j.kernel.impl.index.schema.GenericKey.setCursorException;
import static org.neo4j.kernel.impl.index.schema.GenericKey.toNonNegativeShortExact;
import static org.neo4j.kernel.impl.index.schema.StringIndexKey.lexicographicalUnsignedByteArrayCompare;
import static org.neo4j.values.storable.Values.NO_VALUE;

class TextType extends Type
{
    // in-memory marker in long2 for TEXT value type, i.e. 1:CHAR, 0:STRING
    static final long CHAR_TYPE_STATE_MARKER = 0x2;
    // persistent marker in 2B length, 1:CHAR, 0:STRING
    static final int CHAR_TYPE_LENGTH_MARKER = 0x8000;

    // Affected key state:
    // long0 (length)
    // long1 (bytesDereferenced)
    // long2 (ignoreLength|charValueType)
    // long3 (isHighest)
    // byteArray

    TextType( byte typeId )
    {
        super( ValueGroup.TEXT, typeId,
                Values.of( "" ),
                // max same as min, but this type sets a special flag in initializeAsHighest
                Values.of( "" ) );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return textKeySize( state.long0 );
    }

    @Override
    void copyValue( GenericKey to, GenericKey from )
    {
        to.long0 = from.long0;
        // don't copy long1 since it's instance-local (bytesDereferenced)
        to.long2 = from.long2;
        to.long3 = from.long3;
        setBytesLength( to, (int) from.long0 );
        System.arraycopy( from.byteArray, 0, to.byteArray, 0, (int) from.long0 );
    }

    @Override
    void minimalSplitter( GenericKey left, GenericKey right, GenericKey into )
    {
        int length = 0;
        if ( left.type == Types.TEXT )
        {
            length = StringLayout.minimalLengthFromRightNeededToDifferentiateFromLeft( left.byteArray, (int) left.long0, right.byteArray, (int) right.long0 );
        }
        into.writeUTF8( right.byteArray, 0, length );
    }

    @Override
    Value asValue( GenericKey state )
    {
        // There's a difference between composing a single text value and a array text values
        // and there's therefore no common "raw" variant of it
        if ( state.byteArray == null )
        {
            return NO_VALUE;
        }

        if ( isCharValueType( state.long2 ) )
        {
            // This is a char value.
            return Values.charValue( textAsChar( state.byteArray ) );
        }

        // This is a string value
        state.long1 = TRUE;
        return Values.utf8Value( state.byteArray, 0, (int) state.long0 );
    }

    @Override
    int compareValue( GenericKey left, GenericKey right )
    {
        return compare(
                left.byteArray, left.long0, left.long2, left.long3,
                right.byteArray, right.long0, right.long2, right.long3 );
    }

    @Override
    void putValue( PageCursor cursor, GenericKey state )
    {
        put( cursor, state.byteArray, state.long0, state.long2 );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKey into )
    {
        return read( cursor, size, into );
    }

    static int textKeySize( long long0 )
    {
        return SIZE_STRING_LENGTH + /* short field with bytesLength value */
                (int) long0;        /* bytesLength */
    }

    static int compare(
            byte[] this_byteArray, long this_long0, long this_long2, long this_long3,
            byte[] that_byteArray, long that_long0, long that_long2, long that_long3 )
    {
        if ( this_byteArray != that_byteArray )
        {
            if ( isHighestText( this_long3 ) || isHighestText( that_long3 ) )
            {
                return Boolean.compare( isHighestText( this_long3 ), isHighestText( that_long3 ) );
            }
            if ( this_byteArray == null )
            {
                return -1;
            }
            if ( that_byteArray == null )
            {
                return 1;
            }
        }
        else
        {
            return 0;
        }

        return lexicographicalUnsignedByteArrayCompare( this_byteArray, (int) this_long0, that_byteArray, (int) that_long0,
                booleanOf( this_long2 ) | booleanOf( that_long2 ) );
    }

    static void put( PageCursor cursor, byte[] byteArray, long long0, long long2 )
    {
        // There are two variants of a text value, one is string, the other is char. Both are the same ValueGroup, i.e. TEXT
        // and should be treated the same, it's just that we need to know if it's a char so that we can materialize a CharValue for chars.
        // We put a special marker for char values, knowing that a char is exactly 2 bytes in storage.
        // This can be picked up by reader and set the right flag in state so that a CharValue can be materialized.
        short length = toNonNegativeShortExact( long0 );
        cursor.putShort( isCharValueType( long2 ) ? (short) (length | CHAR_TYPE_LENGTH_MARKER) : length );
        cursor.putBytes( byteArray, 0, length );
    }

    static boolean read( PageCursor cursor, int maxSize, GenericKey into )
    {
        // For performance reasons cannot be redirected to writeString, due to byte[] reuse
        short rawLength = cursor.getShort();
        boolean isCharType = (rawLength & CHAR_TYPE_LENGTH_MARKER) != 0;
        short bytesLength = (short) (rawLength & ~CHAR_TYPE_LENGTH_MARKER);
        if ( bytesLength < 0 || bytesLength > maxSize )
        {
            setCursorException( cursor, "non-valid bytes length for text, " + bytesLength );
            return false;
        }

        // Remember this fact, i.e. set the flag in this state
        setCharType( into, isCharType );
        setBytesLength( into, bytesLength );
        cursor.getBytes( into.byteArray, 0, bytesLength );
        return true;
    }

    static void setCharType( GenericKey into, boolean isCharType )
    {
        if ( isCharType )
        {
            into.long2 |= CHAR_TYPE_STATE_MARKER;
        }
        else
        {
            into.long2 &= ~CHAR_TYPE_STATE_MARKER;
        }
    }

    private static boolean isHighestText( long long3 )
    {
        return long3 == TRUE;
    }

    static boolean isCharValueType( long long2 )
    {
        return booleanOf( long2 >> 1 );
    }

    void write( GenericKey state, byte[] bytes, boolean isCharType )
    {
        state.byteArray = bytes;
        state.long0 = bytes.length;
        setCharType( state, isCharType );
    }

    @Override
    void initializeAsHighest( GenericKey state )
    {
        super.initializeAsHighest( state );
        state.long3 = TRUE;
    }

    static char textAsChar( byte[] byteArray )
    {
        long codePoint = new UTF8StringValue.CodePointCursor( byteArray, 0 ).nextCodePoint();
        if ( (codePoint & ~0xFFFF) != 0 )
        {
            throw new IllegalStateException( "Char value seems to be bigger than what a char can hold " + codePoint );
        }
        return (char) codePoint;
    }

    private static void setBytesLength( GenericKey state, int length )
    {
        if ( booleanOf( state.long1 ) || state.byteArray == null || state.byteArray.length < length )
        {
            state.long1 = FALSE;

            // allocate a bit more than required so that there's a higher chance that this byte[] instance
            // can be used for more keys than just this one
            state.byteArray = new byte[length + length / 2];
        }
        state.long0 = length;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0=" + state.long0 );
        joiner.add( "long1=" + state.long1 );
        joiner.add( "long2=" + state.long2 );
        joiner.add( "long3=" + state.long3 );
        joiner.add( "byteArray=" + Arrays.toString( state.byteArray ) );
    }
}
