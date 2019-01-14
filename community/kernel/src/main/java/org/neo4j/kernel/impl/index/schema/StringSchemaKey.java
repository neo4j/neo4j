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

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values). A value can be any {@link String},
 * or rather any string that {@link GBPTree} can handle.
 */
class StringSchemaKey extends NativeSchemaKey<StringSchemaKey>
{
    static final int ENTITY_ID_SIZE = Long.BYTES;

    private boolean ignoreLength;

    // UTF-8 bytes, grows on demand. Actual length is dictated by bytesLength field.
    byte[] bytes;
    int bytesLength;
    // Set to true when the internal byte[] have been handed out to an UTF8Value, so that the next call to setBytesLength
    // will be forced to allocate a new array. The byte[] isn't cleared with null since this key still logically contains those bytes.
    private boolean bytesDereferenced;

    int size()
    {
        return ENTITY_ID_SIZE + bytesLength;
    }

    @Override
    protected Value assertCorrectType( Value value )
    {
        if ( !Values.isTextValue( value ) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support strings, tried to create key from " + value );
        }
        return value;
    }

    @Override
    void initialize( long entityId )
    {
        super.initialize( entityId );
        ignoreLength = false;
    }

    @Override
    public Value asValue()
    {
        if ( bytes == null )
        {
            return Values.NO_VALUE;
        }

        // Dereference our bytes so that we won't overwrite it on next read
        bytesDereferenced = true;
        return Values.utf8Value( bytes, 0, bytesLength );
    }

    @Override
    void initValueAsLowest()
    {
        bytes = null;
    }

    @Override
    void initValueAsHighest()
    {
        bytes = null;
    }

    void initAsPrefixLow( String prefix )
    {
        writeString( prefix );
        initialize( Long.MIN_VALUE );
        // Don't set ignoreLength = true here since the "low" a.k.a. left side of the range should care about length.
        // This will make the prefix lower than those that matches the prefix (their length is >= that of the prefix)
    }

    void initAsPrefixHigh( String prefix )
    {
        writeString( prefix );
        initialize( Long.MAX_VALUE );
        ignoreLength = true;
    }

    private boolean isHighest()
    {
        return getCompareId() && getEntityId() == Long.MAX_VALUE && bytes == null;
    }

    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the {@link StringSchemaKey} to compare to.
     * @return comparison against the {@code other} {@link StringSchemaKey}.
     */
    @Override
    int compareValueTo( StringSchemaKey other )
    {
        if ( bytes != other.bytes )
        {
            if ( bytes == null )
            {
                return isHighest() ? 1 : -1;
            }
            if ( other.bytes == null )
            {
                return other.isHighest() ? -1 : 1;
            }
        }
        else
        {
            return 0;
        }

        return unsignedByteArrayCompare( bytes, bytesLength, other.bytes, other.bytesLength, ignoreLength | other.ignoreLength );
    }

    private static int unsignedByteArrayCompare( byte[] a, int aLength, byte[] b, int bLength, boolean ignoreLength )
    {
        assert a != null && b != null : "Null arrays not supported.";

        if ( a == b && aLength == bLength )
        {
            return 0;
        }

        int length = Math.min( aLength, bLength );
        for ( int i = 0; i < length; i++ )
        {
            int compare = Short.compare( (short) (a[i] & 0xFF), (short) (b[i] & 0xFF) );
            if ( compare != 0 )
            {
                return compare;
            }
        }

        return ignoreLength ? 0 : Integer.compare( aLength, bLength );
    }

    @Override
    public String toString()
    {
        return format( "value=%s,entityId=%d,bytes=%s",
                asValue(),
                getEntityId(),
                bytes == null ? "null" : Arrays.toString( Arrays.copyOf( bytes, bytesLength ) ) );
    }

    @Override
    public void writeString( String value )
    {
        bytes = UTF8.encode( value );
        bytesLength = bytes.length;
        bytesDereferenced = false;
    }

    @Override
    public void writeString( char value )
    {
        writeString( String.valueOf( value ) );
    }

    void copyFrom( StringSchemaKey key )
    {
        setBytesLength( key.bytesLength );
        System.arraycopy( key.bytes, 0, bytes, 0, key.bytesLength );
        setEntityId( key.getEntityId() );
        setCompareId( key.getCompareId() );
    }

    /**
     * Ensures that the internal byte[] is long enough, or longer than the given {@code length}.
     * Also sets the internal {@code bytesLength} field to the given {@code length} so that interactions with the byte[]
     * from this point on will use that for length, instead of the length of the byte[].
     *
     * @param length minimum length that the internal byte[] needs to be.
     */
    void setBytesLength( int length )
    {
        if ( bytesDereferenced || bytes == null || bytes.length < length )
        {
            bytesDereferenced = false;

            // allocate a bit more than required so that there's a higher chance that this byte[] instance
            // can be used for more keys than just this one
            bytes = new byte[length + length / 2];
        }
        bytesLength = length;
    }
}
