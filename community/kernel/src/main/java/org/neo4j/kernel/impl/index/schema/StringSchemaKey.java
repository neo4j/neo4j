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

import java.util.Arrays;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.values.storable.UTF8StringValue.codePointByteArrayCompare;

/**
 * Includes value and entity id (to be able to handle non-unique values). A value can be any {@link String},
 * or rather any string that {@link GBPTree} can handle.
 */
class StringSchemaKey extends NativeSchemaKey
{
    static final int ENTITY_ID_SIZE = Long.BYTES;

    private boolean ignoreLength;

    // TODO something better or?
    // TODO this is UTF-8 bytes for now
    byte[] bytes;

    int size()
    {
        return ENTITY_ID_SIZE + bytes.length;
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
    public Value asValue()
    {
        return bytes == null ? Values.NO_VALUE : Values.utf8Value( bytes );
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
        setEntityId( Long.MIN_VALUE );
        setCompareId( DEFAULT_COMPARE_ID );
    }

    void initAsPrefixHigh( String prefix )
    {
        writeString( prefix );
        setEntityId( Long.MAX_VALUE );
        setCompareId( DEFAULT_COMPARE_ID );
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
    int compareValueTo( StringSchemaKey other )
    {
        // TODO cover all cases of bytes == null and special tie breaker and document
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

        try
        {
            // TODO change to not throw
            return codePointByteArrayCompare( bytes, other.bytes, ignoreLength || other.ignoreLength );
        }
        catch ( Exception e )
        {
            // We can not throw here because we will visit this method inside a pageCursor.shouldRetry() block.
            // Just return a comparison that at least will be commutative.
            return byteArrayCompare( bytes, other.bytes );
        }
    }

    private static int byteArrayCompare( byte[] a, byte[] b )
    {
        assert a != null && b != null : "Null arrays not supported.";

        if ( a == b )
        {
            return 0;
        }

        int length = Math.min( a.length, b.length );
        for ( int i = 0; i < length; i++ )
        {
            int compare = Byte.compare( a[i], b[i] );
            if ( compare != 0 )
            {
                return compare;
            }
        }

        return Integer.compare( a.length, b.length );
    }

    @Override
    public String toString()
    {
        return format( "value=%s,entityId=%d,bytes=%s", asValue(), getEntityId(), Arrays.toString( bytes ) );
    }

    @Override
    public void writeString( String value )
    {
        bytes = UTF8.encode( value );
    }

    @Override
    public void writeString( char value )
    {
        writeString( String.valueOf( value ) );
    }
}
