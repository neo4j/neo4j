/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.store.prototype.neole;

import java.nio.BufferUnderflowException;
import java.nio.charset.Charset;

import org.neo4j.internal.store.cursors.ReadCursor;

import static java.lang.Math.min;
import static org.neo4j.internal.store.prototype.neole.ReadStore.combineReference;
import static org.neo4j.internal.store.prototype.neole.ReadStore.nextPowerOfTwo;

/**
 * The ByteBlockCursor would be used to read data from dynamic stores. This will help if we decide to implement
 * loading of heavy properties in NeoLE.
 */
class ByteBlockCursor extends ReadCursor
{
    /**
     * <pre>
     *  0: in use     (1 byte)
     *  1: byte count (3 bytes)
     *  4: next       (4 bytes)
     * </pre>
     * <h2>in use</h2>
     * <pre>
     * [   x,    ] in use
     * [0   ,    ] first record in chain
     * [1   ,    ] continuation record in chain
     * [    ,xxxx] high(next)
     * </pre>
     * <h2>String array</h2>
     * <pre>
     * [0000,1001] [    ,    ] [    ,    ] [    ,    ] [    ,    ]
     * </pre>
     * <h2>Number array</h2>
     * <pre>
     * [0000,xxxx] [    ,    ] [    ,    ] type
     * [0000,    ] [xxxx,xxxx] [    ,    ] bits used in last byte
     * [0000,    ] [    ,    ] [xxxx,xxxx] required bits
     * </pre>
     */
    private static final int HEADER_SIZE = 8;
    public static final Charset UTF8 = Charset.forName( "UTF-8" );
    private final ReadStore store;
    private final int recordSize;

    ByteBlockCursor( ReadStore store, int recordSize )
    {
        this.store = store;
        this.recordSize = recordSize;
    }

    Object arrayProperty()
    {
        assert (unsignedByte( 0 ) & 0x80) == 0 : "can only read header from first block in chain";
        int itemType = unsignedByte( HEADER_SIZE );
        if ( itemType == PropertyCursor.STRING_REFERENCE )
        {
            return stringArray();
        }
        else if ( itemType <= PropertyCursor.DOUBLE )
        {
            int bitsUsedInLastByte = readByte( HEADER_SIZE + 1 ), requiredBits = readByte( HEADER_SIZE + 2 );
            switch ( itemType )
            {
            case PropertyCursor.BOOL:
                return readBooleanArray( 3, bitsUsedInLastByte, requiredBits );
            case PropertyCursor.BYTE:
                return readByteArray( 3, bitsUsedInLastByte, requiredBits );
            case PropertyCursor.SHORT:
                return readShortArray( 3, bitsUsedInLastByte, requiredBits );
            case PropertyCursor.CHAR:
                return readCharArray( 3, bitsUsedInLastByte, requiredBits );
            case PropertyCursor.INT:
                return readIntArray( 3, bitsUsedInLastByte, requiredBits );
            case PropertyCursor.LONG:
                return readLongArray( 3, bitsUsedInLastByte, requiredBits );
            case PropertyCursor.FLOAT:
                return readFloatArray( 3, bitsUsedInLastByte, requiredBits );
            case PropertyCursor.DOUBLE:
                return readDoubleArray( 3, bitsUsedInLastByte, requiredBits );
            default:
            }
        }
        throw new IllegalArgumentException( "Unknown array type: " + itemType );
    }

    String stringProperty()
    {
        byte[] buffer;
        int length;
        if ( !hasNext() )
        {
            buffer = new byte[dataBytes()];
            length = readChunk( HEADER_SIZE, buffer, 0, buffer.length );
        }
        else
        {
            buffer = new byte[nextPowerOfTwo( 2 * (dataBound() - HEADER_SIZE) )];
            length = 0; // TODO: read data!
        }
        return new String( buffer, 0, length, UTF8 );
    }

    private String[] stringArray()
    {
        // list of UTF8 encoded strings with length prefix
        String[] strings = new String[readInt( HEADER_SIZE + 1 )];
        int offset = HEADER_SIZE + 5;
        byte[] buffer = new byte[128];
        for ( int i = 0; i < strings.length; i++ )
        {
            int length = readInt( offset );
            offset += 4;
            if ( buffer.length < length )
            {
                int size = buffer.length;
                do
                {
                    size *= 2;
                }
                while ( size < length );
                buffer = new byte[size];
            }
            int pos, read = pos = readChunk( offset, buffer, 0, length );
            while ( pos < length )
            {
                if ( !next() )
                {
                    throw new BufferUnderflowException();
                }
                offset = HEADER_SIZE;
                read = readChunk( offset, buffer, pos, length );
                pos += read;
            }
            offset += read;
            strings[i] = new String( buffer, 0, length, UTF8 );
        }
        return strings;
    }

    private boolean[] readBooleanArray( int offset, int bitsUsedInLastByte, int requiredBits )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    private byte[] readByteArray( int offset, int bitsUsedInLastByte, int requiredBits )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    private short[] readShortArray( int offset, int bitsUsedInLastByte, int requiredBits )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    private char[] readCharArray( int offset, int bitsUsedInLastByte, int requiredBits )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    private int[] readIntArray( int offset, int bitsUsedInLastByte, int requiredBits )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    private long[] readLongArray( int offset, int bitsUsedInLastByte, int requiredBits )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    private float[] readFloatArray( int offset, int bitsUsedInLastByte, int requiredBits )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    private double[] readDoubleArray( int offset, int bitsUsedInLastByte, int requiredBits )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    private int readChunk( int offset, byte[] buffer, int pos, int length )
    {
        length = min( length - pos, dataBytes() + HEADER_SIZE - offset );
        read( offset, buffer, pos, length );
        return length;
    }

    boolean hasNext()
    {
        return nextReference() != -1;
    }

    boolean next()
    {
        long next = nextReference();
        if ( next == -1 )
        {
            return false;
        }
        store.block( next, this );
        return true;
    }

    int dataBytes()
    {
        return readInt( 0 ) & 0x00FF_FFFF;
    }

    int blockSize()
    {
        return dataBound() - HEADER_SIZE;
    }

    private long nextReference()
    {
        return combineReference( unsignedInt( 4 ), (readByte( 0 ) & 0x0FL) << 32 );
    }

    void get( int offset, byte[] target, int pos, int length )
    {
        read( HEADER_SIZE + offset, target, pos, length );
    }

    void get( int offset, short[] target, int pos, int length )
    {
        read( HEADER_SIZE + offset, target, pos, length );
    }

    void get( int offset, int[] target, int pos, int length )
    {
        read( HEADER_SIZE + offset, target, pos, length );
    }

    void get( int offset, long[] target, int pos, int length )
    {
        read( HEADER_SIZE + offset, target, pos, length );
    }

    void get( int offset, char[] target, int pos, int length )
    {
        read( HEADER_SIZE + offset, target, pos, length );
    }

    void get( int offset, float[] target, int pos, int length )
    {
        read( HEADER_SIZE + offset, target, pos, length );
    }

    void get( int offset, double[] target, int pos, int length )
    {
        read( HEADER_SIZE + offset, target, pos, length );
    }

    @Override
    protected int dataBound()
    {
        return recordSize;
    }
}
