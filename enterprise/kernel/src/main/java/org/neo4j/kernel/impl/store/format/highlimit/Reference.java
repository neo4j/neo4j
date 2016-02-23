/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.highlimit;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;

/**
 * {@link #encode(long, Object, DataAdapter) Encoding} and {@link #decode(Object, DataAdapter) decoding} of {@code long}
 * references, max 58-bit, into an as compact format as possible. Format is close to how utf-8 does similar encoding.
 *
 * Basically one or more header bits are used to note the number of bytes required to represent a
 * particular {@code long} value followed by the value itself. Number of bytes used for any long ranges from
 * 3 up to the full 8 bytes. The header bits sits in the most significant bit(s) of the most significant byte,
 * so for that the bytes that make up a value is written (and of course read) in big-endian order.
 *
 * Negative values are also supported, in order to handle relative references.
 *
 * @author Mattias Persson
 */
enum Reference
{
    // bit masks below contain one bit for 's' (sign) so actual address space is one bit less than advertised

    // 3-byte, 23-bit addr space: 0sxx xxxx xxxx xxxx xxxx xxxx
    BYTE_3( 3, (byte) 0b0, 1 ),

    // 4-byte, 30-bit addr space: 10sx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
    BYTE_4( 4, (byte) 0b10, 2 ),

    // 5-byte, 37-bit addr space: 110s xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
    BYTE_5( 5, (byte) 0b110, 3 ),

    // 6-byte, 44-bit addr space: 1110 sxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
    BYTE_6( 6, (byte) 0b1110, 4 ),

    // 7-byte, 51-bit addr space: 1111 0sxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
    BYTE_7( 7, (byte) 0b1111_0, 5 ),

    // 8-byte, 59-bit addr space: 1111 1sxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
    BYTE_8( 8, (byte) 0b1111_1, 5 );

    public interface DataAdapter<SOURCE>
    {
        byte get( SOURCE source );

        void put( byte oneByte, SOURCE source ) throws IOException;
    }

    public static final DataAdapter<PageCursor> PAGE_CURSOR_ADAPTER = new DataAdapter<PageCursor>()
    {
        @Override
        public byte get( PageCursor source )
        {
            return source.getByte();
        }

        @Override
        public void put( byte oneByte, PageCursor source )
        {
            source.putByte( oneByte );
        }
    };

    // Take one copy here since Enum#values() does an unnecessary defensive copy every time.
    private static final Reference[] ENCODINGS = Reference.values();

    private final int numberOfBytes;
    private final short highHeader;
    private final short headerMask;
    private final int headerShift;
    private short signBitMask;
    private final long valueOverflowMask;

    Reference( int numberOfBytes, byte header, int headerBits )
    {
        this.numberOfBytes = numberOfBytes;
        this.headerShift = Byte.SIZE - headerBits;
        this.highHeader = (short) (((byte) (header << headerShift)) & 0xFF);
        this.headerMask = (short) (((byte) (0xFF << headerShift)) & 0xFF);
        this.valueOverflowMask = ~valueMask( numberOfBytes, headerShift - 1 /*sign bit uses one bit*/ );
        this.signBitMask = (short) (0x1 << (headerShift - 1));
    }

    private long valueMask( int numberOfBytes, int headerShift )
    {
        long mask = ( 1L << headerShift ) - 1;
        for ( int i = 0; i < numberOfBytes - 1; i++ )
        {
            mask <<= 8;
            mask |= 0xFF;
        }
        return mask;
    }

    private boolean canEncode( long absoluteReference )
    {
        return (absoluteReference & valueOverflowMask) == 0;
    }

    private <SOURCE> void encode( long absoluteReference, boolean positive, SOURCE source,
            DataAdapter<SOURCE> adapter ) throws IOException
    {
        // use big-endianness, most significant byte written first, since it contains encoding information
        int shift = (numberOfBytes-1) << 3;
        byte signBit = (byte) ((positive ? 0 : 1) << (headerShift - 1));

        // first (most significant) byte
        adapter.put( (byte) (highHeader | signBit | (byte) (absoluteReference >>> shift)), source );

        do // rest of the bytes
        {
            shift -= 8;
            adapter.put( (byte) (absoluteReference >>> shift), source );
        }
        while ( shift > 0 );
    }

    private boolean canDecode( short firstByte )
    {
        return (firstByte & headerMask) == highHeader;
    }

    private <SOURCE> long decode( short firstByte, SOURCE source, DataAdapter<SOURCE> adapter )
    {
        int shift = (numberOfBytes-1) << 3;
        boolean positive = (firstByte & signBitMask) == 0;

        // first (most significant) byte
        long mask = ~(0xFFL << (headerShift - 1));
        long result = (mask & firstByte) << shift;

        do // rest of the bytes
        {
            shift -= 8;
            long currentByte = adapter.get( source ) & 0xFFL;
            result |= (currentByte << shift);
        }
        while ( shift > 0 );

        return positive ? result : ~result;
    }

    private int maxBitsSupported()
    {
        return Long.SIZE - Long.numberOfLeadingZeros( ~valueOverflowMask );
    }

    public static <TARGET> void encode( long reference, TARGET target, DataAdapter<TARGET> adapter ) throws IOException
    {
        // checking with < 0 seems to be the fastest way of telling
        boolean positive = reference >= 0;
        long absoluteReference = positive ? reference : ~reference;

        for ( Reference encoding : ENCODINGS )
        {
            if ( encoding.canEncode( absoluteReference ) )
            {
                encoding.encode( absoluteReference, positive, target, adapter );
                return;
            }
        }
        throw unsupportedOperationDueToTooBigReference( reference );
    }

    private static UnsupportedOperationException unsupportedOperationDueToTooBigReference( long reference )
    {
        return new UnsupportedOperationException( format( "Reference %d uses too many bits to be encoded by "
                + "current compression scheme, max %d bits allowed", reference, maxBits() ) );
    }

    public static int length( long reference )
    {
        boolean positive = reference >= 0;
        long absoluteReference = positive ? reference : ~reference;

        for ( Reference encoding : ENCODINGS )
        {
            if ( encoding.canEncode( absoluteReference ) )
            {
                return encoding.numberOfBytes;
            }
        }
        throw unsupportedOperationDueToTooBigReference( reference );
    }

    private static int maxBits()
    {
        int max = 0;
        for ( Reference encoding : ENCODINGS )
        {
            max = Math.max( max, encoding.maxBitsSupported() );
        }
        return max;
    }

    public static <SOURCE> long decode( SOURCE source, DataAdapter<SOURCE> adapter )
    {
        short firstByte = (short) (adapter.get( source ) & 0xFF);
        for ( Reference encoding : ENCODINGS )
        {
            if ( encoding.canDecode( firstByte ) )
            {
                return encoding.decode( firstByte, source, adapter );
            }
        }
        throw new UnsupportedOperationException( "Reference with first byte " + firstByte + " wasn't recognized"
                + " as a reference" );
    }


    /**
     * Convert provided reference to be relative to basisReference
     * @param reference reference that will be converter to relative
     * @param basisReference conversion basis
     * @return reference relative to basisReference
     */
    public static long toRelative( long reference, long basisReference )
    {
        return Math.subtractExact( reference , basisReference );
    }

    /**
     * Convert provided relative to basis reference into absolute
     * @param relativeReference relative reference to convert
     * @param basisReference basis reference
     * @return absolute reference
     */
    public static long toAbsolute( long relativeReference, long basisReference )
    {
        return Math.addExact( relativeReference, basisReference );
    }
}
