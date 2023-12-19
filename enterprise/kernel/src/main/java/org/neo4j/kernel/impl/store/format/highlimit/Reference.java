/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;

/**
 * {@link #encode(long, PageCursor) Encoding} and {@link #decode(PageCursor) decoding} of {@code long}
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
public enum Reference
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

    // Take one copy here since Enum#values() does an unnecessary defensive copy every time.
    private static final Reference[] ENCODINGS = Reference.values();

    static final int MAX_BITS = 58;

    private final int numberOfBytes;
    private final short highHeader;
    private final int headerShift;
    private final long valueOverflowMask;

    Reference( int numberOfBytes, byte header, int headerBits )
    {
        this.numberOfBytes = numberOfBytes;
        this.headerShift = Byte.SIZE - headerBits;
        this.highHeader = (short) (((byte) (header << headerShift)) & 0xFF);
        this.valueOverflowMask = ~valueMask( numberOfBytes, headerShift - 1 /*sign bit uses one bit*/ );
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

    private void encode( long absoluteReference, boolean positive, PageCursor source )
    {
        // use big-endianness, most significant byte written first, since it contains encoding information
        int shift = (numberOfBytes - 1) << 3;
        byte signBit = (byte) ((positive ? 0 : 1) << (headerShift - 1));

        // first (most significant) byte
        source.putByte( (byte) (highHeader | signBit | (byte) (absoluteReference >>> shift)) );

        do // rest of the bytes
        {
            shift -= 8;
            source.putByte( (byte) (absoluteReference >>> shift) );
        }
        while ( shift > 0 );
    }

    private int maxBitsSupported()
    {
        return Long.SIZE - Long.numberOfLeadingZeros( ~valueOverflowMask );
    }

    public static void encode( long reference, PageCursor target )
    {
        // checking with < 0 seems to be the fastest way of telling
        boolean positive = reference >= 0;
        long absoluteReference = positive ? reference : ~reference;

        for ( Reference encoding : ENCODINGS )
        {
            if ( encoding.canEncode( absoluteReference ) )
            {
                encoding.encode( absoluteReference, positive, target );
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

    public static long decode( PageCursor source )
    {
        // Dear future maintainers, this code is a little complicated so I'm going to take some time and explain it to
        // you. Make sure you have some coffee ready.
        //
        // Before we start, I have one plea: Please don't extract the constants out of this function. It is easier to
        // make sense of them when they are embedded within the context of the code. Also, while some of the constants
        // have the same value, they might change for different reasons, so let's just keep them inlined.
        //
        // The code is easier to read when it's all together, so I'll keep the code and the comment separate, and make
        // the comment refer to the code with <N> marks.
        //
        // <1>
        // The first byte of a reference is the header byte. It is an unsigned byte where all the bits matter, but Java
        // has no such concept as an unsigned byte, so we instead store the byte in a 32-bit int, and mask it with 0xFF
        // to read it as if it was unsigned. The 0xFF mask makes sure that the highest-order bit, which would otherwise
        // be used as a sign-bit, stays together with the other 7 bits in the lowest-order byte of the int.
        //
        // <2>
        // The header determines how many bytes go into the reference. These are the size marks. If the first bit of
        // the header is zero, then we have zero size marks and the reference takes up 3 bytes. If the header starts
        // with the bits 10, then we have one size mark and the reference takes up 4 bytes. We can have up to 5 size
        // marks, where the last two options are 11110 for a 7 byte reference, and 11111 for an 8 byte reference.
        // We count the size marks as follows:
        //  1. First extract the 5 high-bits. 0xF8 is 11111000, so xxxx_xxxx & 0xF8 => xxxx_x000.
        //  2. The x'es are a number of ones, possibly zero, followed by a zero. There's an instruction to count
        //     leading zeros, but not leading ones, so we have to invert the 1 size marks into 0s, and the possible 0
        //     end mark into a 1. We use the `& 0xFF` trick to prevent the leading size mark from turning into a
        //     sign-bit. So (~xxxx_x000) & 0xFF => XXXX_X111, e.g. 0111_1000 (no size marks) becomes 1000_0111, and
        //     1101_1000 (two size marks) becomes 0010_0111.
        //  3. Now we can count the leading zeros to find the end mark. Remember that the end-mark is the zero-bit after
        //     the size marks. We *always* have this end-mark at this point, because any 1 in the highest-bit of the
        //     reference was masked to 0 in step 1 above.
        //  4. When we count the number of leading zeros, we have thus far been thinking about the header as a single
        //     byte. However, the register we have been working on is a 32-bit integer, so we have to subtract 3 times 8
        //     bits to get the number of size marks in the original header *byte*.
        //
        // <3>
        // The sign-bit is located after the end-mark, or after the last size mark in the case of an 8 byte reference.
        // We have 8 bits in the header byte, so if we want to place the sign-bit at the lowest-order bit location,
        // then we can think of the size marks and optional end-mark as a pre-shift, pushing the sign-bit towards the
        // low end. We just have to figure out how many bits are left to shift over.
        //
        // <4>
        // If the sign-bit is 1, then we want to produce the 64-bit signed integer number -1, which consists of 64
        // consecutive 1-bits. If the sign-bit is 0, then we want to produce 0, which in binary is 64 consecutive
        // 0-bits. The reason we do this is how negative numbers work. It turns out that -X == -1 ^ (X - 1). Since
        // our compression scheme is all about avoiding the storage of unnecessary high-order zeros, we can more easily
        // store the (X - 1) part plus a sign bit, than a long string of 1-bits followed by useful data. For example,
        // the negative number -42 is 1111111111111111111111111111111111111111111111111111111111010110 in binary,
        // while 41 is just 101001. And given our equation above, -1 ^ 41 == -42.
        //
        // <5>
        // After the size marks, the end-mark and the sign-bit comes a few bits of payload data. The sign-bit location
        // marks the end of the meta-data bits, so we use that as a base for computing a mask that will remove all the
        // meta-data bits. Since the smallest reference takes up 3 bytes, we can immediately shift those payload bits
        // up 16 places to make room for the next two bytes of payload.
        //
        // <6>
        // Then we read the next two bytes (with unsigned mask) and save for the sign-component manipulation, we now
        // have a complete 3-byte reference.
        //
        // <7>
        // The size marks determines how many more bytes the reference takes up, so we loop through them and shift the
        // register up 8 places every time, and add in the next byte with an unsigned mask.
        //
        // <8>
        // Finally XOR the register with the sign component and we have our final value.

        int header = source.getByte() & 0xFF; // <1>
        int sizeMarks = Integer.numberOfLeadingZeros( (~(header & 0xF8)) & 0xFF ) - 24; // <2>
        int signShift = 8 - sizeMarks - (sizeMarks == 5 ? 1 : 2); // <3>
        long signComponent = ~((header >>> signShift) & 1) + 1; // <4>
        long register = (header & ((1 << signShift) - 1)) << 16; // <5>
        register += ((source.getByte() & 0xFF) << 8) + (source.getByte() & 0xFF); // <6>

        while ( sizeMarks > 0 ) // <7>
        {
            register <<= 8;
            register += source.getByte() & 0xFF;
            sizeMarks--;
        }

        return signComponent ^ register; // <8>
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
