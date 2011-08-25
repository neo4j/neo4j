/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.Arrays;

import org.neo4j.kernel.impl.nioneo.store.Buffer;

/**
 * Got bits to store, shift and retrieve and they are more than what fits in a long?
 * Use {@link Bits} then.
 */
public class Bits
{
    // item[0] is most significant, last is least significant
    private final long[] longs;
    private final int numberOfBytes;
    
    public static Bits bits( int numberOfBytes )
    {
        return new Bits( numberOfBytes );
    }
    
    public Bits( int numberOfBytes )
    {
        int requiredLongs = numberOfBytes/8;
        if ( numberOfBytes%8 > 0 ) requiredLongs++;
        longs = new long[requiredLongs];
        this.numberOfBytes = numberOfBytes;
    }
    
    public Bits( long[] longs )
    {
        this.longs = longs;
        this.numberOfBytes = longs.length*8;
    }

    /**
     * A mask which has the {@code steps} most significant bits set to 1, all others 0.
     * It's used to carry bits over between carriers (longs) when shifting left.
     * @param steps the number of most significant bits to have set to 1 in the mask.
     * @return the created mask.
     */
    public static long leftOverflowMask( int steps )
    {
        long mask = 0L;
        for ( int i = 0; i < steps; i++ )
        {
            mask >>= 1;
            mask |= 0x8000000000000000L;
        }
        return mask;
    }
    
    /**
     * A mask which has the {@code steps} least significant bits set to 1, all others 0.
     * It's used to carry bits over between carriers (longs) when shifting right.
     * @param steps the number of least significant bits to have set to 1 in the mask.
     * @return the created mask.
     */
    public static long rightOverflowMask( int steps )
    {
        long mask = 0L;
        for ( int i = 0; i < steps; i++ )
        {
            mask <<= 1;
            mask |= 0x1L;
        }
        return mask;
    }
    
    /**
     * Shifts all bits left {@code steps}.
     * @param steps the number of steps to shift.
     * @return this instance.
     */
    public Bits shiftLeft( int steps )
    {
        while ( steps >= 64 )
        {
            for ( int i = 0; i < longs.length-1; i++ )
            {
                longs[i] = longs[i+1];
                longs[i+1] = 0;
                steps -= 64;
            }
        }
        
        long overspillMask = leftOverflowMask( steps );
        long overspill = 0;
        for ( int i = longs.length-1; i >= 0; i-- )
        {
            long nextOverspill = (longs[i] & overspillMask) >>> (64-steps);
            longs[i] = (longs[i] << steps) | overspill;
            overspill = nextOverspill;
        }
        return this;
    }
    
    /**
     * Shifts all bits right {@code steps}.
     * @param steps the number of steps to shift.
     * @return this instance.
     */
    public Bits shiftRight( int steps )
    {
        while ( steps >= 64 )
        {
            for ( int i = longs.length-1; i > 0; i-- )
            {
                longs[i] = longs[i-1];
                longs[i-1] = 0;
                steps -= 64;
            }
        }
        
        long overspillMask = rightOverflowMask( steps );
        long overspill = 0;
        for ( int i = 0; i < longs.length; i++ )
        {
            long nextOverspill = (longs[i] & overspillMask);
            longs[i] = (longs[i] >>> steps) | (overspill << (64-steps));
            overspill = nextOverspill;
        }
        return this;
    }
    
    /**
     * Applies the bits from {@code value} upon the least significant bits using OR.
     * @param value the value to apply.
     * @param mask mask to mask out the bits from {@code value}.
     * @return this instance.
     */
    public Bits or( byte value, long mask )
    {
        longs[longs.length-1] |= value & mask;
        return this;
    }
    
    /**
     * Applies the bits from {@code value} upon the least significant bits using OR.
     * @param value the value to apply.
     * @return this instance.
     */
    public Bits or( byte value )
    {
        return or( value, 0xFF );
    }
    
    /**
     * Applies the bits from {@code value} upon the least significant bits using OR.
     * @param value the value to apply.
     * @param mask mask to mask out the bits from {@code value}.
     * @return this instance.
     */
    public Bits or( short value, long mask )
    {
        longs[longs.length-1] |= value & mask;
        return this;
    }
    
    /**
     * Applies the bits from {@code value} upon the least significant bits using OR.
     * @param value the value to apply.
     * @return this instance.
     */
    public Bits or( short value )
    {
        return or( value, 0xFFFF );
    }
    
    /**
     * Applies the bits from {@code value} upon the least significant bits using OR.
     * @param value the value to apply.
     * @param mask mask to mask out the bits from {@code value}.
     * @return this instance.
     */
    public Bits or( int value, long mask )
    {
        longs[longs.length-1] |= value & mask;
        return this;
    }
    
    /**
     * Applies the bits from {@code value} upon the least significant bits using OR.
     * @param value the value to apply.
     * @return this instance.
     */
    public Bits or( int value )
    {
        return or( value, 0xFFFFFFFFL );
    }
    
    /**
     * Applies the bits from {@code value} upon the least significant bits using OR.
     * @param value the value to apply.
     * @param mask mask to mask out the bits from {@code value}.
     * @return this instance.
     */
    public Bits or( long value, long mask )
    {
        longs[longs.length-1] |= value & mask;
        return this;
    }
    
    /**
     * Applies the bits from {@code value} upon the least significant bits using OR.
     * @param value the value to apply.
     * @return this instance.
     */
    public Bits or( long value )
    {
        return or( value, 0xFFFFFFFFFFFFFFFFL );
    }
    
    private Bits highOr( byte value, int steps )
    {
        longs[0] |= ((long)(value & rightOverflowMask( steps )) << (64-steps));
        return this;
    }
    
    private Bits highOr( byte value )
    {
        return or( value, Byte.SIZE );
    }
    
    private Bits highOr( short value, int steps )
    {
        longs[0] |= ((long)(value & rightOverflowMask( steps )) << (64-steps));
        return this;
    }
    
    private Bits highOr( short value )
    {
        return or( value, Short.SIZE );
    }
    
    private Bits highOr( int value, int steps )
    {
        longs[0] |= ((long)(value & rightOverflowMask( steps )) << (64-steps));
        return this;
    }
    
    private Bits highOr( int value )
    {
        return or( value, Integer.SIZE );
    }
    
    private Bits highOr( long value, int steps )
    {
        longs[0] |= ((long)(value & rightOverflowMask( steps )) << (64-steps));
        return this;
    }
    
    private Bits highOr( long value )
    {
        return or( value, Long.SIZE );
    }
    
    /**
     * Returns the byte representation from the least significant bits using {@code mask}.
     * @param mask for masking out the bits.
     * @return the byte representation from the least significant bits using {@code mask}.
     */
    public byte getByte( byte mask )
    {
        return (byte) (longs[longs.length-1] & mask);
    }
    
    /**
     * Returns the short representation from the least significant bits using {@code mask}.
     * @param mask for masking out the bits.
     * @return the short representation from the least significant bits using {@code mask}.
     */
    public short getShort( short mask )
    {
        return (short) (longs[longs.length-1] & mask);
    }
    
    /**
     * Returns the int representation from the least significant bits using {@code mask}.
     * @param mask for masking out the bits.
     * @return the int representation from the least significant bits using {@code mask}.
     */
    public int getInt( int mask )
    {
        return (int) (longs[longs.length-1] & mask);
    }
    
    /**
     * Returns the int representation as long from the least significant bits using {@code mask}.
     * @param mask for masking out the bits.
     * @return the int representation as long from the least significant bits using {@code mask}.
     */
    public long getUnsignedInt( int mask )
    {
        return getInt( mask ) & 0xFFFFFFFF;
    }
    
    /**
     * Returns the long representation from the least significant bits using {@code mask}.
     * @param mask for masking out the bits.
     * @return the long representation from the least significant bits using {@code mask}.
     */
    public long getLong( long mask )
    {
        return (longs[longs.length-1] & mask);
    }
    
    /**
     * Returns the underlying long values that has got all the bits applied.
     * The first item in the array has got the most significant bits.
     * @return the underlying long values that has got all the bits applied.
     */
    public long[] getLongs()
    {
        return longs;
    }
    
    /**
     * Writes all bits to {@code buffer}.
     * @param buffer the {@link Buffer} to write to.
     * @return this instance.
     */
    public Bits apply( Buffer buffer )
    {
        int rest = numberOfBytes%8;
        if ( rest > 0 )
        {
            // Uneven, extract the bytes from the first long
            int steps = (rest-1)*8;
            long mask = 0xFFL << steps;
            long source = longs[0];
            for ( int i = 0; i < rest; i++ )
            {
                byte value = (byte) ((source & mask) >>> steps);
                buffer.put( value );
                mask >>>= 8;
                steps -= 8;
            }
        }
        else
        {
            buffer.putLong( longs[0] );
        }
        
        for ( int i = 1; i < longs.length; i++ )
        {
            buffer.putLong( longs[i] );
        }
        return this;
    }
    
    /**
     * Reads from {@code buffer} and fills up all the bits.
     * @param buffer the {@link Buffer} to read from.
     * @return this instance.
     */
    public Bits read( Buffer buffer )
    {
        int rest = numberOfBytes%8;
        while ( rest > 0 )
        {
            byte value = buffer.get();
            shiftLeft( 8 );
            or( value, 0xFF );
            rest--;
        }
        
        int longs = numberOfBytes/8;
        for ( int i = 0; i < longs; i++ )
        {
            shiftLeft( 64 );
            or( buffer.getLong(), 0xFFFFFFFFFFFFFFFFL );
        }
        return this;
    }

    /**
     * A very nice toString, showing each bit, divided into groups of bytes and
     * lines of 8 bytes.
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        int byteCounter = 0;
        for ( long value : longs )
        {
            builder.append( "[" );
            for ( int i = 63; i >= 0; i-- )
            {
                boolean isSet = (value & (1L << i)) != 0;
                builder.append( isSet ? "1" : "0" );
                if ( i%8 == 0 && i > 0 )
                {
                    builder.append( "," );
                }
            }
            builder.append( "]" );
            if ( byteCounter++ % 8 == 0 )
            {
                builder.append( "\n" );
            }
        }
        return builder.toString();
    }
    
    @Override
    public Bits clone()
    {
        return new Bits( Arrays.copyOf( longs, longs.length ) );
    }
    
    public Bits pushLeft( byte value, int steps )
    {
        shiftLeft( steps );
        or( value, rightOverflowMask( steps ) );
        return this;
    }

    public Bits pushLeft( byte value )
    {
        return pushLeft( value, Byte.SIZE );
    }
    
    public Bits pushLeft( short value, int steps )
    {
        shiftLeft( steps );
        or( value, rightOverflowMask( steps ) );
        return this;
    }

    public Bits pushLeft( short value )
    {
        return pushLeft( value, Short.SIZE );
    }
    
    public Bits pushLeft( int value, int steps )
    {
        shiftLeft( steps );
        or( value, rightOverflowMask( steps ) );
        return this;
    }

    public Bits pushLeft( int value )
    {
        return pushLeft( value, Integer.SIZE );
    }
    
    public Bits pushLeft( long value, int steps )
    {
        shiftLeft( steps );
        or( value, rightOverflowMask( steps ) );
        return this;
    }
    
    public Bits pushLeft( long value )
    {
        return pushLeft( value, Long.SIZE );
    }
    
    public Bits pushRight( byte value, int steps )
    {
        shiftRight( steps );
        highOr( value, steps );
        return this;
    }

    public Bits pushRight( byte value )
    {
        return pushRight( value, Byte.SIZE );
    }
    
    public Bits pushRight( short value, int steps )
    {
        shiftRight( steps );
        highOr( value, steps );
        return this;
    }

    public Bits pushRight( short value )
    {
        return pushRight( value, Short.SIZE );
    }
    
    public Bits pushRight( int value, int steps )
    {
        shiftRight( steps );
        highOr( value, steps );
        return this;
    }

    public Bits pushRight( int value )
    {
        return pushRight( value, Integer.SIZE );
    }
    
    public Bits pushRight( long value, int steps )
    {
        shiftRight( steps );
        highOr( value, steps );
        return this;
    }

    public Bits pushRight( long value )
    {
        return pushRight( value, Long.SIZE );
    }
    
    public byte pullRightByte( int steps )
    {
        byte value = getByte( (byte) rightOverflowMask( steps ) );
        shiftRight( steps );
        return value;
    }

    public byte pullRightByte()
    {
        return pullRightByte( Byte.SIZE );
    }
    
    public short pullRightShort( int steps )
    {
        short value = getShort( (short) rightOverflowMask( steps ) );
        shiftRight( steps );
        return value;
    }
    
    public short pullRightShort()
    {
        return pullRightShort( Short.SIZE );
    }

    public int pullRightInt( int steps )
    {
        int value = getInt( (int) rightOverflowMask( steps ) );
        shiftRight( steps );
        return value;
    }

    public int pullRightInt()
    {
        return pullRightInt( Integer.SIZE );
    }
    
    public long pullRightUnsignedInt( int steps )
    {
        long value = getUnsignedInt( (int) rightOverflowMask( steps ) );
        shiftRight( steps );
        return value;
    }
    
    public long pullRightUnsignedInt()
    {
        return pullRightUnsignedInt( Integer.SIZE );
    }
    
    public long pullRightLong( int steps )
    {
        long value = getLong( rightOverflowMask( steps ) );
        shiftRight( steps );
        return value;
    }
    
    public long pullRightLong()
    {
        return pullRightLong( Long.SIZE );
    }
    
    public byte pullLeftByte( int steps )
    {
        byte result = (byte)((longs[0] & leftOverflowMask( steps )) >>> (64-steps));
        shiftLeft( steps );
        return result;
    }

    public byte pullLeftByte()
    {
        return pullLeftByte( Byte.SIZE );
    }
    
    public short pullLeftShort( int steps )
    {
        short result = (short)((longs[0] & leftOverflowMask( steps )) >>> (64-steps));
        shiftLeft( steps );
        return result;
    }

    public short pullLeftShort()
    {
        return pullLeftShort( Short.SIZE );
    }
    
    public int pullLeftInt( int steps )
    {
        int result = (int)((longs[0] & leftOverflowMask( steps )) >>> (64-steps));
        shiftLeft( steps );
        return result;
    }

    public int pullLeftInt()
    {
        return pullLeftInt( Integer.SIZE );
    }
    
    public long pullLeftUnsignedInt( int steps )
    {
        long result = (int)((longs[0] & leftOverflowMask( steps )) >>> (64-steps));
        shiftLeft( steps );
        return result;
    }
    
    public long pullLeftUnsignedInt()
    {
        return pullLeftUnsignedInt( Integer.SIZE );
    }
    
    public long pullLeftLong( int steps )
    {
        long result = (longs[0] & leftOverflowMask( steps )) >>> (64-steps);
        shiftLeft( steps );
        return result;
    }

    public long pullLeftLong()
    {
        return pullLeftLong( Long.SIZE );
    }
}
