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
    
    private long leftOverspillMask( int steps )
    {
        long mask = 0L;
        for ( int i = 0; i < steps; i++ )
        {
            mask >>= 1;
            mask |= 0x8000000000000000L;
        }
        return mask;
    }
    
    public static long rightOverspillMask( int steps )
    {
        long mask = 0L;
        for ( int i = 0; i < steps; i++ )
        {
            mask <<= 1;
            mask |= 0x1L;
        }
        return mask;
    }
    
    public void shiftLeft( int steps )
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
        
        long overspillMask = leftOverspillMask( steps );
        long overspill = 0;
        for ( int i = longs.length-1; i >= 0; i-- )
        {
            long nextOverspill = (longs[i] & overspillMask) >>> (64-steps);
            longs[i] = (longs[i] << steps) | overspill;
            overspill = nextOverspill;
        }
    }
    
    public void shiftRight( int steps )
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
        
        long overspillMask = rightOverspillMask( steps );
        long overspill = 0;
        for ( int i = 0; i < longs.length; i++ )
        {
            long nextOverspill = (longs[i] & overspillMask);
            longs[i] = (longs[i] >>> steps) | (overspill << (64-steps));
            overspill = nextOverspill;
        }
    }
    
    public void or( byte value, long mask )
    {
        longs[longs.length-1] |= value & mask;
    }
    
    public void or( short value, long mask )
    {
        longs[longs.length-1] |= value & mask;
    }
    
    public void or( int value, long mask )
    {
        longs[longs.length-1] |= value & mask;
    }
    
    public void or( long value, long mask )
    {
        longs[longs.length-1] |= value & mask;
    }
    
    public byte getByte( byte mask )
    {
        return (byte) (longs[longs.length-1] & mask);
    }
    
    public short getShort( short mask )
    {
        return (short) (longs[longs.length-1] & mask);
    }
    
    public int getInt( int mask )
    {
        return (int) (longs[longs.length-1] & mask);
    }
    
    public long getUnsignedInt( int mask )
    {
        return getInt( mask ) & 0xFFFFFFFF;
    }
    
    public long getLong( long mask )
    {
        return longs[longs.length-1] & mask;
    }
    
    public long[] getLongs()
    {
        return longs;
    }
    
    public void apply( Buffer buffer )
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
    }
    
    public void read( Buffer buffer )
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
    }

    /**
     * A very nice toString, showing each bit, divided into groups of bytes.
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
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
        }
        return builder.toString();
    }
}
