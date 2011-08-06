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

public class Bits
{
    // item[0] is most significant, last is least significant
    private final long[] longs;
    
    public Bits( int numberOfBytes )
    {
        longs = new long[numberOfBytes/8];
    }
    
    public Bits( long[] longs )
    {
        this.longs = longs;
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
        long overspillMask = rightOverspillMask( steps );
        long overspill = 0;
        for ( int i = 0; i < longs.length; i++ )
        {
            long nextOverspill = (longs[i] & overspillMask);
            longs[i] = (longs[i] >> steps) | (overspill << (64-steps));
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
    
    public long getLong( long mask )
    {
        return longs[longs.length-1] & mask;
    }
    
    public long[] getLongs()
    {
        return longs;
    }
    
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
