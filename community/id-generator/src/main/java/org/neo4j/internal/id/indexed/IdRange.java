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
package org.neo4j.internal.id.indexed;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

import static java.lang.Long.toBinaryString;
import static java.lang.String.format;
import static java.util.Arrays.fill;

/**
 * Value in a GB+Tree for indexing id states. Accompanies that with a generation, i.e. which generation this value were written in.
 * ID states are kept in three bit-sets, each consisting of one or more {@code long}s. The three bit-sets are:
 * <ul>
 *     <li>Commit bits, i.e. 0=used, 1=unused</li>
 *     <li>Reuse bits, i.e. 0=not reusable, 1=free</li>
 *     <li>Reserved bits, i.e. 0=not reserved, 1=reserved</li>
 * </ul>
 *
 * Each {@link IdRange} is associated with an {@link IdRangeKey} which specifies the range, e.g. an ID range of 3 in a layout where ids-per-entry is 128
 * holds IDs between 384-511.
 */
class IdRange
{
    static final int BITSET_COUNT = 3;

    static final int BITSET_COMMIT = 0;
    static final int BITSET_REUSE = 1;
    static final int BITSET_RESERVED = 2;

    /**
     * Each {@code long} contains two bit-sets, one for commit bits and one for reuse bits
     */
    static final int BITSET_SIZE = Long.SIZE;

    private long generation;
    private boolean addition;
    private final long[][] bitSets;
    private final int numOfLongs;

    IdRange( int numOfLongs )
    {
        this.bitSets = new long[BITSET_COUNT][numOfLongs];
        this.numOfLongs = numOfLongs;
    }

    IdState getState( int n )
    {
        int longIndex = n / BITSET_SIZE;
        int bitIndex = n % BITSET_SIZE;
        boolean commitBit = (bitSets[BITSET_COMMIT][longIndex] & bitMask( bitIndex )) != 0;
        if ( commitBit )
        {
            boolean reuseBit = (bitSets[BITSET_REUSE][longIndex] & bitMask( bitIndex )) != 0;
            boolean reservedBit = (bitSets[BITSET_RESERVED][longIndex] & bitMask( bitIndex )) != 0;
            return reuseBit && !reservedBit ? IdState.FREE : IdState.DELETED;
        }
        return IdState.USED;
    }

    private static long bitMask( int bitIndex )
    {
        return 1L << bitIndex;
    }

    void setBit( int type, int n )
    {
        int longIndex = n / BITSET_SIZE;
        int bitIndex = n % BITSET_SIZE;
        bitSets[type][longIndex] |= bitMask( bitIndex );
    }

    void setBitsForAllTypes( int n )
    {
        int longIndex = n / BITSET_SIZE;
        int bitIndex = n % BITSET_SIZE;
        bitSets[BITSET_COMMIT][longIndex] |= bitMask( bitIndex );
        bitSets[BITSET_REUSE][longIndex] |= bitMask( bitIndex );
        bitSets[BITSET_RESERVED][longIndex] |= bitMask( bitIndex );
    }

    void clear( long generation, boolean addition )
    {
        this.generation = generation;
        this.addition = addition;
        fill( bitSets[BITSET_COMMIT], 0 );
        fill( bitSets[BITSET_REUSE], 0 );
        fill( bitSets[BITSET_RESERVED], 0 );
    }

    long getGeneration()
    {
        return generation;
    }

    void setGeneration( long generation )
    {
        this.generation = generation;
    }

    long[][] getBitSets()
    {
        return bitSets;
    }

    void normalize()
    {
        for ( int i = 0; i < numOfLongs; i++ )
        {
            // Set the reuse bits to whatever the commit bits are. This will let USED be USED and DELETED will become FREE
            bitSets[BITSET_REUSE][i] = bitSets[BITSET_COMMIT][i];
            bitSets[BITSET_RESERVED][i] = 0;
        }
    }

    boolean mergeFrom( IdRange other, boolean recoveryMode )
    {
        if ( !recoveryMode )
        {
            verifyMerge( other );
        }

        for ( int bitSetIndex = 0; bitSetIndex < BITSET_COUNT; bitSetIndex++ )
        {
            mergeBitSet( bitSets[bitSetIndex], other.bitSets[bitSetIndex], other.addition );
        }

        return true;
    }

    private static void mergeBitSet( long[] into, long[] mergeFrom, boolean addition )
    {
        for ( int i = 0; i < into.length; i++ )
        {
            into[i] = addition ? into[i] | mergeFrom[i]
                               : into[i] & ~mergeFrom[i];
        }
    }

    private void verifyMerge( IdRange other )
    {
        boolean addition = other.addition;
        long[] intoBitSet = bitSets[BITSET_COMMIT];
        long[] fromBitSet = other.bitSets[BITSET_COMMIT];
        for ( int i = 0; i < intoBitSet.length; i++ )
        {
            long into = intoBitSet[i];
            long from = fromBitSet[i];
            if ( addition )
            {
                if ( (into & from ) != 0 )
                {
                    throw new IllegalStateException( format( "Illegal addition ID state transition longIdx: %d%ninto: %s%nfrom: %s",
                            i, toPaddedBinaryString( into ), toPaddedBinaryString( from ) ) );
                }
            }
            // don't very removal since we can't quite verify transitioning to USED since 0 is the default bit value
        }
    }

    private static String toPaddedBinaryString( long bits )
    {
        char[] padded = StringUtils.leftPad( toBinaryString( bits ), Long.SIZE, '0' ).toCharArray();

        // Now add a space between each byte
        int numberOfSpaces = padded.length / Byte.SIZE - 1;
        char[] spaced = new char[padded.length + numberOfSpaces];
        Arrays.fill( spaced, ' ' );
        for ( int i = 0; i < numberOfSpaces + 1; i++ )
        {
            System.arraycopy( padded, i * Byte.SIZE, spaced, i * Byte.SIZE + i, Byte.SIZE );
        }
        return String.valueOf( spaced );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder().append( " gen:" ).append( generation );
        appendBitSet( builder, bitSets[BITSET_COMMIT], "commit  " );
        appendBitSet( builder, bitSets[BITSET_REUSE], "reuse   " );
        appendBitSet( builder, bitSets[BITSET_RESERVED], "reserved" );
        return builder.toString();
    }

    private static void appendBitSet( StringBuilder builder, long[] bitSet, String name )
    {
        builder.append( format( "%n" ) ).append( name ).append( ':' );
        String delimiter = "";
        for ( int i = bitSet.length - 1; i >= 0; i-- )
        {
            builder.append( delimiter ).append( toPaddedBinaryString( bitSet[i] ) );
            delimiter = " , ";
        }
    }

    public boolean isEmpty()
    {
        for ( long bits : bitSets[BITSET_COMMIT] )
        {
            if ( bits != 0 )
            {
                return false;
            }
        }
        return true;
    }

    enum IdState
    {
        USED,
        DELETED,
        FREE
    }
}
