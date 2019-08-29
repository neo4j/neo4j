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
    private static final int BITSET_COUNT = 3;

    static final int BITSET_COMMIT = 0;
    static final int BITSET_REUSE = 1;
    static final int BITSET_RESERVED = 2;

    /**
     * Each {@code long} contains two bit-sets, one for commit bits and one for reuse bits
     */
    static final int BITSET_SIZE = Long.SIZE;

    private long generation;
    private transient boolean addition;
    private final long[][] bitsets;
    private final int numOfLongs;

    IdRange( int numOfLongs )
    {
        this.bitsets = new long[BITSET_COUNT][numOfLongs];
        this.numOfLongs = numOfLongs;
    }

    IdState getState( int n )
    {
        int longIndex = n / BITSET_SIZE;
        int bitIndex = n % BITSET_SIZE;
        boolean commitBit = (bitsets[BITSET_COMMIT][longIndex] & bitMask( bitIndex )) != 0;
        if ( commitBit )
        {
            boolean reuseBit = (bitsets[BITSET_REUSE][longIndex] & bitMask( bitIndex )) != 0;
            boolean reservedBit = (bitsets[BITSET_RESERVED][longIndex] & bitMask( bitIndex )) != 0;
            return reuseBit && !reservedBit ? IdState.FREE : IdState.DELETED;
        }
        return IdState.USED;
    }

    private long bitMask( int bitIndex )
    {
        return 1L << bitIndex;
    }

    void setBit( int type, int n )
    {
        int longIndex = n / BITSET_SIZE;
        int bitIndex = n % BITSET_SIZE;
        bitsets[type][longIndex] |= bitMask( bitIndex );
    }

    void setBitsForAllTypes( int n )
    {
        int longIndex = n / BITSET_SIZE;
        int bitIndex = n % BITSET_SIZE;
        bitsets[BITSET_COMMIT][longIndex] |= bitMask( bitIndex );
        bitsets[BITSET_REUSE][longIndex] |= bitMask( bitIndex );
        bitsets[BITSET_RESERVED][longIndex] |= bitMask( bitIndex );
    }

    void clear( long generation, boolean addition )
    {
        this.generation = generation;
        this.addition = addition;
        fill( bitsets[BITSET_COMMIT], 0 );
        fill( bitsets[BITSET_REUSE], 0 );
        fill( bitsets[BITSET_RESERVED], 0 );
    }

    long getGeneration()
    {
        return generation;
    }

    void setGeneration( long generation )
    {
        this.generation = generation;
    }

    long[][] getBitsets()
    {
        return bitsets;
    }

    void normalize()
    {
        for ( int i = 0; i < numOfLongs; i++ )
        {
            // Set the reuse bits to whatever the commit bits are. This will let USED be USED and DELETED will become FREE
            bitsets[BITSET_REUSE][i] = bitsets[BITSET_COMMIT][i];
            bitsets[BITSET_RESERVED][i] = 0;
        }
    }

    boolean mergeFrom( IdRange other, boolean recoveryMode )
    {
        for ( int i = 0; i < numOfLongs; i++ )
        {
            long commit = other.bitsets[BITSET_COMMIT][i];
            long reuse = other.bitsets[BITSET_REUSE][i];
            long reserved = other.bitsets[BITSET_RESERVED][i];

            if ( !recoveryMode )
            {
                verifyMerge( other, i );
            }
            // else anything goes

            bitsets[BITSET_COMMIT][i] = other.addition
                                      ? bitsets[BITSET_COMMIT][i] | commit
                                      : bitsets[BITSET_COMMIT][i] & ~commit;
            bitsets[BITSET_REUSE][i] = other.addition
                                      ? bitsets[BITSET_REUSE][i] | reuse
                                      : bitsets[BITSET_REUSE][i] & ~reuse;
            bitsets[BITSET_RESERVED][i] = other.addition
                                      ? bitsets[BITSET_RESERVED][i] | reserved
                                      : bitsets[BITSET_RESERVED][i] & ~reserved;
        }

        return true;
    }

    private void verifyMerge( IdRange other, int i )
    {
        long into = bitsets[BITSET_COMMIT][i];
        long from = other.bitsets[BITSET_COMMIT][i];
        if ( other.addition )
        {
            if ( (into & from ) != 0 )
            {
                throw new IllegalStateException( format( "Illegal addition ID state transition longIdx: %d%ninto: %s%nfrom: %s",
                        i, toPaddedBinaryString( into ), toPaddedBinaryString( from ) ) );
            }
        }
        // don't very removal since we can't quite verify transitioning to USED since 0 is the default bit value
    }

    private static String toPaddedBinaryString( long bitset )
    {
        char[] padded = StringUtils.leftPad( toBinaryString( bitset ), Long.SIZE, '0' ).toCharArray();

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
        StringBuilder builder = new StringBuilder();
        appendBitset( builder, bitsets[BITSET_COMMIT], "commit  " );
        appendBitset( builder, bitsets[BITSET_REUSE], "reuse   " );
        appendBitset( builder, bitsets[BITSET_RESERVED], "reserved" );
        builder.append( " gen:" ).append( generation );
        return builder.toString();
    }

    private void appendBitset( StringBuilder builder, long[] bitset, String name )
    {
        for ( int i = 0; i < bitset.length; i++ )
        {
            if ( i > 0 )
            {
                builder.append( " , " );
            }
            builder.append( toPaddedBinaryString( bitset[i] ) );
        }
    }

    public boolean isEmpty()
    {
        for ( long bits : bitsets[BITSET_COMMIT] )
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
