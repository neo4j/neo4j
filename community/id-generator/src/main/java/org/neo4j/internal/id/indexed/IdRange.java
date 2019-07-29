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
 * ID states are kept in one or more {@code long}s where each long holds two 32-bits bit-sets, one for commit bits and one for reuse bits.
 * The combination of commit/reuse bits makes up the state of an ID, like this:
 *
 * <pre>
 *     <--------------- REUSE BITS ---------------> <--------------- COMMIT BITS -------------->
 *  MSB[    ,    ][    ,    ][    ,    ][   x,    ] [    ,    ][    ,    ][    ,    ][   x,    ]LSB
 *                                          ▲                                            ▲
 *                                          │                                            │
 *                                          └───── BITS THAT MAKE UP ID AT OFFSET 4 ─────┘
 * </pre>
 *
 * Each {@link IdRange} is associated with an {@link IdRangeKey} which specifies the range, e.g. an ID range of 3 in a layout where ids-per-entry is 128
 * holds IDs between 384-511.
 *
 * These are the various states that an ID can have:
 * <pre>
 *
 * </pre>
 *
 * <table border="1">
 *     <tr>
 *         <th>REUSE</th>
 *         <th>COMMIT</th>
 *         <th>STATE</th>
 *     </tr>
 *     <tr>
 *         <td>0</td>
 *         <td>0</td>
 *         <td>USED</td>
 *     </tr>
 *     <tr>
 *         <td>0</td>
 *         <td>1</td>
 *         <td>DELETED</td>
 *     </tr>
 *     <tr>
 *         <td>1</td>
 *         <td>1</td>
 *         <td>FREE</td>
 *     </tr>
 *     <tr>
 *         <td>1</td>
 *         <td>0</td>
 *         <td>-</td>
 *     </tr>
 * </table>
 *
 * <pre>
 *     R: recovery
 *     S: single instance
 *     C: clustering
 *
 *     USED    -> USED        R
 *     USED    -> DELETED     RSC
 *     USED    -> FREE        C
 *     DELETED -> USED        RC
 *     DELETED -> DELETED     R
 *     DELETED -> FREE        SC
 *     FREE    -> USED        RC
 *     FREE    -> DELETED     RSC
 *     FREE    -> FREE
 * </pre>
 */
class IdRange
{
    /**
     * Each {@code long} contains two bit-sets, one for commit bits and one for reuse bits
     */
    private static final int BITSET_SIZE = Long.SIZE / 2;
    private static final long COMMIT_BITS_MASK = 0xFFFFFFFFL;

    private long generation;
    private transient boolean addition;
    private final long[] longs;

    IdRange( int numOfLongs )
    {
        this.longs = new long[numOfLongs];
    }

    IdState getState( int n )
    {
        int longIndex = n / BITSET_SIZE;
        int bitIndex = n % BITSET_SIZE;
        long bits = longs[longIndex];
        boolean commitBit = (bits & commitBitMask( bitIndex )) != 0;
        if ( commitBit )
        {
            boolean reuseBit = (bits & reuseBitMask( bitIndex )) != 0;
            return reuseBit ? IdState.FREE : IdState.DELETED;
        }
        return IdState.USED;
    }

    private long commitBitMask( int bitIndex )
    {
        return 1L << bitIndex;
    }

    private long reuseBitMask( int bitIndex )
    {
        return 1L << (bitIndex + BITSET_SIZE);
    }

    void setCommitBit( int n )
    {
        int longIndex = n / BITSET_SIZE;
        int bitIndex = n % BITSET_SIZE;
        longs[longIndex] |= commitBitMask( bitIndex );
    }

    void setReuseBit( int n )
    {
        int longIndex = n / BITSET_SIZE;
        int bitIndex = n % BITSET_SIZE;
        longs[longIndex] |= reuseBitMask( bitIndex );
    }

    void setCommitAndReuseBit( int n )
    {
        int longIndex = n / BITSET_SIZE;
        int bitIndex = n % BITSET_SIZE;
        longs[longIndex] |= commitBitMask( bitIndex ) | reuseBitMask( bitIndex );
    }

    void clear( long generation, boolean addition )
    {
        this.generation = generation;
        this.addition = addition;
        fill( longs, 0 );
    }

    long getGeneration()
    {
        return generation;
    }

    void setGeneration( long generation )
    {
        this.generation = generation;
    }

    long[] getLongs()
    {
        return longs;
    }

    void normalize()
    {
        for ( int i = 0; i < longs.length; i++ )
        {
            // Set the reuse bits to whatever the commit bits are. This will let USED be USED and DELETED will become FREE
            long commitBits = longs[i] & COMMIT_BITS_MASK;
            longs[i] = commitBits | (commitBits << BITSET_SIZE);
        }
    }

    boolean mergeFrom( IdRange other, boolean recoveryMode )
    {
        for ( int i = 0; i < longs.length; i++ )
        {
            long from = other.longs[i];
            if ( from == 0 )
            {
                continue;
            }

            if ( !recoveryMode )
            {
                verifyMerge( other, i );
            }
            // else anything goes

            longs[i] = other.addition
                       ? longs[i] | from
                       : longs[i] & ~from;
        }

        return true;
    }

    private void verifyMerge( IdRange other, int i )
    {
        long into = longs[i];
        long from = other.longs[i];
        long commitBits = from & COMMIT_BITS_MASK;
        if ( other.addition )
        {
            if ( (longs[i] & commitBits ) != 0 )
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

    public boolean isEmpty()
    {
        for ( long bits : longs )
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
