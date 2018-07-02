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

import java.util.Arrays;

import static java.lang.Long.toBinaryString;
import static java.lang.String.format;
import static java.util.Arrays.fill;

/**
 * A small bit-set of dibits representing ID states in a specific ID range.
 */
class IdRange
{
    private static final int DIBITS_PER_LONG = Long.SIZE / 2;
    private static final long R_MASK = 0b01010101_01010101_01010101_01010101_01010101_01010101_01010101_01010101L;
    private static final long L_MASK = 0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010L;

    private long generation;
    private final long[] octlets;

    IdRange( int numOfOctlets )
    {
        this.octlets = new long[numOfOctlets];
    }

    IdState getState( int n )
    {
        final int octletIdx = getOctletIdx( n );
        final int pos = (n % DIBITS_PER_LONG) << 1;
        final int dibit = (int) (octlets[octletIdx] >> pos) & 0b11;
        return IdState.valueOf( dibit );
    }

    void setState( int n, IdState state )
    {
        final int octletIdx = getOctletIdx( n );
        final int dibitPos = (n % DIBITS_PER_LONG) << 1;
        final long mask = ~(0b11L << dibitPos);
        octlets[octletIdx] = (octlets[octletIdx] & mask) | (state.code << dibitPos);
    }

    int size()
    {
        return DIBITS_PER_LONG * octlets.length;
    }

    void clear( long generation )
    {
        this.generation = generation;
        fill( octlets, 0 );
    }

    long getGeneration()
    {
        return generation;
    }

    void setGeneration( long generation )
    {
        this.generation = generation;
    }

    long[] getOctlets()
    {
        return octlets;
    }

    void normalize()
    {
        // Example:
        // x = 00 10 01 11 -> 00 10 10 10
        //        │   │ └ reserved
        //        │   └ deleted
        //        └─ free
        // 1. filter deleted bit
        // d = x & R_MASK -> 00 00 01 01
        // 2. shift left to copy deleted bits to free bit (d << 1)
        // f = d << 1 -> 00 00 10 10
        // 3. reset deleted bits
        // x = x ^ d -> 00 10 10 10
        // 4. set free bits from deleted bits
        // x = x | f -> 00 10 10 10
        for ( int i = 0; i < octlets.length; i++ )
        {
            final var octlet = octlets[i];
            final var d = octlet & R_MASK;
            octlets[i] = (octlet ^ d) | (d << 1);
        }
    }

    boolean mergeFrom( IdRange other, boolean recoveryMode )
    {
        final StateTransitionVerifier verifier = recoveryMode ? StateTransitionVerifier.NOP : IdRange::verifyTransitions;
        long dirty = 0;
        for ( int i = 0; i < octlets.length; i++ )
        {
            final long into = octlets[i];
            final long from = other.octlets[i];
            verifier.verify( into, from, i );

            //              from (CD)
            //          00   01   11   10
            //        ╔════╦════╦════╦════╗
            //     00 ║ 00 ║ 01 ║ 00 ║ 10 ║
            //   i    ╠════╬════╬════╬════╣
            //   n 01 ║ 01 ║ 01*║ 00*║ 10 ║
            //   t    ╠════╬════╬════╬════╣
            //   o 11 ║ 11 ║ 01*║ 00 ║ 10 ║
            // (AB)   ╠════╬════╬════╬════╣
            //     10 ║ 10 ║ 01*║ 11 ║ 10*║
            //        ╚════╩════╩════╩════╝
            // NOTES:
            //  1) unused combinations must be caught by verification and fail the merge
            //  2) * combinations are only valid during recovery and will fail during normal operation
            //
            // X = (A & ~B & C) | (A & ~D) | (C & ~D)
            // Y = (A & ~B & D) | (B & ~C) | (~C & D)

            final long a = (into & L_MASK) >>> 1;
            final long b = into & R_MASK;
            final long bi = ~into & R_MASK;
            final long c = (from & L_MASK) >>> 1;
            final long ci = (~from & L_MASK) >>> 1;
            final long d = from & R_MASK;
            final long di = ~from & R_MASK;

            final long x = (a & bi & c) | (a & di) | (c & di);
            final long y = (a & bi & d) | (b & ci) | (ci & d);

            final long result = (x << 1) | y;
            dirty |= octlets[i] ^ result;
            octlets[i] = result;
        }
        return dirty != 0;
    }

    private static void verifyTransitions( long before, long after, int octletIndex )
    {
        // Truth table for error detection,
        // where 1 == forbidden transition
        //
        //             after (CD)
        //          00  01  11  10
        //         ╔═══╦═══╦═══╦═══╗
        //  b   00 ║ 0 ║ 0 ║ 0*║ 0*║
        //  e      ╠═══╬═══╬═══╬═══╣
        //  f   01 ║ 0 ║ 1 ║ 1 ║ 0 ║
        //  o      ╠═══╬═══╬═══╬═══╣
        //  r   11 ║ 0 ║ 1 ║ 0 ║ 0 ║
        //  e      ╠═══╬═══╬═══╬═══╣
        // (AB) 10 ║ 0 ║ 1 ║ 0 ║ 1 ║
        //         ╚═══╩═══╩═══╩═══╝
        // NOTES:
        // 00+11 is totally ignored in merge since it happens when ID is initially used and should be marked as USED,
        //  but obviously its state in the range is not RESERVED(11) yet, but USED(00).
        // 00+10 is possible when ID generated from HighID is freed as a result of tx rollback, so it won't be RESERVED in the tree
        //
        // Using Karnaugh Map we get following function:
        // E = (A & ~B & C & ~D) | (A & ~C & D) | (~A & B & D)

        final long a = (before & L_MASK) >>> 1;
        final long ai = (~before & L_MASK) >>> 1;
        final long b = before & R_MASK;
        final long bi = ~before & R_MASK;
        final long c = (after & L_MASK) >>> 1;
        final long ci = (~after & L_MASK) >>> 1;
        final long d = after & R_MASK;
        final long di = ~after & R_MASK;

        final long error = (a & bi & c & di) | (a & ci & d) | (ai & b & d);

        if ( error != 0 )
        {
            throw new IllegalStateException(  format( "Illegal ID state transition octletIdx: %d%nbefore: %s%nafter:  %s",
                    octletIndex, toPaddedBinaryString( before ), toPaddedBinaryString( after ) ) );
        }
    }

    private static String toPaddedBinaryString( long octlet )
    {
        char[] unpadded = toBinaryString( octlet ).toCharArray();
        char[] padded = new char[Long.SIZE];
        Arrays.fill( padded, '0' );
        System.arraycopy( unpadded, 0, padded, padded.length - unpadded.length, unpadded.length );
        int numberOfSpaces = padded.length / 2 - 1;
        char[] spaced = new char[padded.length + numberOfSpaces];
        Arrays.fill( spaced, ' ' );
        for ( int i = 0; i < numberOfSpaces + 1; i++ )
        {
            System.arraycopy( padded, i * 2, spaced, i * 3, 2 );
        }
        return String.valueOf( spaced );
    }

    private int getOctletIdx( int n )
    {
        return n / DIBITS_PER_LONG;
    }

    public boolean isEmpty()
    {
        for ( long octlet : octlets )
        {
            if ( octlet != 0 )
            {
                return false;
            }
        }
        return true;
    }

    private interface StateTransitionVerifier
    {
        StateTransitionVerifier NOP = ( a, b, c ) ->
        {
        };
        void verify( long into, long from, int octletIndex );
    }

    enum IdState
    {
        USED( 0b00 ),
        DELETED( 0b01 ),
        FREE( 0b10 ),
        RESERVED( 0b11 );

        final long code;

        IdState( int code )
        {
            this.code = code;
        }

        static IdState valueOf( int code )
        {
            if ( code == FREE.code )
            {
                return FREE;
            }
            else if ( code == DELETED.code )
            {
                return DELETED;
            }
            else if ( code == USED.code )
            {
                return USED;
            }
            else if ( code == RESERVED.code )
            {
                return RESERVED;
            }
            else
            {
                throw new IllegalArgumentException( "Illegal state code dibit: " + code );
            }
        }
    }
}
