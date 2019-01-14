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
package org.neo4j.concurrent;

import java.util.Arrays;

import org.neo4j.string.HexString;

/**
 * This is a concurrent data structure used to track
 * some set if boolean flags. Any time a flag is set to high,
 * it'll remain high for a period of time, after which
 * it'll fall back to low again. How long the flags stay high
 * depend on how often they are toggled - meaning this uses
 * both recency and frequency to determine which flags to keep
 * high.
 *
 * The more often a flag is toggled high, the longer it'll
 * take before it resets to low - if a flag gets set more
 * often than sweep is called, it will always be high.
 *
 * This data structure is coordination free, but sacrifices
 * accuracy for performance.
 *
 * Intended usage is that you'd have a set of keys you care
 * about, and set a max time where you'd like to mark a key
 * as low if that time passes; say 7 days.
 *
 * So, you'd set {@link #keepalive} to 7, and then you'd
 * schedule a thread to call {@link #sweep()} once per day.
 * Now, flags that were toggled once will be set to low again
 * the next day, while flags that were extensively used will
 * take up to seven days before falling back to low.
 *
 * Flags that are toggled at or more than once every day will
 * always be high.
 */
public class DecayingFlags
{
    /**
     * A flag in the set, with a unique index pointing
     * to the bit that correlates to this flag.
     */
    public static class Key
    {
        private final int index;

        public Key( int index )
        {
            this.index = index;
        }

        public int index()
        {
            return index;
        }
    }

    /**
     * To model the time-based "decay" of the flags,
     * each flag is represented as an int that counts
     * the number of times the flag has been toggled, up to
     * {@link #keepalive}. Each time #sweep is called, all
     * flags are decremented by 1, once a flag reaches 0 it
     * is considered low.
     *
     * This way, if a flag is not "renewed", the flag will
     * eventually fall back to low.
     */
    private int[] flags;

    /**
     * The max number of sweep iteration a flag is kept alive
     * if it is not toggled. The more toggles seen in a flag,
     * the more likely it is to hit this threshold.
     */
    private final int keepalive;

    /**
     * @param keepalive controls the maximum length of time
     *                     a flag will stay toggled if it is not
     *                     renewed, expressed as the number of times
     *                     {@link #sweep()} needs to be called.
     */
    public DecayingFlags( int keepalive )
    {
        this.keepalive = keepalive;
        this.flags = new int[16];
    }

    public void flag( Key key )
    {
        // We dynamically size this up as needed
        if ( key.index >= flags.length )
        {
            resize( key.index );
        }

        int flag = flags[key.index];
        if ( flag < keepalive )
        {
            flags[key.index] = flag + 1;
        }
    }

    /**
     * This is how decay happens, the interval at which
     * this method is called controls how long unused
     * flags are kept 'high'. Each invocation of this will
     * decrement the flag counters by 1, marking any that
     * reach 0 as low.
     */
    public void sweep()
    {
        for ( int i = 0; i < flags.length; i++ )
        {
            int count = flags[i];
            if ( count > 0 )
            {
                flags[i] = count - 1;
            }
        }
    }

    private synchronized void resize( int minSize )
    {
        int newSize = flags.length;
        while ( newSize < minSize )
        {
            newSize += 16;
        }

        if ( flags.length < newSize )
        {
            flags = Arrays.copyOf( flags, newSize );
        }
    }

    public String asHex()
    {
        // Convert the flags to a byte-array, each
        // flag represented as a single bit.
        byte[] bits = new byte[flags.length / 8];

        // Go over the flags, eight at a time to align
        // with sticking eight bits at a time into the
        // output array.
        for ( int i = 0; i < flags.length; i += 8 )
        {
            bits[i / 8] = (byte)(
                (bit( i ) << 7) |
                (bit( i + 1 ) << 6) |
                (bit( i + 2 ) << 5) |
                (bit( i + 3 ) << 4) |
                (bit( i + 4 ) << 3) |
                (bit( i + 5 ) << 2) |
                (bit( i + 6 ) << 1) |
                (bit( i + 7 )) ) ;
        }
        return HexString.encodeHexString( bits );
    }

    private int bit( int idx )
    {
        return flags[idx] > 0 ? 1 : 0;
    }
}
