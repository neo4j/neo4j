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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( RandomExtension.class )
class ConcurrentSparseLongBitSetTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldSetSomeBits()
    {
        // given
        ConcurrentSparseLongBitSet set = new ConcurrentSparseLongBitSet( 128 );
        BitSet key = new BitSet( 128 );

        // when
        set( set, key, 5, 6, true );
        set( set, key, 62, 4, true );
        set( set, key, 70, 7, true );
        long[] snapshot = new long[2];
        set.snapshotRange( 0, snapshot );

        // then
        for ( int i = 0; i < 128; i++ )
        {
            int arrayIndex = i / Long.SIZE;
            int offset = i % Long.SIZE;
            assertEquals( key.get( i ), (snapshot[arrayIndex] & (1L << offset)) != 0 );
        }
    }

    @Test
    void shouldSetRemoveSet()
    {
        // given
        ConcurrentSparseLongBitSet set = new ConcurrentSparseLongBitSet( 128 );
        assertTrue( set.set( 0, 8, true ) );
        assertTrue( set.set( 0, 8, false ) );

        // when
        boolean reset = set.set( 0, 8, true );

        // then
        assertTrue( reset );
    }

    @Test
    void shouldSetNonConflictingBitsConcurrently() throws Throwable
    {
        // given
        ConcurrentSparseLongBitSet set = new ConcurrentSparseLongBitSet( 128 );
        Race race = new Race().withMaxDuration( 10, SECONDS );
        int numberOfThreads = 10;
        int idsPerChunk = 1 << random.nextInt( 4 );
        for ( int i = 0; i < numberOfThreads; i++ )
        {
            race.addContestant( setter( set, idsPerChunk, i, numberOfThreads, random.nextLong() ), 1_000_000 );
        }

        // when
        race.go();

        // then we're good, all assertions were made while running
    }

    @Test
    void shouldSetConflictingBitsConcurrently() throws Throwable
    {
        // given
        ConcurrentSparseLongBitSet set = new ConcurrentSparseLongBitSet( 128 );
        Race race = new Race().withMaxDuration( 10, SECONDS );
        AtomicBoolean isSet = new AtomicBoolean();
        race.addContestants( 10, () ->
        {
            boolean wasSet = set.set( 3, 10, true );
            if ( wasSet )
            {
                assertTrue( isSet.compareAndSet( false, true ) );
            }
        }, 1 );

        // when
        race.go();

        // then
        assertTrue( isSet.get() );
    }

    @Test
    void shouldRemoveEmptyRanges()
    {
        // given
        ConcurrentSparseLongBitSet set = new ConcurrentSparseLongBitSet( 128 );
        set.set( 5, 2, true );
        set.set( 7, 2, true );
        assertEquals( 1, set.size() );

        // when
        set.set( 5, 4, false );

        // then
        assertEquals( 0, set.size() );

        // and when
        set.set( 9, 5, true );
        assertEquals( 1, set.size() );
    }

    private Runnable setter( ConcurrentSparseLongBitSet set, int idsPerChunk, int i, int numberOfThreads, long seed )
    {
        return new Runnable()
        {
            private final BitSet key = new BitSet();
            private final long[] reader = new long[2];
            private final long[] temp = new long[2];
            private final Random random = new Random( seed );

            @Override
            public void run()
            {
                int chunk = random.nextInt( 1024 );
                int id = (chunk * numberOfThreads + i) * idsPerChunk;
                boolean isSet = key.get( chunk );

                // read
                set.snapshotRange( id / set.getIdsPerEntry(), reader );
                Arrays.fill( temp, 0 );
                BitsUtil.setBits( temp, id % set.getIdsPerEntry(), idsPerChunk, 0 );
                assertTrue( bitsMatches( reader, temp, isSet ) );

                // write
                boolean actuallySet = set.set( id, idsPerChunk, !isSet );
                assertTrue( actuallySet );
                key.set( chunk, !isSet );
            }
        };
    }

    private void set( ConcurrentSparseLongBitSet set, BitSet key, long id, int slots, boolean value )
    {
        boolean actuallySet = set.set( id, slots, value );
        assertTrue( actuallySet );
        for ( int i = 0; i < slots; i++ )
        {
            key.set( (int) (id + i), value );
        }
    }

    static boolean bitsMatches( long[] bits, long[] mask, boolean value )
    {
        assert bits.length == mask.length;
        if ( value )
        {
            for ( int i = 0; i < bits.length; i++ )
            {
                if ( (bits[i] & mask[i]) != mask[i] )
                {
                    return false;
                }
            }
        }
        else
        {
            for ( int i = 0; i < bits.length; i++ )
            {
                if ( (bits[i] & mask[i]) != 0 )
                {
                    return false;
                }
            }
        }
        return true;
    }
}
