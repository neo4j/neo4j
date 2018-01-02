/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static java.lang.System.currentTimeMillis;

import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class LongArrayTest
{
    @Test
    public void shouldHandleSomeRandomSetAndGet() throws Exception
    {
        // GIVEN
        int length = random.nextInt( 100_000 ) + 100;
        long defaultValue = random.nextInt( 2 ) - 1; // 0 or -1
        LongArray array = newArray( length, defaultValue );
        long[] expected = new long[length];
        Arrays.fill( expected, defaultValue );

        // WHEN
        int operations = random.nextInt( 1_000 ) + 10;
        for ( int i = 0; i < operations; i++ )
        {
            // THEN
            int index = random.nextInt( length );
            long value = random.nextLong();
            switch ( random.nextInt( 3 ) )
            {
            case 0: // set
                array.set( index, value );
                expected[index] = value;
                break;
            case 1: // get
                assertEquals( "Seed:" + seed, expected[index], array.get( index ) );
                break;
            default: // swap
                int items = Math.min( random.nextInt( 10 )+1, length-index );
                int toIndex = (index + length/2) % (length-items);
                array.swap( index, toIndex, items );
                swap( expected, index, toIndex, items );
                break;
            }
        }
    }

    @Test
    public void shouldHandleMultipleCallsToClose() throws Exception
    {
        // GIVEN
        LongArray array = newArray( 10, -1 );

        // WHEN
        array.close();

        // THEN should also work
        array.close();
    }

    private void swap( long[] expected, int fromIndex, int toIndex, int items )
    {
        for ( int i = 0; i < items; i++ )
        {
            long fromValue = expected[fromIndex+i];
            expected[fromIndex+i] = expected[toIndex+i];
            expected[toIndex+i] = fromValue;
        }
    }

    @Parameters
    public static Collection<Object[]> data()
    {
        return Arrays.asList(
                new Object[] {NumberArrayFactory.HEAP},
                new Object[] {NumberArrayFactory.OFF_HEAP}
                );
    }

    public LongArrayTest( NumberArrayFactory factory )
    {
        this.factory = factory;
    }

    private LongArray newArray( int length, long defaultValue )
    {
        return array = factory.newLongArray( length, defaultValue );
    }

    private final NumberArrayFactory factory;
    private final long seed = currentTimeMillis();
    private final Random random = new Random( seed );
    private LongArray array;

    @After
    public void after()
    {
        array.close();
    }
}
