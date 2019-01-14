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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.neo4j.io.pagecache.PageCache;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.OFF_HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.auto;

@RunWith( Parameterized.class )
public class LongArrayTest extends NumberArrayPageCacheTestSupport
{
    private static Fixture fixture;

    @Test
    public void shouldHandleSomeRandomSetAndGet()
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
                int toIndex = random.nextInt( length );
                array.swap( index, toIndex );
                swap( expected, index, toIndex );
                break;
            }
        }
    }

    @Test
    public void shouldHandleMultipleCallsToClose()
    {
        // GIVEN
        NumberArray<?> array = newArray( 10, -1 );

        // WHEN
        array.close();

        // THEN should also work
        array.close();
    }

    private void swap( long[] expected, int fromIndex, int toIndex )
    {
        long fromValue = expected[fromIndex];
        expected[fromIndex] = expected[toIndex];
        expected[toIndex] = fromValue;
    }

    @Parameters
    public static Collection<NumberArrayFactory> data() throws IOException
    {
        fixture = prepareDirectoryAndPageCache( LongArrayTest.class );
        PageCache pageCache = fixture.pageCache;
        File dir = fixture.directory;
        NumberArrayFactory autoWithPageCacheFallback = auto( pageCache, dir, true, NO_MONITOR );
        NumberArrayFactory pageCacheArrayFactory = new PageCachedNumberArrayFactory( pageCache, dir );
        return Arrays.asList( HEAP, OFF_HEAP, autoWithPageCacheFallback, pageCacheArrayFactory );
    }

    @AfterClass
    public static void closeFixture() throws Exception
    {
        fixture.close();
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
