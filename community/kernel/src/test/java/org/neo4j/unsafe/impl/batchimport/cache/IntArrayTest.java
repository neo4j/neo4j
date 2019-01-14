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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.ThrowingConsumer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.pagecache.PageCache;

import static java.lang.System.currentTimeMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.stream;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.OFF_HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.auto;

class IntArrayTest extends NumberArrayPageCacheTestSupport
{
    private static Fixture fixture;
    private final long seed = currentTimeMillis();
    private final Random random = new Random( seed );

    @BeforeAll
    static void setUp() throws IOException
    {
        fixture = prepareDirectoryAndPageCache( IntArrayTest.class );
    }

    @AfterAll
    static void tearDown() throws Exception
    {
        fixture.close();
    }

    @TestFactory
    Stream<DynamicTest> shouldHandleSomeRandomSetAndGet()
    {
        // GIVEN
        ThrowingConsumer<NumberArrayFactory> arrayFactoryConsumer = factory ->
        {
            int length = random.nextInt( 100_000 ) + 100;
            int defaultValue = random.nextInt( 2 ) - 1; // 0 or -1
            try ( IntArray array = factory.newIntArray( length, defaultValue ) )
            {
                int[] expected = new int[length];
                Arrays.fill( expected, defaultValue );

                // WHEN
                int operations = random.nextInt( 1_000 ) + 10;
                for ( int i = 0; i < operations; i++ )
                {
                    // THEN
                    int index = random.nextInt( length );
                    int value = random.nextInt();
                    switch ( random.nextInt( 3 ) )
                    {
                    case 0: // set
                        array.set( index, value );
                        expected[index] = value;
                        break;
                    case 1: // get
                        assertEquals( expected[index], array.get( index ), "Seed:" + seed );
                        break;
                    default: // swap
                        int toIndex = random.nextInt( length );
                        array.swap( index, toIndex );
                        swap( expected, index, toIndex );
                        break;
                    }
                }
            }
        };
        return stream( arrayFactories(), getNumberArrayFactoryName(), arrayFactoryConsumer );
    }

    @TestFactory
    Stream<DynamicTest> shouldHandleMultipleCallsToClose()
    {
        return DynamicTest.stream( arrayFactories(), getNumberArrayFactoryName(), factory ->
        {
            // GIVEN
            NumberArray<?> array = factory.newIntArray( 10, -1 );

            // WHEN
            array.close();

            // THEN should also work
            array.close();
        } );
    }

    private static Function<NumberArrayFactory,String> getNumberArrayFactoryName()
    {
        return factory -> factory.getClass().getName();
    }

    private static void swap( int[] expected, int fromIndex, int toIndex )
    {
        int fromValue = expected[fromIndex];
        expected[fromIndex] = expected[toIndex];
        expected[toIndex] = fromValue;
    }

    private static Iterator<NumberArrayFactory> arrayFactories()
    {
        PageCache pageCache = fixture.pageCache;
        File dir = fixture.directory;
        NumberArrayFactory autoWithPageCacheFallback = auto( pageCache, dir, true, NO_MONITOR );
        NumberArrayFactory pageCacheArrayFactory = new PageCachedNumberArrayFactory( pageCache, dir );
        return Iterators.iterator( HEAP, OFF_HEAP, autoWithPageCacheFallback, pageCacheArrayFactory );
    }
}
