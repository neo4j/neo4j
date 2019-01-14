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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import org.neo4j.io.pagecache.PageCache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO_WITHOUT_PAGECACHE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.OFF_HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.auto;

@RunWith( Parameterized.class )
public class ByteArrayTest extends NumberArrayPageCacheTestSupport
{
    private static final byte[] DEFAULT = new byte[50];
    private static final int LENGTH = 1_000;
    private static Fixture fixture;

    @Parameters
    public static Collection<Supplier<ByteArray>> data() throws IOException
    {
        fixture = prepareDirectoryAndPageCache( ByteArrayTest.class );
        PageCache pageCache = fixture.pageCache;
        File dir = fixture.directory;
        NumberArrayFactory autoWithPageCacheFallback = auto( pageCache, dir, true, NO_MONITOR );
        NumberArrayFactory pageCacheArrayFactory = new PageCachedNumberArrayFactory( pageCache, dir );
        int chunkSize = LENGTH / ChunkedNumberArrayFactory.MAGIC_CHUNK_COUNT;
        return Arrays.asList(
                () -> HEAP.newByteArray( LENGTH, DEFAULT ),
                () -> HEAP.newDynamicByteArray( chunkSize, DEFAULT ),
                () -> OFF_HEAP.newByteArray( LENGTH, DEFAULT ),
                () -> OFF_HEAP.newDynamicByteArray( chunkSize, DEFAULT ),
                () -> AUTO_WITHOUT_PAGECACHE.newByteArray( LENGTH, DEFAULT ),
                () -> AUTO_WITHOUT_PAGECACHE.newDynamicByteArray( chunkSize, DEFAULT ),
                () -> autoWithPageCacheFallback.newByteArray( LENGTH, DEFAULT ),
                () -> autoWithPageCacheFallback.newDynamicByteArray( chunkSize, DEFAULT ),
                () -> pageCacheArrayFactory.newByteArray( LENGTH, DEFAULT ),
                () -> pageCacheArrayFactory.newDynamicByteArray( chunkSize, DEFAULT )
        );
    }

    @AfterClass
    public static void closeFixture() throws Exception
    {
        fixture.close();
    }

    @Parameter
    public Supplier<ByteArray> factory;
    private ByteArray array;

    @Before
    public void before()
    {
        array = factory.get();
    }

    @After
    public void after()
    {
        array.close();
    }

    @Test
    public void shouldSetAndGetBasicTypes()
    {
        int index = 0;
        byte[] actualBytes = new byte[DEFAULT.length];
        byte[] expectedBytes = new byte[actualBytes.length];
        ThreadLocalRandom.current().nextBytes( actualBytes );

        int len = LENGTH - 1; // subtract one because we access TWO elements.
        for ( int i = 0; i < len; i++ )
        {
            try
            {
                // WHEN
                setSimpleValues( index );
                setArray( index + 1, actualBytes );

                // THEN
                verifySimpleValues( index );
                verifyArray( index + 1, actualBytes, expectedBytes );
            }
            catch ( Throwable throwable )
            {
                throw new AssertionError( "Failure at index " + i, throwable );
            }
        }
    }

    private void setSimpleValues( int index )
    {
        array.setByte( index, 0, (byte) 123 );
        array.setShort( index, 1, (short) 1234 );
        array.setInt( index, 5, 12345 );
        array.setLong( index, 9, Long.MAX_VALUE - 100 );
        array.set3ByteInt( index, 17, 0b10101010_10101010_10101010 );
        array.set5ByteLong( index, 20, 0b10101010_10101010_10101010_10101010_10101010L );
        array.set6ByteLong( index, 25, 0b10101010_10101010_10101010_10101010_10101010_10101010L );
    }

    private void verifySimpleValues( int index )
    {
        assertEquals( (byte) 123, array.getByte( index, 0 ) );
        assertEquals( (short) 1234, array.getShort( index, 1 ) );
        assertEquals( 12345, array.getInt( index, 5 ) );
        assertEquals( Long.MAX_VALUE - 100, array.getLong( index, 9 ) );
        assertEquals( 0b10101010_10101010_10101010, array.get3ByteInt( index, 17 ) );
        assertEquals( 0b10101010_10101010_10101010_10101010_10101010L, array.get5ByteLong( index, 20 ) );
        assertEquals( 0b10101010_10101010_10101010_10101010_10101010_10101010L, array.get6ByteLong( index, 25 ) );
    }

    private void setArray( int index, byte[] bytes )
    {
        array.set( index, bytes );
    }

    private void verifyArray( int index, byte[] actualBytes, byte[] scratchBuffer )
    {
        array.get( index, scratchBuffer );
        assertArrayEquals( actualBytes, scratchBuffer );
    }

    @Test
    public void shouldDetectMinusOneFor3ByteInts()
    {
        // WHEN
        array.set3ByteInt( 10, 2, -1 );
        array.set3ByteInt( 10, 5, -1 );

        // THEN
        assertEquals( -1L, array.get3ByteInt( 10, 2 ) );
        assertEquals( -1L, array.get3ByteInt( 10, 5 ) );
    }

    @Test
    public void shouldDetectMinusOneFor5ByteLongs()
    {
        // WHEN
        array.set5ByteLong( 10, 2, -1 );
        array.set5ByteLong( 10, 7, -1 );

        // THEN
        assertEquals( -1L, array.get5ByteLong( 10, 2 ) );
        assertEquals( -1L, array.get5ByteLong( 10, 7 ) );
    }

    @Test
    public void shouldDetectMinusOneFor6ByteLongs()
    {
        // WHEN
        array.set6ByteLong( 10, 2, -1 );
        array.set6ByteLong( 10, 8, -1 );

        // THEN
        assertEquals( -1L, array.get6ByteLong( 10, 2 ) );
        assertEquals( -1L, array.get6ByteLong( 10, 8 ) );
    }

    @Test
    public void shouldHandleMultipleCallsToClose()
    {
        // WHEN
        array.close();

        // THEN should also work
        array.close();
    }
}
