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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Integer.max;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO_WITHOUT_PAGECACHE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.CHUNKED_FIXED_SIZE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.OFF_HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.auto;

@RunWith( Parameterized.class )
public class NumberArrayTest extends NumberArrayPageCacheTestSupport
{
    private static Fixture fixture;

    @FunctionalInterface
    interface Writer<N extends NumberArray<N>>
    {
        void write( N array, int index, Object value );
    }

    @FunctionalInterface
    interface Reader<N extends NumberArray<N>>
    {
        Object read( N array, int index );
    }

    private static final int INDEXES = 50_000;
    private static final int CHUNK_SIZE = max( 1, INDEXES / 100 );

    @Parameters( name = "{0}" )
    public static Collection<Object[]> arrays() throws IOException
    {
        fixture = prepareDirectoryAndPageCache( NumberArrayTest.class );
        PageCache pageCache = fixture.pageCache;
        File dir = fixture.directory;
        Collection<Object[]> list = new ArrayList<>();
        Map<String,NumberArrayFactory> factories = new HashMap<>();
        factories.put( "HEAP", HEAP );
        factories.put( "OFF_HEAP", OFF_HEAP );
        factories.put( "AUTO_WITHOUT_PAGECACHE", AUTO_WITHOUT_PAGECACHE );
        factories.put( "CHUNKED_FIXED_SIZE", CHUNKED_FIXED_SIZE );
        factories.put( "autoWithPageCacheFallback", auto( pageCache, dir, true, NO_MONITOR ) );
        factories.put( "PageCachedNumberArrayFactory", new PageCachedNumberArrayFactory( pageCache, dir ) );
        for ( Map.Entry<String,NumberArrayFactory> entry : factories.entrySet() )
        {
            String name = entry.getKey() + " => ";
            NumberArrayFactory factory = entry.getValue();
            list.add( line(
                    name + "IntArray",
                    factory.newIntArray( INDEXES, -1 ),
                    random -> random.nextInt( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Integer) value ), IntArray::get ) );
            list.add( line(
                    name + "DynamicIntArray",
                    factory.newDynamicIntArray( CHUNK_SIZE, -1 ),
                    random -> random.nextInt( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Integer) value ), IntArray::get ) );

            list.add( line(
                    name + "LongArray",
                    factory.newLongArray( INDEXES, -1 ),
                    random -> random.nextLong( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Long) value ), LongArray::get ) );
            list.add( line(
                    name + "DynamicLongArray",
                    factory.newDynamicLongArray( CHUNK_SIZE, -1 ),
                    random -> random.nextLong( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Long) value ), LongArray::get ) );

            list.add( line(
                    name + "ByteArray5",
                    factory.newByteArray( INDEXES, defaultByteArray( 5 ) ),
                    random -> random.nextInt( 1_000_000_000 ),
                    ( array, index, value ) -> array.setInt( index, 1, (Integer) value ),
                    ( array, index ) -> array.getInt( index, 1 ) ) );
            list.add( line(
                    name + "DynamicByteArray5",
                    factory.newDynamicByteArray( CHUNK_SIZE, defaultByteArray( 5 ) ),
                    random -> random.nextInt( 1_000_000_000 ),
                    ( array, index, value ) -> array.setInt( index, 1, (Integer) value ),
                    ( array, index ) -> array.getInt( index, 1 ) ) );

            Function<RandomRule,Object> valueGenerator = random -> new long[] {
                    random.nextLong(),
                    random.nextInt(),
                    (short) random.nextInt(),
                    (byte) random.nextInt()
            };
            Writer<ByteArray> writer = ( array, index, value ) ->
            {
                long[] values = (long[]) value;
                array.setLong( index, 0, values[0] );
                array.setInt( index, 8, (int) values[1] );
                array.setShort( index, 12, (short) values[2] );
                array.setByte( index, 14, (byte) values[3] );
            };
            Reader<ByteArray> reader = ( array, index ) -> new long[] {
                array.getLong( index, 0 ),
                array.getInt( index, 8 ),
                array.getShort( index, 12 ),
                array.getByte( index, 14 )
            };
            list.add( line(
                    name + "ByteArray15",
                    factory.newByteArray( INDEXES, defaultByteArray( 15 ) ),
                    valueGenerator, writer, reader ) );
            list.add( line(
                    name + "DynamicByteArray15",
                    factory.newDynamicByteArray( CHUNK_SIZE, defaultByteArray( 15 ) ),
                    valueGenerator, writer, reader ) );
        }
        return list;
    }

    private static byte[] defaultByteArray( int length )
    {
        byte[] result = new byte[length];
        Arrays.fill( result, (byte) -1 );
        return result;
    }

    private static <N extends NumberArray<N>> Object[] line( String name, N array, Function<RandomRule,Object> valueGenerator,
            Writer<N> writer, Reader<N> reader )
    {
        return new Object[] {name, array, valueGenerator, writer, reader};
    }

    @AfterClass
    public static void closeFixture() throws Exception
    {
        fixture.close();
    }

    @Rule
    public RandomRule random = new RandomRule();

    @Parameter( 0 )
    public String name;
    @Parameter( 1 )
    public NumberArray<?> array;
    @Parameter( 2 )
    public Function<RandomRule,Object> valueGenerator;
    @SuppressWarnings( "rawtypes" )
    @Parameter( 3 )
    public Writer writer;
    @SuppressWarnings( "rawtypes" )
    @Parameter( 4 )
    public Reader reader;

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldGetAndSetRandomItems()
    {
        // GIVEN
        Map<Integer,Object> key = new HashMap<>();
        Object defaultValue = reader.read( array, 0 );

        // WHEN setting random items
        for ( int i = 0; i < INDEXES * 2; i++ )
        {
            int index = random.nextInt( INDEXES );
            Object value = valueGenerator.apply( random );
            writer.write( i % 2 == 0 ? array : array.at( index ), index, value );
            key.put( index, value );
        }

        // THEN they should be read correctly
        assertAllValues( key, defaultValue );

        // AND WHEN swapping some
        for ( int i = 0; i < INDEXES / 2; i++ )
        {
            int fromIndex = random.nextInt( INDEXES );
            int toIndex;
            do
            {
                toIndex = random.nextInt( INDEXES );
            }
            while ( toIndex == fromIndex );
            Object fromValue = reader.read( array, fromIndex );
            Object toValue = reader.read( array, toIndex );
            key.put( fromIndex, toValue );
            key.put( toIndex, fromValue );
            array.swap( fromIndex, toIndex );
        }

        // THEN they should end up in the correct places
        assertAllValues( key, defaultValue );
    }

    @SuppressWarnings( "unchecked" )
    private void assertAllValues( Map<Integer,Object> key, Object defaultValue )
    {
        for ( int index = 0; index < INDEXES; index++ )
        {
            Object value = reader.read( index % 2 == 0 ? array : array.at( index ), index );
            Object expectedValue = key.getOrDefault( index, defaultValue );
            if ( value instanceof long[] )
            {
                assertArrayEquals( "index " + index, (long[]) expectedValue, (long[]) value );
            }
            else
            {
                assertEquals( "index " + index, expectedValue, value );
            }
        }
    }

    @After
    public void after()
    {
        array.close();
    }
}
