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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Integer.max;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO_WITHOUT_PAGECACHE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.CHUNKED_FIXED_SIZE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.OFF_HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.auto;

class NumberArrayTest extends NumberArrayPageCacheTestSupport
{
    private static final RandomRule random = new RandomRule();
    private static final int INDEXES = 50_000;
    private static final int CHUNK_SIZE = max( 1, INDEXES / 100 );
    private static Fixture fixture;

    @BeforeAll
    static void setUp() throws IOException
    {
        fixture = prepareDirectoryAndPageCache( NumberArrayTest.class );
        random.reset();
    }

    @AfterAll
    static void tearDown() throws Exception
    {
        fixture.close();
    }

    @TestFactory
    Stream<DynamicTest> shouldGetAndSetRandomItems()
    {
        ThrowingConsumer<NumberArrayTestData> throwingConsumer = data ->
        {
            try ( NumberArray array = data.array )
            {
                Map<Integer,Object> key = new HashMap<>();
                Reader reader = data.reader;
                Object defaultValue = reader.read( array, 0 );

                // WHEN setting random items
                for ( int i = 0; i < INDEXES * 2; i++ )
                {
                    int index = random.nextInt( INDEXES );
                    Object value = data.valueGenerator.apply( random );
                    data.writer.write( i % 2 == 0 ? array : array.at( index ), index, value );
                    key.put( index, value );
                }

                // THEN they should be read correctly
                assertAllValues( key, defaultValue, reader, array );

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
                assertAllValues( key, defaultValue, reader, array );
            }
        };
        return DynamicTest.stream( arrays().iterator(), data -> data.name, throwingConsumer );
    }

    public static Collection<NumberArrayTestData> arrays()
    {
        PageCache pageCache = fixture.pageCache;
        File dir = fixture.directory;
        Collection<NumberArrayTestData> list = new ArrayList<>();
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
            list.add( arrayData( name + "IntArray", factory.newIntArray( INDEXES, -1 ), random -> random.nextInt( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Integer) value ), IntArray::get ) );
            list.add( arrayData( name + "DynamicIntArray", factory.newDynamicIntArray( CHUNK_SIZE, -1 ), random -> random.nextInt( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Integer) value ), IntArray::get ) );

            list.add( arrayData( name + "LongArray", factory.newLongArray( INDEXES, -1 ), random -> random.nextLong( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Long) value ), LongArray::get ) );
            list.add( arrayData( name + "DynamicLongArray", factory.newDynamicLongArray( CHUNK_SIZE, -1 ), random -> random.nextLong( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Long) value ), LongArray::get ) );

            list.add( arrayData( name + "ByteArray5", factory.newByteArray( INDEXES, defaultByteArray( 5 ) ), random -> random.nextInt( 1_000_000_000 ),
                    ( array, index, value ) -> array.setInt( index, 1, (Integer) value ), ( array, index ) -> array.getInt( index, 1 ) ) );
            list.add( arrayData( name + "DynamicByteArray5", factory.newDynamicByteArray( CHUNK_SIZE, defaultByteArray( 5 ) ),
                    random -> random.nextInt( 1_000_000_000 ), ( array, index, value ) -> array.setInt( index, 1, (Integer) value ),
                    ( array, index ) -> array.getInt( index, 1 ) ) );

            Function<RandomRule,Object> valueGenerator =
                    random -> new long[]{random.nextLong(), random.nextInt(), (short) random.nextInt(), (byte) random.nextInt()};
            Writer<ByteArray> writer = ( array, index, value ) ->
            {
                long[] values = (long[]) value;
                array.setLong( index, 0, values[0] );
                array.setInt( index, 8, (int) values[1] );
                array.setShort( index, 12, (short) values[2] );
                array.setByte( index, 14, (byte) values[3] );
            };
            Reader<ByteArray> reader = ( array, index ) -> new long[]{array.getLong( index, 0 ), array.getInt( index, 8 ), array.getShort( index, 12 ),
                    array.getByte( index, 14 )};
            list.add( arrayData( name + "ByteArray15", factory.newByteArray( INDEXES, defaultByteArray( 15 ) ), valueGenerator, writer, reader ) );
            list.add( arrayData( name + "DynamicByteArray15", factory.newDynamicByteArray( CHUNK_SIZE, defaultByteArray( 15 ) ), valueGenerator, writer,
                    reader ) );
        }
        return list;
    }

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

    private static class NumberArrayTestData<T extends NumberArray<T>>
    {
        private String name;
        private T array;
        private Function<RandomRule,Object> valueGenerator;
        private Writer<T> writer;
        private Reader<T> reader;

        NumberArrayTestData( String name, T array, Function<RandomRule,Object> valueGenerator, Writer<T> writer, Reader<T> reader )
        {
            this.name = name;
            this.array = array;
            this.valueGenerator = valueGenerator;
            this.writer = writer;
            this.reader = reader;
        }
    }

    private static byte[] defaultByteArray( int length )
    {
        byte[] result = new byte[length];
        Arrays.fill( result, (byte) -1 );
        return result;
    }

    private static <N extends NumberArray<N>> NumberArrayTestData arrayData( String name, N array, Function<RandomRule,Object> valueGenerator, Writer<N> writer,
            Reader<N> reader )
    {
        return new NumberArrayTestData( name, array, valueGenerator, writer, reader );
    }

    private static void assertAllValues( Map<Integer,Object> key, Object defaultValue, Reader reader, NumberArray array )
    {
        for ( int index = 0; index < INDEXES; index++ )
        {
            Object value = reader.read( index % 2 == 0 ? array : array.at( index ), index );
            Object expectedValue = key.getOrDefault( index, defaultValue );
            if ( value instanceof long[] )
            {
                assertArrayEquals( (long[]) expectedValue, (long[]) value, "index " + index );
            }
            else
            {
                assertEquals( expectedValue, value, "index " + index );
            }
        }
    }
}
