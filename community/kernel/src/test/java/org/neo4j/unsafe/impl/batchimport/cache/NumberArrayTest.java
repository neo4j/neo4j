/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO_WITHOUT_PAGECACHE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.CHUNKED_FIXED_SIZE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;
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
        factories.put( "autoWithPageCacheFallback", auto( pageCache, dir, true ) );
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
                    factory.newDynamicIntArray( INDEXES / 100, -1 ),
                    random -> random.nextInt( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Integer) value ), IntArray::get ) );

            list.add( line(
                    name + "LongArray",
                    factory.newLongArray( INDEXES, -1 ),
                    random -> random.nextLong( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Long) value ), LongArray::get ) );
            list.add( line(
                    name + "DynamicLongArray",
                    factory.newDynamicLongArray( INDEXES / 100, -1 ),
                    random -> random.nextLong( 1_000_000_000 ),
                    ( array, index, value ) -> array.set( index, (Long) value ), LongArray::get ) );

            list.add( line(
                    name + "ByteArray",
                    factory.newByteArray( INDEXES, new byte[] {-1, -1, -1, -1, -1} ),
                    random -> random.nextInt( 1_000_000_000 ),
                    ( array, index, value ) -> array.setInt( index, 1, (Integer) value ),
                    ( array, index ) -> array.getInt( index, 1 ) ) );
            list.add( line(
                    name + "DynamicByteArray",
                    factory.newDynamicByteArray( INDEXES / 100, new byte[] {-1, -1, -1, -1, -1} ),
                    random -> random.nextInt( 1_000_000_000 ),
                    ( array, index, value ) -> array.setInt( index, 1, (Integer) value ),
                    ( array, index ) -> array.getInt( index, 1 ) ) );
        }
        return list;
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
    public void shouldGetAndSetRandomItems() throws Exception
    {
        // GIVEN
        Map<Integer,Object> key = new HashMap<>();
        Object defaultValue = reader.read( array, 0 );

        // WHEN
        for ( int i = 0; i < 100_000; i++ )
        {
            int index = random.nextInt( INDEXES );
            Object value = valueGenerator.apply( random );
            writer.write( i % 2 == 0 ? array : array.at( index ), index, value );
            key.put( index, value );
        }

        // THEN
        for ( int index = 0; index < INDEXES; index++ )
        {
            Object value = reader.read( index % 2 == 0 ? array : array.at( index ), index );
            Object expectedValue = key.getOrDefault( index, defaultValue );
            assertEquals( expectedValue, value );
        }
    }

    @After
    public void after()
    {
        array.close();
    }
}
