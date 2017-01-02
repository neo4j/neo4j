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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.CHUNKED_FIXED_SIZE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.OFF_HEAP;

@RunWith( Parameterized.class )
public class NumberArrayTest
{
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

    @Parameters
    public static Collection<Object[]> arrays()
    {
        Collection<Object[]> list = new ArrayList<>();
        for ( NumberArrayFactory factory : array( HEAP, OFF_HEAP, AUTO, CHUNKED_FIXED_SIZE ) )
        {
            list.add( line(
                    factory.newIntArray( INDEXES, -1 ),
                    (random) -> random.nextInt( 1_000_000_000 ),
                    (array, index, value) -> array.set( index, (Integer) value ),
                    (array, index) -> array.get( index ) ) );
            list.add( line(
                    factory.newDynamicIntArray( INDEXES / 100, -1 ),
                    (random) -> random.nextInt( 1_000_000_000 ),
                    (array, index, value) -> array.set( index, (Integer) value ),
                    (array, index) -> array.get( index ) ) );

            list.add( line(
                    factory.newLongArray( INDEXES, -1 ),
                    (random) -> random.nextLong( 1_000_000_000 ),
                    (array, index, value) -> array.set( index, (Long) value ),
                    (array, index) -> array.get( index ) ) );
            list.add( line(
                    factory.newDynamicLongArray( INDEXES / 100, -1 ),
                    (random) -> random.nextLong( 1_000_000_000 ),
                    (array, index, value) -> array.set( index, (Long) value ),
                    (array, index) -> array.get( index ) ) );

            list.add( line(
                    factory.newByteArray( INDEXES, new byte[] {-1, -1, -1, -1, -1} ),
                    (random) -> random.nextInt( 1_000_000_000 ),
                    (array, index, value) -> array.setInt( index, 1, (Integer) value ),
                    (array, index) -> array.getInt( index, 1 ) ) );
            list.add( line(
                    factory.newDynamicByteArray( INDEXES / 100, new byte[] {-1, -1, -1, -1, -1} ),
                    (random) -> random.nextInt( 1_000_000_000 ),
                    (array, index, value) -> array.setInt( index, 1, (Integer) value ),
                    (array, index) -> array.getInt( index, 1 ) ) );
        }
        return list;
    }

    private static <N extends NumberArray<N>> Object[] line( N array, Function<RandomRule,Object> valueGenerator,
            Writer<N> writer, Reader<N> reader )
    {
        return new Object[] {array, valueGenerator, writer, reader};
    }

    @Rule
    public RandomRule random = new RandomRule();

    @Parameter( 0 )
    public NumberArray<?> array;
    @Parameter( 1 )
    public Function<RandomRule,Object> valueGenerator;
    @SuppressWarnings( "rawtypes" )
    @Parameter( 2 )
    public Writer writer;
    @SuppressWarnings( "rawtypes" )
    @Parameter( 3 )
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
            Object expectedValue = key.containsKey( index ) ? key.get( index ) : defaultValue;
            assertEquals( expectedValue, value );
        }
    }

    @After
    public void after()
    {
        array.close();
    }
}
