/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.helpers.collection;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LruCacheTest
{
    @Test
    void shouldThrowWhenMaxSizeIsNotGreaterThanZero()
    {
        assertThrows( IllegalArgumentException.class, () -> new LruCache<>( "TestCache", 0 ) );
    }

    @Test
    void shouldThrowWhenPuttingEntryWithNullKey()
    {
        assertThrows( IllegalArgumentException.class, () ->
                new LruCache<>( "TestCache", 70 ).put( null, new Object() ) );
    }

    @Test
    void shouldThrowWhenPuttingEntryWithNullValue()
    {
        assertThrows( IllegalArgumentException.class, () ->
                new LruCache<>( "TestCache", 70 ).put( new Object(), null ) );
    }

    @Test
    void shouldThrowWhenGettingWithANullKey()
    {
        assertThrows( IllegalArgumentException.class, () ->
                new LruCache<>( "TestCache", 70 ).get( null ) );
    }

    @Test
    void shouldThrowWhenRemovingWithANullKey()
    {
        assertThrows( IllegalArgumentException.class, () ->
                new LruCache<>( "TestCache", 70 ).remove( null ) );
    }

    @Test
    void shouldWork()
    {
        LruCache<Integer, String> cache = new LruCache<>( "TestCache", 3 );

        String s1 = "1";
        Integer key1 = 1;
        String s2 = "2";
        Integer key2 = 2;
        String s3 = "3";
        Integer key3 = 3;
        String s4 = "4";
        Integer key4 = 4;
        String s5 = "5";
        Integer key5 = 5;

        cache.put( key1, s1 );
        cache.put( key2, s2 );
        cache.put( key3, s3 );
        cache.get( key2 );

        assertEquals( new HashSet<>(  Arrays.asList(key1, key2, key3) ), cache.keySet());

        cache.put( key4, s4 );

        assertEquals( new HashSet<>(  Arrays.asList(key2, key3, key4) ), cache.keySet());

        cache.put( key5, s5 );

        assertEquals( new HashSet<>(  Arrays.asList(key2, key4, key5) ), cache.keySet());

        int size = cache.size();

        assertEquals( 3, size );
        assertNull( cache.get( key1 ) );
        assertEquals( s2, cache.get( key2 ) );
        assertNull( cache.get( key3 ) );
        assertEquals( s4, cache.get( key4 ) );
        assertEquals( s5, cache.get( key5 ) );

        cache.clear();

        assertEquals( 0, cache.size() );
    }

    @Test
    void shouldResizeTheCache()
    {
        final Set<String> cleaned = new HashSet<>();
        LruCache<Integer, String> cache = new LruCache<Integer, String>( "TestCache", 3 )
        {
            @Override
            public void elementCleaned( String element )
            {
                cleaned.add( element );
            }
        };

        String s1 = "1";
        Integer key1 = 1;
        String s2 = "2";
        Integer key2 = 2;
        String s3 = "3";
        Integer key3 = 3;
        String s4 = "4";
        Integer key4 = 4;
        String s5 = "5";
        Integer key5 = 5;

        cache.put( key1, s1 );
        cache.put( key2, s2 );
        cache.put( key3, s3 );
        cache.get( key2 );

        assertEquals( set( key1, key2, key3 ), cache.keySet() );
        assertEquals( cache.maxSize(), cache.size() );

        cache.resize( 5 );

        assertEquals( 5, cache.maxSize() );
        assertEquals( 3, cache.size() );
        assertTrue( cleaned.isEmpty() );

        cache.put( key4, s4 );

        assertEquals( set( key1, key2, key3, key4 ), cache.keySet() );

        cache.put( key5, s5 );

        assertEquals( set( key1, key2, key3, key4, key5 ), cache.keySet() );
        assertEquals( cache.maxSize(), cache.size() );

        cache.resize( 4 );

        assertEquals( set( key2, key3, key4, key5 ), cache.keySet() );
        assertEquals( cache.maxSize(), cache.size() );
        assertEquals( set( s1 ), cleaned );

        cleaned.clear();

        cache.resize( 3 );

        assertEquals( set( key2, key4, key5 ), cache.keySet() );
        assertEquals( 3, cache.maxSize() );
        assertEquals( 3, cache.size() );
        assertEquals( set( s3 ), cleaned );
    }

    @Test
    void shouldClear()
    {
        final Set<String> cleaned = new HashSet<>();
        LruCache<Integer, String> cache = new LruCache<Integer, String>( "TestCache", 3 )
        {
            @Override
            public void elementCleaned( String element )
            {
                cleaned.add( element );
            }
        };

        String s1 = "1";
        Integer key1 = 1;
        String s2 = "2";
        Integer key2 = 2;
        String s3 = "3";
        Integer key3 = 3;
        String s4 = "4";
        Integer key4 = 4;
        String s5 = "5";
        Integer key5 = 5;

        cache.put( key1, s1 );
        cache.put( key2, s2 );
        cache.put( key3, s3 );
        cache.get( key2 );

        assertEquals( set( key1, key2, key3 ), cache.keySet() );
        assertEquals( cache.maxSize(), cache.size() );

        cache.resize( 5 );

        assertEquals( 5, cache.maxSize() );
        assertEquals( 3, cache.size() );

        cache.put( key4, s4 );

        assertEquals( set( key1, key2, key3, key4 ), cache.keySet() );

        cache.put( key5, s5 );

        assertEquals( set( key1, key2, key3, key4, key5 ), cache.keySet() );
        assertEquals( cache.maxSize(), cache.size() );

        cache.clear( );

        assertEquals( 0, cache.size() );
        assertEquals( set( s1, s2, s3, s4, s5 ), cleaned );
    }

    private static <E> Set<E> set( E... elems )
    {
        return new HashSet<>( Arrays.asList( elems ) );
    }
}
