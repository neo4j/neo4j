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
package org.neo4j.kernel.impl.util;

import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

public class VersionedHashMapTest
{

    @Test
    public void shouldGetAndContain() throws Exception
    {
        // Given
        VersionedHashMap<Object, Object> map = new VersionedHashMap<>();

        // When
        map.put( 22, true );

        // Then
        assertThat(map.containsKey( 22 ), equalTo(true));
        assertThat(map.containsKey( 21 ), equalTo(false));
        assertThat(map.containsKey( 23 ), equalTo(false));

        assertThat(map.get(22), equalTo((Object)true));
        assertThat(map.containsValue( true ), equalTo(true));
        assertThat(map.containsValue( false ), equalTo(false));
    }

    @Test
    public void shouldReplace() throws Exception
    {
        // Given
        VersionedHashMap<Object, Object> map = new VersionedHashMap<>();
        map.put( 22, true );

        // When
        map.put( 22, false );

        // Then
        assertThat(map.containsKey( 22 ), equalTo(true));
        assertThat(map.containsKey( 21 ), equalTo(false));
        assertThat(map.containsKey( 23 ), equalTo(false));

        assertThat(map.get(22), equalTo((Object)false));
        assertThat(map.containsValue( false ), equalTo(true));
        assertThat(map.containsValue( true ), equalTo(false));
    }

    @Test
    public void shouldRemove() throws Exception
    {
        // Given
        VersionedHashMap<Object, Object> map = new VersionedHashMap<>();
        map.put( 22, true );

        // When
        map.remove( 22 );

        // Then
        assertThat(map.containsKey( 22 ), equalTo(false));
        assertThat(map.containsKey( 21 ), equalTo(false));
        assertThat(map.containsKey( 23 ), equalTo(false));

        assertThat(map.get(22), equalTo(null));
        assertThat(map.containsValue( false ), equalTo(false));
        assertThat(map.containsValue( true ), equalTo(false));
    }

    @Test
    public void shouldNotSeeAdditionsWhileIterating() throws Exception
    {
        // Given
        VersionedHashMap<Object, Object> map = new VersionedHashMap<>();
        for ( int i = 0; i < 10; i++ )
        {
            map.put( 16 + i, true );
        }

        // When
        boolean added = false;
        int count = 0;
        for ( Object k : map.keySet() )
        {
            if(!added)
            {
                added = true;
                for ( int i = 0; i < 10; i++ )
                {
                    map.put( i, true );
                }
            }

            count++;
        }

        // Then
        assertThat(count, equalTo(10));
        assertThat(map.size(), equalTo(20));
    }

    @Test
    public void shouldSeeRemovalsWhileIterating() throws Exception
    {
        // Given
        VersionedHashMap<Object, Object> map = new VersionedHashMap<>();
        for ( int i = 0; i < 10; i++ )
        {
            map.put( i, true );
        }

        // When
        int count = 0;
        for ( Object k : map.keySet() )
        {
            // Remove all objects in the map.
            for ( Object o : map.keySet() )
            {
                map.remove(o);
            }

            count++;
        }

        // Then
        assertThat(count, equalTo(1));
        assertThat(map.size(), equalTo(0));
    }

    @Test
    public void shouldAllowRemovalsWhileIterating() throws Exception
    {
        // Given
        VersionedHashMap<Object, Object> map = new VersionedHashMap<>();
        for ( int i = 0; i < 10; i++ )
        {
            map.put( i, true );
        }

        // When
        int count = 0;
        Iterator<Map.Entry<Object,Object>> it = map.entrySet().iterator();
        while(it.hasNext())
        {
            it.next();
            it.remove();
            count++;
        }

        // Then
        assertThat( count, equalTo( 10 ) );
        assertThat(map.size(), equalTo(0));
    }

    @Test
    public void shouldHandleResizing() throws Exception
    {
        // Given
        VersionedHashMap<Object, Object> map = new VersionedHashMap<>( 16, 0.5f );

        // When
        for ( int i = 0; i < 128; i++ )
        {
            map.put( i, i );
        }

        // Then
        assertThat( map.size(), equalTo(128) );

        int count = 0;
        for ( int i = 0; i < 128; i++ )
        {
            assertThat(map.get(i), equalTo((Object)i));
            count++;
        }
        assertThat(count, equalTo(128));
    }

    @Test
    public void shouldAllowRemovalsWhileIteratingEvenInFaceOfResizing() throws Exception
    {
        // Given
        VersionedHashMap<Object, Object> map = new VersionedHashMap<>(16, 5.0f);
        for ( int i = 0; i < 128; i++ )
        {
            map.put( i, true );
        }

        // When
        int count = 0;
        for ( Object k : map.keySet() )
        {
            // Add some entries, these should not be visible
            for ( int i = 128; i < 256; i++ )
            {
                map.put( i, true );
            }

            // Remove 0 -> 127
            for ( int i = 0; i < 128; i++ )
            {
                map.remove( i );
            }

            count++;
        }

        // Then
        assertThat(count, equalTo(1));
        assertThat(map.size(), equalTo(128));
    }
}
