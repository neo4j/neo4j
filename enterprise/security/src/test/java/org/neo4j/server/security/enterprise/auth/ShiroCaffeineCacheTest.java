/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth;

import com.google.common.testing.FakeTicker;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ShiroCaffeineCacheTest
{
    private ShiroCaffeineCache<Integer,String> cache;
    private FakeTicker fakeTicker;
    private long TTL = 100;

    @Before
    public void setUp()
    {
        fakeTicker = new FakeTicker();
        cache = new ShiroCaffeineCache<>( fakeTicker::read, Runnable::run, TTL, 5, true );
    }

    @Test
    public void shouldFailToCreateAuthCacheForTTLZeroIfUsingTLL()
    {
        new ShiroCaffeineCache<>( fakeTicker::read, Runnable::run, 0, 5, false );
        try
        {
            new ShiroCaffeineCache<>( fakeTicker::read, Runnable::run, 0, 5, true );
            fail("Expected IllegalArgumentException for a TTL of 0");
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), containsString( "TTL must be larger than zero." ) );
        }
        catch ( Throwable t )
        {
            fail("Expected IllegalArgumentException for a TTL of 0");
        }
    }

    @Test
    public void shouldNotGetNonExistentValue()
    {
        assertThat( cache.get( 1 ), equalTo( null ) );
    }

    @Test
    public void shouldPutAndGet()
    {
        cache.put( 1, "1" );
        assertThat( cache.get( 1 ), equalTo( "1" ) );
    }

    @Test
    public void shouldNotReturnExpiredValueThroughPut()
    {
        assertNull( cache.put( 1, "first" ));
        assertThat( cache.put( 1, "second" ), equalTo( "first" ) );
        fakeTicker.advance( TTL + 1, MILLISECONDS );
        assertNull( cache.put( 1, "third" ) );
    }

    @Test
    public void shouldRemove()
    {
        assertNull( cache.remove( 1 ) );
        cache.put( 1, "1" );
        assertThat( cache.remove( 1 ), equalTo( "1" ) );
    }

    @Test
    public void shouldClear()
    {
        cache.put( 1, "1" );
        cache.put( 2, "2" );
        assertThat( cache.size(), equalTo( 2 ) );
        cache.clear();
        assertThat( cache.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldGetKeys()
    {
        cache.put( 1, "1" );
        cache.put( 2, "1" );
        cache.put( 3, "1" );
        assertThat( cache.keys(), containsInAnyOrder( 1, 2, 3 ) );
    }

    @Test
    public void shouldGetValues()
    {
        cache.put( 1, "1" );
        cache.put( 2, "1" );
        cache.put( 3, "1" );
        assertThat( cache.values(), containsInAnyOrder( "1", "1", "1" ) );
    }

    @Test
    public void shouldNotListExpiredValues()
    {
        cache.put( 1, "1" );
        fakeTicker.advance( TTL + 1, MILLISECONDS );
        cache.put( 2, "foo" );

        assertThat( cache.values(), containsInAnyOrder( "foo" ) );
    }

    @Test
    public void shouldNotGetExpiredValues()
    {
        cache.put( 1, "1" );
        fakeTicker.advance( TTL + 1, MILLISECONDS );
        cache.put( 2, "foo" );

        assertThat( cache.get( 1 ), equalTo( null ) );
        assertThat( cache.get( 2 ), equalTo( "foo" ) );
    }

    @Test
    public void shouldNotGetKeysForExpiredValues()
    {
        cache.put( 1, "1" );
        fakeTicker.advance( TTL + 1, MILLISECONDS );
        cache.put( 2, "foo" );

        assertThat( cache.keys(), containsInAnyOrder( 2 ) );
    }

    @Test
    public void shouldRemoveIfExceededCapacity()
    {
        cache.put( 1, "one" );
        cache.put( 2, "two" );
        cache.put( 3, "three" );
        cache.put( 4, "four" );
        cache.put( 5, "five" );
        cache.put( 6, "six" );

        assertThat( cache.size(), equalTo( 5 ) );
    }

    @Test
    public void shouldGetValueAfterTimePassed()
    {
        cache.put( 1, "foo" );
        fakeTicker.advance( TTL - 1, MILLISECONDS );
        assertThat( cache.get( 1 ), equalTo( "foo" ) );
    }
}
