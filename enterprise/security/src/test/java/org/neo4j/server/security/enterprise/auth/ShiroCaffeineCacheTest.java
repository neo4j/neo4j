/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
