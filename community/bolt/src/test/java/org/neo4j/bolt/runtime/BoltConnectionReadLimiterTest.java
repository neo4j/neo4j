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
package org.neo4j.bolt.runtime;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BoltConnectionReadLimiterTest
{
    private static final Job job = s -> s.run( "INIT", null, null );
    private BoltConnection connection;
    private EmbeddedChannel channel;
    private Log log;

    @Before
    public void setup()
    {
        channel = new EmbeddedChannel();
        log = mock( Log.class );

        connection = mock( BoltConnection.class );
        when( connection.id() ).thenReturn( channel.id().asLongText() );
        when( connection.channel() ).thenReturn( channel );
    }

    @After
    public void cleanup()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldNotDisableAutoReadBelowHighWatermark()
    {
        BoltConnectionReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( connection, job );

        assertTrue( channel.config().isAutoRead() );
        verify( log, never() ).warn( anyString(), any(), any() );
    }

    @Test
    public void shouldDisableAutoReadWhenAtHighWatermark()
    {
        BoltConnectionReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );

        assertFalse( channel.config().isAutoRead() );
        verify( log ).warn( contains( "disabled" ), eq( channel.remoteAddress() ), eq( 3 ) );
    }

    @Test
    public void shouldDisableAutoReadOnlyOnceWhenAboveHighWatermark()
    {
        BoltConnectionReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );

        assertFalse( channel.config().isAutoRead() );
        verify( log, times( 1 ) ).warn( contains( "disabled" ), eq( channel.remoteAddress() ), eq( 3 ) );
    }

    @Test
    public void shouldEnableAutoReadWhenAtLowWatermark()
    {
        BoltConnectionReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.drained( connection, Arrays.asList( job, job  ) );

        assertTrue( channel.config().isAutoRead() );
        verify( log, times( 1 ) ).warn( contains( "disabled" ), eq( channel.remoteAddress() ), eq( 3 ) );
        verify( log, times( 1 ) ).warn( contains( "enabled" ), eq( channel.remoteAddress() ), eq( 1 ) );
    }

    @Test
    public void shouldEnableAutoReadOnlyOnceWhenBelowLowWatermark()
    {
        BoltConnectionReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.drained( connection, Arrays.asList( job, job, job ) );

        assertTrue( channel.config().isAutoRead() );
        verify( log, times( 1 ) ).warn( contains( "disabled" ), eq( channel.remoteAddress() ), eq( 3 ) );
        verify( log, times( 1 ) ).warn( contains( "enabled" ), eq( channel.remoteAddress() ), eq( 1 ) );
    }

    @Test
    public void shouldDisableAndEnableAutoRead()
    {
        int lowWatermark = 3;
        int highWatermark = 5;
        BoltConnectionReadLimiter limiter = newLimiter( lowWatermark, highWatermark );

        assertTrue( channel.config().isAutoRead() );

        for ( int i = 0; i < highWatermark + 1; i++ )
        {
            limiter.enqueued( connection, job );
        }
        assertFalse( channel.config().isAutoRead() );

        limiter.drained( connection, singleton( job ) );
        assertFalse( channel.config().isAutoRead() );
        limiter.drained( connection, singleton( job ) );
        assertFalse( channel.config().isAutoRead() );

        limiter.drained( connection, singleton( job ) );
        assertTrue( channel.config().isAutoRead() );

        for ( int i = 0; i < 3; i++ )
        {
            limiter.enqueued( connection, job );
        }
        assertFalse( channel.config().isAutoRead() );

        limiter.drained( connection, Arrays.asList( job, job, job, job, job, job ) );
        assertTrue( channel.config().isAutoRead() );
    }

    @Test
    public void shouldNotAcceptNegativeLowWatermark()
    {
        try
        {
            newLimiter( -1, 5 );
            fail( "exception expected" );
        }
        catch ( IllegalArgumentException exc )
        {
            assertThat( exc.getMessage(), startsWith( "invalid lowWatermark value" )  );
        }
    }

    @Test
    public void shouldNotAcceptLowWatermarkEqualToHighWatermark()
    {
        try
        {
            newLimiter( 5, 5 );
            fail( "exception expected" );
        }
        catch ( IllegalArgumentException exc )
        {
            assertThat( exc.getMessage(), startsWith( "invalid lowWatermark value" )  );
        }
    }

    @Test
    public void shouldNotAcceptLowWatermarkLargerThanHighWatermark()
    {
        try
        {
            newLimiter( 6, 5 );
            fail( "exception expected" );
        }
        catch ( IllegalArgumentException exc )
        {
            assertThat( exc.getMessage(), startsWith( "invalid lowWatermark value" )  );
        }
    }

    @Test
    public void shouldNotAcceptZeroHighWatermark()
    {
        try
        {
            newLimiter( 1, 0 );
            fail( "exception expected" );
        }
        catch ( IllegalArgumentException exc )
        {
            assertThat( exc.getMessage(), startsWith( "invalid highWatermark value" )  );
        }
    }

    @Test
    public void shouldNotAcceptNegativeHighWatermark()
    {
        try
        {
            newLimiter( 1, -1 );
            fail( "exception expected" );
        }
        catch ( IllegalArgumentException exc )
        {
            assertThat( exc.getMessage(), startsWith( "invalid highWatermark value" )  );
        }
    }

    private BoltConnectionReadLimiter newLimiter( int low, int high )
    {
        LogService logService = mock( LogService.class );
        when( logService.getInternalLog( BoltConnectionReadLimiter.class ) ).thenReturn( log );
        return new BoltConnectionReadLimiter( logService, low, high );
    }
}
