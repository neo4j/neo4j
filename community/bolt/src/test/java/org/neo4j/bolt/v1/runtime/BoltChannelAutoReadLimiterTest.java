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
package org.neo4j.bolt.v1.runtime;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.logging.Log;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

import static org.hamcrest.CoreMatchers.is;
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

public class BoltChannelAutoReadLimiterTest
{
    private static final Job job = s -> s.run( "INIT", null, null );
    private Channel channel;
    private Log log;

    @Before
    public void setup()
    {
        this.channel = new EmbeddedChannel();
        this.log = mock( Log.class );
    }

    @Test
    public void shouldUseWatermarksFromSystemProperties()
    {
        FeatureToggles.set( BoltChannelAutoReadLimiter.class, BoltChannelAutoReadLimiter.LOW_WATERMARK_NAME, 5 );
        FeatureToggles.set( BoltChannelAutoReadLimiter.class, BoltChannelAutoReadLimiter.HIGH_WATERMARK_NAME, 10 );

        try
        {
           BoltChannelAutoReadLimiter limiter = newLimiterWithDefaults();

           assertThat( limiter.getLowWatermark(), is( 5 ) );
           assertThat( limiter.getHighWatermark(), is( 10 ) );
        }
        finally
        {
            FeatureToggles.clear( BoltChannelAutoReadLimiter.class, BoltChannelAutoReadLimiter.LOW_WATERMARK_NAME );
            FeatureToggles.clear( BoltChannelAutoReadLimiter.class, BoltChannelAutoReadLimiter.HIGH_WATERMARK_NAME );
        }
    }

    @Test
    public void shouldNotDisableAutoReadBelowHighWatermark()
    {
        BoltChannelAutoReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( job );

        assertTrue( channel.config().isAutoRead() );
        verify( log, never() ).warn( anyString(), any(), any() );
    }

    @Test
    public void shouldDisableAutoReadWhenAtHighWatermark()
    {
        BoltChannelAutoReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( job );
        limiter.enqueued( job );
        limiter.enqueued( job );

        assertFalse( channel.config().isAutoRead() );
        verify( log ).warn( contains( "disabled" ), eq( channel.id() ), eq( 3 ) );
    }

    @Test
    public void shouldDisableAutoReadOnlyOnceWhenAboveHighWatermark()
    {
        BoltChannelAutoReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( job );
        limiter.enqueued( job );
        limiter.enqueued( job );
        limiter.enqueued( job );
        limiter.enqueued( job );

        assertFalse( channel.config().isAutoRead() );
        verify( log, times( 1 ) ).warn( contains( "disabled" ), eq( channel.id() ), eq( 3 ) );
    }

    @Test
    public void shouldEnableAutoReadWhenAtLowWatermark()
    {
        BoltChannelAutoReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( job );
        limiter.enqueued( job );
        limiter.enqueued( job );
        limiter.dequeued( job );
        limiter.dequeued( job );

        assertTrue( channel.config().isAutoRead() );
        verify( log, times( 1 ) ).warn( contains( "disabled" ), eq( channel.id() ), eq( 3 ) );
        verify( log, times( 1 ) ).warn( contains( "enabled" ), eq( channel.id() ), eq( 1 ) );
    }

    @Test
    public void shouldEnableAutoReadOnlyOnceWhenBelowLowWatermark()
    {
        BoltChannelAutoReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( job );
        limiter.enqueued( job );
        limiter.enqueued( job );
        limiter.dequeued( job );
        limiter.dequeued( job );
        limiter.dequeued( job );

        assertTrue( channel.config().isAutoRead() );
        verify( log, times( 1 ) ).warn( contains( "disabled" ), eq( channel.id() ), eq( 3 ) );
        verify( log, times( 1 ) ).warn( contains( "enabled" ), eq( channel.id() ), eq( 1 ) );
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

    private BoltChannelAutoReadLimiter newLimiter( int low, int high )
    {
        return new BoltChannelAutoReadLimiter( channel, log, low, high );
    }

    private BoltChannelAutoReadLimiter newLimiterWithDefaults()
    {
        return new BoltChannelAutoReadLimiter( channel, log );
    }

}
