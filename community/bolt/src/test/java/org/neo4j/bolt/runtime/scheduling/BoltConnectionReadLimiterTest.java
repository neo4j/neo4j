/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.runtime.scheduling;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.Job;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;

class BoltConnectionReadLimiterTest
{
    private static final Job job = machine -> machine.process( new HelloMessage( emptyMap() ), nullResponseHandler() );
    private BoltConnection connection;
    private EmbeddedChannel channel;
    private Log log;

    @BeforeEach
    void setup()
    {
        channel = new EmbeddedChannel();
        log = mock( Log.class );

        connection = mock( BoltConnection.class );
        when( connection.id() ).thenReturn( channel.id().asLongText() );
        when( connection.channel() ).thenReturn( channel );
    }

    @AfterEach
    void cleanup()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldNotDisableAutoReadBelowHighWatermark()
    {
        BoltConnectionReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( connection, job );

        assertTrue( channel.config().isAutoRead() );
        verify( log, never() ).warn( anyString(), any(), any() );
    }

    @Test
    void shouldDisableAutoReadWhenAtHighWatermark()
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
    void shouldDisableAutoReadOnlyOnceWhenAboveHighWatermark()
    {
        BoltConnectionReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );

        assertFalse( channel.config().isAutoRead() );
        verify( log ).warn( contains( "disabled" ), eq( channel.remoteAddress() ), eq( 3 ) );
    }

    @Test
    void shouldEnableAutoReadWhenAtLowWatermark()
    {
        BoltConnectionReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.drained( connection, Arrays.asList( job, job ) );

        assertTrue( channel.config().isAutoRead() );
        verify( log ).warn( contains( "disabled" ), eq( channel.remoteAddress() ), eq( 3 ) );
        verify( log ).warn( contains( "enabled" ), eq( channel.remoteAddress() ), eq( 1 ) );
    }

    @Test
    void shouldEnableAutoReadOnlyOnceWhenBelowLowWatermark()
    {
        BoltConnectionReadLimiter limiter = newLimiter( 1, 2 );

        assertTrue( channel.config().isAutoRead() );

        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.enqueued( connection, job );
        limiter.drained( connection, Arrays.asList( job, job, job ) );

        assertTrue( channel.config().isAutoRead() );
        verify( log ).warn( contains( "disabled" ), eq( channel.remoteAddress() ), eq( 3 ) );
        verify( log ).warn( contains( "enabled" ), eq( channel.remoteAddress() ), eq( 1 ) );
    }

    @Test
    void shouldDisableAndEnableAutoRead()
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
    void shouldNotAcceptNegativeLowWatermark()
    {
        var e = assertThrows( IllegalArgumentException.class, () -> newLimiter( -1, 5 ) );
        assertThat( e.getMessage() ).startsWith( "invalid lowWatermark value" );
    }

    @Test
    void shouldNotAcceptLowWatermarkEqualToHighWatermark()
    {
        var e = assertThrows( IllegalArgumentException.class, () -> newLimiter( 5, 5 ) );
        assertThat( e.getMessage() ).startsWith( "invalid lowWatermark value" );
    }

    @Test
    void shouldNotAcceptLowWatermarkLargerThanHighWatermark()
    {
        var e = assertThrows( IllegalArgumentException.class, () -> newLimiter( 6, 5 ) );
        assertThat( e.getMessage() ).startsWith( "invalid lowWatermark value" );
    }

    @Test
    void shouldNotAcceptZeroHighWatermark()
    {
        var e = assertThrows( IllegalArgumentException.class, () -> newLimiter( 1, 0 ) );
        assertThat( e.getMessage() ).startsWith( "invalid highWatermark value" );
    }

    @Test
    void shouldNotAcceptNegativeHighWatermark()
    {
        var e = assertThrows( IllegalArgumentException.class, () -> newLimiter( 1, -1 ) );
        assertThat( e.getMessage() ).startsWith( "invalid highWatermark value" );
    }

    private BoltConnectionReadLimiter newLimiter( int low, int high )
    {
        LogService logService = mock( LogService.class );
        when( logService.getInternalLog( BoltConnectionReadLimiter.class ) ).thenReturn( log );
        return new BoltConnectionReadLimiter( logService, low, high );
    }
}
