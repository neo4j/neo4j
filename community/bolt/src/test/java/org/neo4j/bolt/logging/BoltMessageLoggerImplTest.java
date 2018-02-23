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
package org.neo4j.bolt.logging;

import io.netty.channel.Channel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.util.Attribute;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import java.net.InetSocketAddress;

import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.test.extension.MockitoExtension;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.logging.BoltMessageLoggerImpl.CORRELATION_ATTRIBUTE_KEY;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;

@ExtendWith( MockitoExtension.class )
class BoltMessageLoggerImplTest
{

    private static final String REMOTE_ADDRESS = "127.0.0.1:60297";
    private static final String CORRELATION_ID = "Bolt-CorrelationId-1234";
    private static String errorMessage = "Oh my woes!";
    private static Neo4jError error = Neo4jError.from( new DeadlockDetectedException( errorMessage ) );

    @Mock
    private Channel channel;
    @Mock
    private BoltMessageLog boltMessageLog;
    @Mock
    private Attribute<String> correlationIdAttribute;

    private BoltMessageLoggerImpl boltMessageLogger;

    @BeforeEach
    void setUp()
    {
        when( channel.remoteAddress() ).thenReturn( new InetSocketAddress( "localhost", 60297 ) );
        when( correlationIdAttribute.get() ).thenReturn( CORRELATION_ID );
        when( channel.attr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( correlationIdAttribute );
        boltMessageLogger = new BoltMessageLoggerImpl( boltMessageLog, channel );
    }

    @Test
    void clientEventWithDetails()
    {
        // when
        boltMessageLogger.clientEvent( "TEST", () -> "details" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C TEST details" );
    }

    @Test
    void getRemoteAddressAsIsIncaseOfNonSocketChannel()
    {
        // given
        Channel channel = mock( Channel.class );
        when( channel.attr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( correlationIdAttribute );
        when( correlationIdAttribute.get() ).thenReturn( CORRELATION_ID );
        when( channel.remoteAddress() ).thenReturn( new DomainSocketAddress( "socketPath" ) );
        // when
        BoltMessageLoggerImpl boltMessageLogger = new BoltMessageLoggerImpl( boltMessageLog, channel );
        boltMessageLogger.clientEvent( "TEST", () -> "details" );
        // then
        verify( boltMessageLog ).info( "socketPath", CORRELATION_ID, "C TEST details" );
    }

    @Test
    void serverEvent()
    {
        // when
        boltMessageLogger.serverEvent( "TEST" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S TEST -" );
    }

    @Test
    void serverEventWithDetails()
    {
        // when
        boltMessageLogger.serverEvent( "TEST", () -> "details" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S TEST details" );
    }

    @Test
    void logAckFailure()
    {
        // when
        boltMessageLogger.logAckFailure();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C ACK_FAILURE -" );

    }

    @Test
    void logInit()
    {
        // when
        boltMessageLogger.logInit( "userAgent" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C INIT userAgent" );

    }

    @Test
    void logRun()
    {
        // when
        boltMessageLogger.logRun();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C RUN -" );
    }

    @Test
    void logPullAll()
    {
        // when
        boltMessageLogger.logPullAll();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C PULL_ALL -" );
    }

    @Test
    void logDiscardAll()
    {
        // when
        boltMessageLogger.logDiscardAll();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C DISCARD_ALL -" );
    }

    @Test
    void logReset()
    {
        // when
        boltMessageLogger.logReset();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C RESET -" );
    }

    @Test
    void logSuccess()
    {
        // when
        boltMessageLogger.logSuccess( () -> asMapValue( map( "key", "value"  ) ) );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S SUCCESS {key: \"value\"}" );
    }

    @Test
    void logIgnored()
    {
        // when
        boltMessageLogger.logIgnored();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S IGNORED -" );
    }

    @Test
    void logFailure()
    {
        // when
        boltMessageLogger.logFailure( error.status() );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID,
                "S FAILURE Neo.TransientError.Transaction.DeadlockDetected" );

    }

    @Test
    void logCorrelationIdClientEvent()
    {
        // when
        boltMessageLogger.clientEvent( "TEST" );

        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C TEST -" );
    }

    @Test
    void logCorrelationIdClientErrorWithDetails()
    {
        // given
        when( correlationIdAttribute.get() ).thenReturn( CORRELATION_ID );
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( true );

        // when
        boltMessageLogger.clientError( "TEST", "errorMessage", () -> "details" );

        // then
        verify( boltMessageLog ).error( REMOTE_ADDRESS, CORRELATION_ID, "C TEST details", "errorMessage" );
    }

    @Test
    void logCorrelationIdServerError()
    {
        // when
        boltMessageLogger.serverError( "TEST", "errorMessage" );

        // then
        verify( boltMessageLog ).error( REMOTE_ADDRESS, CORRELATION_ID, "S TEST", "errorMessage" );
    }

    @Test
    void logServerErrorWithStatus()
    {
        // when
        boltMessageLogger.serverError( "TEST", error.status() );

        // then
        verify( boltMessageLog ).error( REMOTE_ADDRESS, CORRELATION_ID, "S TEST",
                "Neo.TransientError.Transaction.DeadlockDetected" );
    }

    @Test
    void logCorrelationIdClientEventWithDetails()
    {
        // given
        when( correlationIdAttribute.get() ).thenReturn( CORRELATION_ID );
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( true );
        when( channel.attr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( correlationIdAttribute );

        // when
        boltMessageLogger.clientEvent( "TEST", () -> "details" );

        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C TEST details" );
    }

    @Test
    void createCorrelationIdIfNotAvailableInInfoLogger()
    {
        // given
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( false );
        BoltMessageLoggerImpl boltMessageLogger = new BoltMessageLoggerImpl( boltMessageLog, channel );

        // when
        boltMessageLogger.clientEvent( "TEST" );

        // then
        verify( correlationIdAttribute ).set( anyString() );
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C TEST -" );
    }

    @Test
    void createCorrelationIdIfNotAvailableInErrorLogger()
    {
        // given
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( false );
        BoltMessageLoggerImpl boltMessageLogger = new BoltMessageLoggerImpl( boltMessageLog, channel );

        // when
        boltMessageLogger.serverError( "TEST", "errorMessage" );

        // then
        verify( correlationIdAttribute ).set( anyString() );
        verify( boltMessageLog ).error( REMOTE_ADDRESS, CORRELATION_ID, "S TEST", "errorMessage" );
    }
}
