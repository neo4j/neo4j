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
package org.neo4j.bolt.logging;

import io.netty.channel.Channel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.util.Attribute;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;

import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.kernel.DeadlockDetectedException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.logging.BoltMessageLoggerImpl.CORRELATION_ATTRIBUTE_KEY;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;

@RunWith( MockitoJUnitRunner.class )
public class BoltMessageLoggerImplTest
{

    private static final String REMOTE_ADDRESS = "127.0.0.1:60297";
    private static final String CORRELATION_ID = "Bolt-CorrelationId-1234";
    private static String errorMessage = "Oh my woes!";
    private static Neo4jError error = Neo4jError.from( new DeadlockDetectedException( errorMessage ) );

    @Mock
    private Channel channel;
    @Mock
    BoltMessageLog boltMessageLog;
    @Mock
    Attribute<String> correlationIdAttribute;

    BoltMessageLoggerImpl boltMessageLogger;

    @Before
    public void setUp()
    {
        when( channel.remoteAddress() ).thenReturn( new InetSocketAddress( "localhost", 60297 ) );
        when( correlationIdAttribute.get() ).thenReturn( CORRELATION_ID );
        when( channel.attr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( correlationIdAttribute );
        boltMessageLogger = new BoltMessageLoggerImpl( boltMessageLog, channel );
    }

    @Test
    public void clientEventWithDetails()
    {
        // when
        boltMessageLogger.clientEvent( "TEST", () -> "details" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C TEST details" );
    }

    @Test
    public void getRemoteAddressAsIsIncaseOfNonSocketChannel()
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
    public void serverEvent()
    {
        // when
        boltMessageLogger.serverEvent( "TEST" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S TEST -" );
    }

    @Test
    public void serverEventWithDetails()
    {
        // when
        boltMessageLogger.serverEvent( "TEST", () -> "details" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S TEST details" );
    }

    @Test
    public void logAckFailure()
    {
        // when
        boltMessageLogger.logAckFailure();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C ACK_FAILURE -" );

    }

    @Test
    public void logInit()
    {
        // when
        boltMessageLogger.logInit( "userAgent" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C INIT userAgent" );

    }

    @Test
    public void logRun()
    {
        // when
        boltMessageLogger.logRun();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C RUN -" );
    }

    @Test
    public void logPullAll()
    {
        // when
        boltMessageLogger.logPullAll();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C PULL_ALL -" );
    }

    @Test
    public void logDiscardAll()
    {
        // when
        boltMessageLogger.logDiscardAll();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C DISCARD_ALL -" );
    }

    @Test
    public void logReset()
    {
        // when
        boltMessageLogger.logReset();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C RESET -" );
    }

    @Test
    public void logSuccess()
    {
        // when
        boltMessageLogger.logSuccess( () -> asMapValue( map( "key", "value"  ) ) );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S SUCCESS {key: \"value\"}" );
    }

    @Test
    public void logIgnored()
    {
        // when
        boltMessageLogger.logIgnored();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S IGNORED -" );
    }

    @Test
    public void logFailure()
    {
        // when
        boltMessageLogger.logFailure( error.status() );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID,
                "S FAILURE Neo.TransientError.Transaction.DeadlockDetected" );

    }

    @Test
    public void logCorrelationIdClientEvent()
    {
        // when
        boltMessageLogger.clientEvent( "TEST" );

        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C TEST -" );
    }

    @Test
    public void logCorrelationIdClientErrorWithDetails()
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
    public void logCorrelationIdServerError()
    {
        // when
        boltMessageLogger.serverError( "TEST", "errorMessage" );

        // then
        verify( boltMessageLog ).error( REMOTE_ADDRESS, CORRELATION_ID, "S TEST", "errorMessage" );
    }

    @Test
    public void logServerErrorWithStatus()
    {
        // when
        boltMessageLogger.serverError( "TEST", error.status() );

        // then
        verify( boltMessageLog ).error( REMOTE_ADDRESS, CORRELATION_ID, "S TEST",
                "Neo.TransientError.Transaction.DeadlockDetected" );
    }

    @Test
    public void logCorrelationIdClientEventWithDetails()
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
    public void createCorrelationIdIfNotAvailableInInfoLogger()
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
    public void createCorrelationIdIfNotAvailableInErrorLogger()
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
