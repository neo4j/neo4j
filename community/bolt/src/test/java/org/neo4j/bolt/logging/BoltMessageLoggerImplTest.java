package org.neo4j.bolt.logging;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.bolt.logging.BoltMessageLoggerImpl.CORRELATION_ATTRIBUTE_KEY;

@RunWith(MockitoJUnitRunner.class)
public class BoltMessageLoggerImplTest
{

    @Mock
    private Channel channel;
    @Mock
    BoltMessageLog boltMessageLog;
    @Mock
    Attribute<String> correlationIdAttribute;


    BoltMessageLoggerImpl boltMessageLogger;


    @Before
    public void setUp() throws Exception
    {
        when( channel.remoteAddress() ).thenReturn( new InetSocketAddress( "localhost", 60297 ) );
        when( correlationIdAttribute.get() ).thenReturn( "Bolt-X-CorrelationId-1234" );
        when( channel.attr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( correlationIdAttribute );
        boltMessageLogger = new BoltMessageLoggerImpl( boltMessageLog, channel );
    }

    @Test
    public void logCorrelationIdClientEvent() throws Exception
    {
        // given
        when( correlationIdAttribute.get() ).thenReturn( "Bolt-X-CorrelationId-1234" );
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( true );

        // when
        boltMessageLogger.clientEvent( "TEST" );

        // then
        verify( boltMessageLog ).info( "localhost/127.0.0.1:60297", "C: <TEST>",
                "Bolt-X-CorrelationId-1234" );
    }

    @Test
    public void logCorrelationIdClientErrorWithDetails() throws Exception
    {
        // given
        when( correlationIdAttribute.get() ).thenReturn( "Bolt-X-CorrelationId-1234" );
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( true );

        // when
        boltMessageLogger.clientError( "TEST", "errorMessage", () -> "details" );

        // then
        verify( boltMessageLog ).error( "localhost/127.0.0.1:60297", "errorMessage", "C: <TEST> details",
                "Bolt-X-CorrelationId-1234" );
    }


    @Test
    public void logCorrelationIdServerError() throws Exception
    {
        // given
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( true );

        // when
        boltMessageLogger.serverError( "TEST", "errorMessage" );

        // then
        verify( boltMessageLog ).error( "localhost/127.0.0.1:60297", "errorMessage", "S: <TEST>",
                "Bolt-X-CorrelationId-1234" );
    }

    @Test
    public void logCorrelationIdClientEventWithDetails() throws Exception
    {
        // given
        when( correlationIdAttribute.get() ).thenReturn( "Bolt-X-CorrelationId-1234" );
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( true );
        when( channel.attr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( correlationIdAttribute );

        // when
        boltMessageLogger.clientEvent( "TEST", () -> "details" );

        // then
        verify( boltMessageLog ).info( "localhost/127.0.0.1:60297", "C: <TEST> details",
                "Bolt-X-CorrelationId-1234" );
    }

    @Test
    public void createCorrelationIdIfNotAvailableInInfoLogger() throws Exception
    {
        // given
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( false );
        BoltMessageLoggerImpl boltMessageLogger = new BoltMessageLoggerImpl( boltMessageLog, channel );

        // when
        boltMessageLogger.clientEvent( "TEST" );

        // then
        verify( correlationIdAttribute ).set( anyString() );
        verify( boltMessageLog ).info( "localhost/127.0.0.1:60297", "C: <TEST>", "Bolt-X-CorrelationId-1234" );
    }

    @Test
    public void createCorrelationIdIfNotAvailableInErrorLogger() throws Exception
    {
        // given
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( false );
        BoltMessageLoggerImpl boltMessageLogger = new BoltMessageLoggerImpl( boltMessageLog, channel );

        // when
        boltMessageLogger.serverError( "TEST", "errorMessage" );

        // then
        verify( correlationIdAttribute ).set( anyString() );
        verify( boltMessageLog ).error( "localhost/127.0.0.1:60297", "errorMessage", "S: <TEST>",
                "Bolt-X-CorrelationId-1234" );
    }
}
