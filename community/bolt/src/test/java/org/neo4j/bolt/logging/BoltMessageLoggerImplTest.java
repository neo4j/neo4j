package org.neo4j.bolt.logging;

import java.net.InetSocketAddress;
import java.util.Collections;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.kernel.DeadlockDetectedException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.bolt.logging.BoltMessageLoggerImpl.CORRELATION_ATTRIBUTE_KEY;

@RunWith(MockitoJUnitRunner.class)
public class BoltMessageLoggerImplTest
{

    private static final String REMOTE_ADDRESS = "localhost/127.0.0.1:60297";
    private static final String CORRELATION_ID = "Bolt-X-CorrelationId-1234";
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
        when( correlationIdAttribute.get() ).thenReturn( CORRELATION_ID );
        when( channel.attr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( correlationIdAttribute );
        boltMessageLogger = new BoltMessageLoggerImpl( boltMessageLog, channel );
    }

    @Test
    public void clientEventWithDetails() throws Exception
    {
        // when
        boltMessageLogger.clientEvent( "TEST", () -> "details" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C: TEST details" );
    }

    @Test
    public void serverEvent() throws Exception
    {
        // when
        boltMessageLogger.serverEvent( "TEST" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S: TEST -" );
    }

    @Test
    public void serverEventWithDetails() throws Exception
    {
        // when
        boltMessageLogger.serverEvent( "TEST", () -> "details" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S: TEST details" );
    }


    @Test
    public void logAckFailure() throws Exception
    {
        // when
        boltMessageLogger.logAckFailure();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C: ACK_FAILURE -" );

    }

    @Test
    public void logInit() throws Exception
    {
        // when
        boltMessageLogger.logInit( "userAgent", Collections.singletonMap( "username", "password" ) );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C: INIT userAgent [\"username\"]" );

    }

    @Test
    public void logRun() throws Exception
    {
        // when
        boltMessageLogger.logRun( "RETURN 42", Collections.singletonMap( "param1", "value" ) );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C: RUN RETURN 42 {\"param1\":\"value\"}" );
    }

    @Test
    public void logPullAll() throws Exception
    {
        // when
        boltMessageLogger.logPullAll();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C: PULL_ALL -" );
    }

    @Test
    public void logDiscardAll() throws Exception
    {
        // when
        boltMessageLogger.logDiscardAll();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C: DISCARD_ALL -" );
    }

    @Test
    public void logReset() throws Exception
    {
        // when
        boltMessageLogger.logReset();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C: RESET -" );
    }

    @Test
    public void logSuccess() throws Exception
    {
        // when
        boltMessageLogger.logSuccess( "metadata" );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S: SUCCESS \"metadata\"" );
    }

    @Test
    public void logIgnored() throws Exception
    {
        // when
        boltMessageLogger.logIgnored();
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "S: IGNORED -" );
    }

    @Test
    public void logFailure() throws Exception
    {
        // given
        String errorMessage = "Oh my woes!";
        Neo4jError error = Neo4jError.from( new DeadlockDetectedException( errorMessage ) );
        // when
        boltMessageLogger.logFailure( error.status(), errorMessage );
        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID,
                "S: FAILURE Neo.TransientError.Transaction.DeadlockDetected Oh my woes!" );

    }

    @Test
    public void logCorrelationIdClientEvent() throws Exception
    {
        // given
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( true );

        // when
        boltMessageLogger.clientEvent( "TEST" );

        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C: TEST -" );
    }

    @Test
    public void logCorrelationIdClientErrorWithDetails() throws Exception
    {
        // given
        when( correlationIdAttribute.get() ).thenReturn( CORRELATION_ID );
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( true );

        // when
        boltMessageLogger.clientError( "TEST", "errorMessage", () -> "details" );

        // then
        verify( boltMessageLog ).error( REMOTE_ADDRESS, "errorMessage", "C: <TEST> details",
                CORRELATION_ID );
    }

    @Test
    public void logCorrelationIdServerError() throws Exception
    {
        // given
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( true );

        // when
        boltMessageLogger.serverError( "TEST", "errorMessage" );

        // then
        verify( boltMessageLog ).error( REMOTE_ADDRESS, "errorMessage", "S: <TEST>",
                CORRELATION_ID );
    }

    @Test
    public void logCorrelationIdClientEventWithDetails() throws Exception
    {
        // given
        when( correlationIdAttribute.get() ).thenReturn( CORRELATION_ID );
        when( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( true );
        when( channel.attr( CORRELATION_ATTRIBUTE_KEY ) ).thenReturn( correlationIdAttribute );

        // when
        boltMessageLogger.clientEvent( "TEST", () -> "details" );

        // then
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C: TEST details" );
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
        verify( boltMessageLog ).info( REMOTE_ADDRESS, CORRELATION_ID, "C: TEST -" );
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
        verify( boltMessageLog ).error( REMOTE_ADDRESS, "errorMessage", "S: <TEST>",
                CORRELATION_ID );
    }
}
