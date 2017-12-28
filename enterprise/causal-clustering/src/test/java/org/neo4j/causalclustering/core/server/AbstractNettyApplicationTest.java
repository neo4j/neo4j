package org.neo4j.causalclustering.core.server;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractNettyApplicationTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldTerminateEventLoopGroupOnShutdown() throws Throwable
    {
        // given
        StubNettyApplication
                stubServer = StubNettyApplication.realEventExecutor();
        assertFalse( stubServer.getEventExecutors().isTerminated() );

        // when
        stubServer.shutdown();

        // then
        assertTrue( stubServer.getEventExecutors().isTerminated() );
    }

    @Test
    public void shouldHandleNullOnShutdown() throws Throwable
    {
        // given
        StubNettyApplication
                stubServer = new StubNettyApplication( null );
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "EventLoopGroup cannot be null" );

        // then
        stubServer.shutdown();
    }

    @Test
    public void shouldThrowFailedFutureCause() throws Throwable
    {
        // given
        EventLoopGroup eventExecutors = mock( EventLoopGroup.class );
        Future future = mock( Future.class );
        Exception exception = new RuntimeException( "some exception" );
        doThrow( exception ).when( future ).get( anyInt(), any( TimeUnit.class ) );
        when( eventExecutors.shutdownGracefully( anyInt(), anyInt(), any( TimeUnit.class ) ) ).thenReturn( future );
        StubNettyApplication
                stubServer = new StubNettyApplication( eventExecutors );
        expectedException.expect( RuntimeException.class );
        expectedException.expectMessage( "some exception" );

        // when
        stubServer.shutdown();
    }

    @Test
    public void shouldStopAndStart() throws Throwable
    {
        // given
        StubNettyApplication stubServer = StubNettyApplication.mockedEventExecutor();
        stubServer.init();

        // when
        stubServer.start();
        stubServer.stop();
        stubServer.stop();
        stubServer.start();

        //then
        assertEquals( 2, stubServer.bootstrap().getBindCalls() );
    }

    @Test
    public void shouldNotRebindIfAlreadyRunning() throws Throwable
    {
        // given
        StubNettyApplication stubServer = StubNettyApplication.mockedEventExecutor();
        stubServer.init();

        // when
        stubServer.start();
        stubServer.start();

        //then
        assertEquals( 1, stubServer.bootstrap().getBindCalls() );
    }
}
