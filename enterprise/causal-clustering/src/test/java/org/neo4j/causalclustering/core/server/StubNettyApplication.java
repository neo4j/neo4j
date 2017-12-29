package org.neo4j.causalclustering.core.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StubNettyApplication extends AbstractNettyApplication<StubNettyApplication.CountingBindRequestBootstrap>
{
    private final EventLoopGroup eventExecutors;
    private final CountingBindRequestBootstrap bootstrap;

    static StubNettyApplication mockedEventExecutor() throws InterruptedException, ExecutionException, TimeoutException
    {
        return new StubNettyApplication( createMockedEventExecutor() );
    }

    static StubNettyApplication realEventExecutor() throws InterruptedException, ExecutionException, TimeoutException
    {
        return new StubNettyApplication();
    }

    private StubNettyApplication()
    {
        this( new NioEventLoopGroup( 0, new NamedThreadFactory( "test" ) ) );
    }

    StubNettyApplication( Exception bindFailure ) throws InterruptedException, ExecutionException, TimeoutException
    {
        super( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        this.eventExecutors = createMockedEventExecutor();
        this.bootstrap = new CountingBindRequestBootstrap( bindFailure );

    }

    StubNettyApplication( EventLoopGroup eventExecutors )
    {
        super( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        this.eventExecutors = eventExecutors;
        this.bootstrap = new CountingBindRequestBootstrap();

    }

    public EventLoopGroup getEventExecutors()
    {
        return eventExecutors;
    }

    @Override
    protected EventLoopGroup getEventLoopGroup()
    {
        return eventExecutors;
    }

    @Override
    protected CountingBindRequestBootstrap bootstrap()
    {
        return bootstrap;
    }

    @Override
    protected InetSocketAddress bindAddress()
    {
        return new InetSocketAddress( 1 );
    }

    public class CountingBindRequestBootstrap extends Bootstrap
    {
        private int bindCalls = 0;
        private final Exception failure;
        private final boolean failed;

        CountingBindRequestBootstrap()
        {
            this.failure = null;
            this.failed = false;
        }

        private CountingBindRequestBootstrap( Exception failure )
        {
            this.failure = failure;
            this.failed = true;
        }

        @Override
        public ChannelFuture bind( SocketAddress address )
        {
            bindCalls++;
            Channel mockedChannel = mock( Channel.class );
            ChannelFuture mockedFuture = mock( ChannelFuture.class );
            when( mockedFuture.awaitUninterruptibly() ).thenReturn( mockedFuture );
            when( mockedFuture.channel() ).thenReturn( mockedChannel );
            when( mockedChannel.close() ).thenReturn( mockedFuture );
            if ( failed )
            {
                when( mockedFuture.isSuccess() ).thenReturn( false );
                when( mockedFuture.cause() ).thenReturn( failure );
            }
            else
            {
                when( mockedFuture.isSuccess() ).thenReturn( true );
            }

            return mockedFuture;
        }

        public int getBindCalls()
        {
            return bindCalls;
        }
    }

    private static EventLoopGroup createMockedEventExecutor()
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException
    {
        EventLoopGroup eventExecutors = mock( EventLoopGroup.class );
        Future future = mock( Future.class );
        doReturn( null ).when( future ).get( anyInt(), any( TimeUnit.class ) );
        when( eventExecutors.shutdownGracefully( anyInt(), anyInt(), any( TimeUnit.class ) ) ).thenReturn( future );
        return eventExecutors;
    }
}
