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
package org.neo4j.bolt.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.util.Attribute;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.rule.concurrent.OtherThreadRule;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportWriteThrottleTest
{
    @Rule
    public OtherThreadRule<Void> otherThread = new OtherThreadRule<>( 1, TimeUnit.MINUTES );

    private ChannelHandlerContext context;
    private Channel channel;
    private SocketChannelConfig config;
    private ThrottleLock lock;
    private Attribute lockAttribute;

    @Before
    public void setup() throws Exception
    {
        lock = newThrottleLockMock();

        config = mock( SocketChannelConfig.class );

        lockAttribute = mock( Attribute.class );
        when( lockAttribute.get() ).thenReturn( lock );

        Attribute durationExceedAttribute = mock( Attribute.class );
        when( durationExceedAttribute.get() ).thenReturn( null );

        channel = mock( SocketChannel.class, Answers.RETURNS_MOCKS );
        when( channel.config() ).thenReturn( config );
        when( channel.isOpen() ).thenReturn( true );
        when( channel.remoteAddress() ).thenReturn( InetSocketAddress.createUnresolved( "localhost", 32000 ) );
        when( channel.attr( TransportWriteThrottle.LOCK_KEY ) ).thenReturn( lockAttribute );
        when( channel.attr( TransportWriteThrottle.MAX_DURATION_EXCEEDED_KEY ) ).thenReturn( durationExceedAttribute );

        ChannelPipeline pipeline = channel.pipeline();
        when( channel.pipeline() ).thenReturn( pipeline );

        context = mock( ChannelHandlerContext.class, Answers.RETURNS_MOCKS );
        when( context.channel() ).thenReturn( channel );
    }

    @Test
    public void shouldSetWriteBufferWatermarkOnChannelConfigWhenInstalled()
    {
        // given
        TransportThrottle throttle = newThrottle();

        // when
        throttle.install( channel );

        // expect
        ArgumentCaptor<WriteBufferWaterMark> argument = ArgumentCaptor.forClass( WriteBufferWaterMark.class );
        verify( config, times( 1 ) ).setWriteBufferWaterMark( argument.capture() );

        assertEquals( 64, argument.getValue().low() );
        assertEquals( 256, argument.getValue().high() );
    }

    @Test
    public void shouldNotLockWhenWritable() throws Exception
    {
        // given
        TestThrottleLock lockOverride = new TestThrottleLock();
        TransportThrottle throttle = newThrottleAndInstall( channel, lockOverride );
        when( channel.isWritable() ).thenReturn( true );

        // when
        Future future = otherThread.execute( state ->
        {
            throttle.acquire( channel );
            return null;
        } );

        // expect
        try
        {
            future.get( 2000, TimeUnit.MILLISECONDS );
        }
        catch ( Throwable t )
        {
            fail( "should not throw" );
        }

        assertTrue( future.isDone() );
        assertThat( lockOverride.lockCallCount(), is( 0 ) );
        assertThat( lockOverride.unlockCallCount(), is( 0 ) );
    }

    @Test
    public void shouldLockWhenNotWritable() throws Exception
    {
        // given
        TestThrottleLock lockOverride = new TestThrottleLock();
        TransportThrottle throttle = newThrottleAndInstall( channel, lockOverride );
        when( channel.isWritable() ).thenReturn( false );

        // when
        Future<Void> future = otherThread.execute( state ->
        {
            throttle.acquire( channel );
            return null;
        } );

        // expect
        try
        {
            future.get( 2000, TimeUnit.MILLISECONDS );

            fail( "should timeout" );
        }
        catch ( TimeoutException t )
        {
            // expected
        }

        assertFalse( future.isDone() );
        assertThat( lockOverride.lockCallCount(), greaterThan( 0 ) );
        assertThat( lockOverride.unlockCallCount(), is( 0 ) );

        // stop the thread that is trying to acquire the lock
        // otherwise it remains actively spinning even after the test
        future.cancel( true );
        try
        {
            otherThread.get().awaitFuture( future );
            fail( "Exception expected" );
        }
        catch ( CancellationException ignore )
        {
        }
    }

    @Test
    public void shouldResumeWhenWritableOnceAgain() throws Exception
    {
        // given
        TransportThrottle throttle = newThrottleAndInstall( channel );
        when( channel.isWritable() ).thenReturn( false ).thenReturn( true );

        // when
        throttle.acquire( channel );

        // expect
        verify( lock, atLeast( 1 ) ).lock( any(), anyLong() );
        verify( lock, never() ).unlock( any() );
    }

    @Test
    public void shouldResumeWhenWritabilityChanged() throws Exception
    {
        TestThrottleLock lockOverride = new TestThrottleLock();

        // given
        TransportThrottle throttle = newThrottleAndInstall( channel, lockOverride );
        when( channel.isWritable() ).thenReturn( false );

        Future<Void> completionFuture = otherThread.execute( state ->
        {
            throttle.acquire( channel );
            return null;
        } );

        otherThread.get().waitUntilWaiting();

        // when
        when( channel.isWritable() ).thenReturn( true );
        ArgumentCaptor<ChannelInboundHandler> captor = ArgumentCaptor.forClass( ChannelInboundHandler.class );
        verify( channel.pipeline() ).addLast( captor.capture() );
        captor.getValue().channelWritabilityChanged( context );

        otherThread.get().awaitFuture( completionFuture );

        assertThat( lockOverride.lockCallCount(), greaterThan( 0 ) );
        assertThat( lockOverride.unlockCallCount(), is( 1 ) );
    }

    @Test
    public void shouldThrowThrottleExceptionWhenMaxDurationIsReached() throws Exception
    {
        // given
        TestThrottleLock lockOverride = new TestThrottleLock();
        FakeClock clock = Clocks.fakeClock( 1, TimeUnit.SECONDS );
        TransportThrottle throttle = newThrottleAndInstall( channel, lockOverride, clock, Duration.ofSeconds( 5 ) );
        when( channel.isWritable() ).thenReturn( false );

        // when
        Future<Void> future = otherThread.execute( state ->
        {
            throttle.acquire( channel );
            return null;
        } );

        otherThread.get().waitUntilWaiting();
        clock.forward( 6, TimeUnit.SECONDS );

        // expect
        try
        {
            future.get( 1, TimeUnit.MINUTES );

            fail( "expecting ExecutionException" );
        }
        catch ( ExecutionException ex )
        {
            assertThat( ex.getCause(), instanceOf( TransportThrottleException.class ) );
            assertThat( ex.getMessage(), containsString( "will be closed because the client did not consume outgoing buffers for" ) );
        }
    }

    private TransportThrottle newThrottle()
    {
        return newThrottle( null, Clocks.systemClock(), Duration.ZERO );
    }

    private TransportThrottle newThrottle( ThrottleLock lockOverride, Clock clock, Duration maxLockDuration )
    {
        if ( lockOverride != null )
        {
            lock = lockOverride;

            when( lockAttribute.get() ).thenReturn( lockOverride );
        }

        return new TransportWriteThrottle( 64, 256, clock, maxLockDuration, () -> lock );
    }

    private TransportThrottle newThrottleAndInstall( Channel channel )
    {
        return newThrottleAndInstall( channel, null );
    }

    private TransportThrottle newThrottleAndInstall( Channel channel, ThrottleLock lockOverride )
    {
        return newThrottleAndInstall( channel, lockOverride, Clocks.systemClock(), Duration.ZERO );
    }

    private TransportThrottle newThrottleAndInstall( Channel channel, ThrottleLock lockOverride, Clock clock, Duration maxLockDuration )
    {
        TransportThrottle throttle = newThrottle( lockOverride, clock, maxLockDuration );

        throttle.install( channel );

        return throttle;
    }

    private static ThrottleLock newThrottleLockMock() throws InterruptedException
    {
        ThrottleLock lock = mock( ThrottleLock.class );
        doAnswer( invocation ->
        {
            // sleep a bit to prevent the caller thread spinning in a tight loop
            // every mock invocation is recorded and generates objects, like the stacktrace
            Thread.sleep( 500 );
            return null;
        } ).when( lock ).lock( any(), anyLong() );
        return lock;
    }

    private static class TestThrottleLock implements ThrottleLock
    {
        private AtomicInteger lockCount = new AtomicInteger( 0 );
        private AtomicInteger unlockCount = new AtomicInteger( 0 );
        private ThrottleLock actualLock = new DefaultThrottleLock();

        @Override
        public void lock( Channel channel, long timeout ) throws InterruptedException
        {
            actualLock.lock( channel, timeout );
            lockCount.incrementAndGet();
        }

        @Override
        public void unlock( Channel channel )
        {
            actualLock.unlock( channel );
            unlockCount.incrementAndGet();
        }

        public int lockCallCount()
        {
            return lockCount.get();
        }

        public int unlockCallCount()
        {
            return unlockCount.get();
        }

    }

}
