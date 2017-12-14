/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportWriteThrottleTest
{
    private ChannelHandlerContext context;
    private Channel channel;
    private SocketChannelConfig config;
    private TestThrottleLock lock;

    @Before
    public void setup() throws Exception
    {
        lock = new TestThrottleLock();

        config = mock( SocketChannelConfig.class );

        Attribute lockAttribute = mock( Attribute.class );
        when( lockAttribute.get() ).thenReturn( lock );

        channel = mock( SocketChannel.class, Answers.RETURNS_MOCKS );
        when( channel.config() ).thenReturn( config );
        when( channel.isOpen() ).thenReturn( true );
        when( channel.attr( any() ) ).thenReturn( lockAttribute );

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
        TransportThrottle throttle = newThrottleAndInstall( channel );
        when( channel.isWritable() ).thenReturn( true );

        // when
        Future future = Executors.newSingleThreadExecutor().submit( () -> throttle.acquire( channel ) );

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
        assertThat( lock.lockCallCount(), is( 0 ) );
        assertThat( lock.unlockCallCount(), is( 0 ) );
    }

    @Test
    public void shouldLockWhenNotWritable() throws Exception
    {
        // given
        TransportThrottle throttle = newThrottleAndInstall( channel );
        when( channel.isWritable() ).thenReturn( false );

        // when
        Future future = Executors.newSingleThreadExecutor().submit( () -> throttle.acquire( channel ) );

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
        catch ( Throwable t )
        {
            fail( "should timeout" );
        }

        assertFalse( future.isDone() );
        assertThat( lock.lockCallCount(), greaterThan( 0 ) );
        assertThat( lock.unlockCallCount(), is( 0 ) );
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
        assertThat( lock.lockCallCount(), greaterThan( 0 ) );
        assertThat( lock.unlockCallCount(), is( 0 ) );
    }

    @Test
    public void shouldResumeWhenWritabilityChanged() throws Exception
    {
        // given
        TransportThrottle throttle = newThrottleAndInstall( channel );
        when( channel.isWritable() ).thenReturn( false );

        Future future = Executors.newSingleThreadExecutor().submit( () -> throttle.acquire( channel ) );

        // Wait until lock is acquired.
        if ( !lock.waitLocked( 10, TimeUnit.SECONDS ) )
        {
            fail( "lock should be acquired" );
        }

        // when
        when( channel.isWritable() ).thenReturn( true );
        ArgumentCaptor<ChannelInboundHandler> captor = ArgumentCaptor.forClass( ChannelInboundHandler.class );
        verify( channel.pipeline() ).addLast( captor.capture() );
        captor.getValue().channelWritabilityChanged( context );

        // expect
        try
        {
            future.get( 20, TimeUnit.SECONDS );
        }
        catch ( Throwable t )
        {
            fail( "should not throw" );
        }

        assertThat( lock.lockCallCount(), greaterThan( 0 ) );
        assertThat( lock.unlockCallCount(), is( 1 ) );
    }

    private TransportThrottle newThrottle()
    {
        return new TransportWriteThrottle( 64, 256, () -> lock );
    }

    private TransportThrottle newThrottleAndInstall( Channel channel )
    {
        TransportThrottle throttle = newThrottle();

        throttle.install( channel );

        return throttle;
    }

    private static class TestThrottleLock implements ThrottleLock
    {
        private AtomicInteger lockCount = new AtomicInteger( 0 );
        private AtomicInteger unlockCount = new AtomicInteger( 0 );
        private Semaphore semaphore = new Semaphore( 1 );
        private volatile CountDownLatch lockWaiter = new CountDownLatch( 1 );

        @Override
        public void lock( Channel channel, long timeout ) throws InterruptedException
        {
            semaphore.acquire();
            lockCount.incrementAndGet();
            lockWaiter.countDown();
        }

        @Override
        public void unlock( Channel channel )
        {
            semaphore.release();
            unlockCount.incrementAndGet();
            lockWaiter = new CountDownLatch( 1 );
        }

        public boolean waitLocked( long timeout, TimeUnit unit ) throws InterruptedException
        {
            return lockWaiter.await( timeout, unit );
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
