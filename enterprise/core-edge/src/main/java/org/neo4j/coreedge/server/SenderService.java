/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.server;

import org.neo4j.coreedge.network.Message;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.neo4j.coreedge.raft.net.NonBlockingChannel;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.net.monitoring.MessageQueueMonitor;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class SenderService extends LifecycleAdapter implements Outbound<AdvertisedSocketAddress>
{
    private final Expiration expiration;
    private final ConcurrentHashMap<AdvertisedSocketAddress,TimestampedNonBlockingChannel> lazyChannelMap =
            new ConcurrentHashMap<>();
    private final ExpiryScheduler scheduler;
    private final ChannelInitializer<SocketChannel> channelInitializer;
    private final ReadWriteLock serviceLock = new ReentrantReadWriteLock();
    private final Log log;
    private final Monitors monitors;

    private JobScheduler.JobHandle jobHandle;
    private boolean senderServiceRunning;
    private Bootstrap bootstrap;
    private NioEventLoopGroup eventLoopGroup;
    private int maxQueueSize;

    public SenderService( ExpiryScheduler expiryScheduler,
                          Expiration expiration,
                          ChannelInitializer<SocketChannel> channelInitializer,
                          LogProvider logProvider,
                          Monitors monitors,
                          int maxQueueSize)
    {
        this.expiration = expiration;
        this.scheduler = expiryScheduler;
        this.channelInitializer = channelInitializer;
        this.log = logProvider.getLog( getClass() );
        this.monitors = monitors;
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    public void send( AdvertisedSocketAddress to, Message... messages )
    {
        serviceLock.readLock().lock();
        try
        {
            if ( !senderServiceRunning )
            {
                return;
            }

            MessageQueueMonitor monitor = monitors.newMonitor( MessageQueueMonitor.class, NonBlockingChannel.class );
            TimestampedNonBlockingChannel lazyChannel = getAndUpdateLife( to, monitor );
            NonBlockingChannel nonBlockingChannel = lazyChannel.get();
            monitor.register(to.socketAddress());
            for ( Object msg : messages )
            {
                nonBlockingChannel.send( msg );
            }
        }
        finally
        {
            serviceLock.readLock().unlock();
        }
    }

    public int activeChannelCount()
    {
        return lazyChannelMap.size();
    }

    private TimestampedNonBlockingChannel getAndUpdateLife( AdvertisedSocketAddress to, MessageQueueMonitor monitor )
    {
        TimestampedNonBlockingChannel timestampedLazyChannel = lazyChannelMap.get( to );

        if ( timestampedLazyChannel == null )
        {
            Expiration.ExpirationTime expirationTime = expiration.new ExpirationTime();
            timestampedLazyChannel = new TimestampedNonBlockingChannel( expirationTime,
                    new NonBlockingChannel( bootstrap, to.socketAddress(),
                            new InboundKeepAliveHandler( expirationTime ), log, monitor, maxQueueSize ) );

            TimestampedNonBlockingChannel existingTimestampedLazyChannel =
                    lazyChannelMap.putIfAbsent( to, timestampedLazyChannel );

            if ( existingTimestampedLazyChannel != null )
            {
                timestampedLazyChannel.get().dispose();
                timestampedLazyChannel = existingTimestampedLazyChannel;
            }
        }

        timestampedLazyChannel.getEndOfLife().renew();

        return timestampedLazyChannel;
    }

    @Override
    public synchronized void start()
    {
        serviceLock.writeLock().lock();
        try
        {
            eventLoopGroup = new NioEventLoopGroup( 0, new NamedThreadFactory( "sender-service" ) );
            bootstrap = new Bootstrap()
                    .group( eventLoopGroup )
                    .channel( NioSocketChannel.class )
                    .handler( channelInitializer );

            if ( scheduler != null )
            {
                jobHandle = scheduler.schedule( this::reapDeadChannels );
            }

            senderServiceRunning = true;
        }
        finally
        {
            serviceLock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void stop()
    {
        serviceLock.writeLock().lock();
        try
        {
            senderServiceRunning = false;

            if ( jobHandle != null )
            {
                jobHandle.cancel( true );
                jobHandle = null;
            }

            Iterator<TimestampedNonBlockingChannel> itr = lazyChannelMap.values().iterator();
            while ( itr.hasNext() )
            {
                TimestampedNonBlockingChannel timestampedChannel = itr.next();
                timestampedChannel.get().dispose();
                itr.remove();
            }

            try
            {
                eventLoopGroup.shutdownGracefully( 0, 0, MICROSECONDS ).sync();
            }
            catch ( InterruptedException e )
            {
                log.warn( "Interrupted while stopping sender service." );
            }
        }
        finally
        {
            serviceLock.writeLock().unlock();
        }
    }

    private synchronized void reapDeadChannels()
    {
        Iterator<TimestampedNonBlockingChannel> itr = lazyChannelMap.values().iterator();
        while ( itr.hasNext() )
        {
            TimestampedNonBlockingChannel timestampedChannel = itr.next();

            serviceLock.writeLock().lock();
            try
            {
                if ( timestampedChannel.getEndOfLife().expired() )
                {
                    timestampedChannel.get().dispose();
                    itr.remove();
                }
            }
            finally
            {
                serviceLock.writeLock().unlock();
            }
        }
    }

    private final class TimestampedNonBlockingChannel
    {
        private final Expiration.ExpirationTime endOfLife;
        private NonBlockingChannel channel;

        public TimestampedNonBlockingChannel( Expiration.ExpirationTime endOfLife, NonBlockingChannel channel )
        {
            this.endOfLife = endOfLife;
            this.channel = channel;
        }

        public NonBlockingChannel get()
        {
            return channel;
        }

        public Expiration.ExpirationTime getEndOfLife()
        {
            return endOfLife;
        }
    }

    @ChannelHandler.Sharable
    private class InboundKeepAliveHandler extends ChannelInboundHandlerAdapter
    {
        private final Expiration.ExpirationTime expirationTime;

        public InboundKeepAliveHandler( Expiration.ExpirationTime expirationTime )
        {
            this.expirationTime = expirationTime;
        }

        @Override
        public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
        {
            expirationTime.renew();
            super.channelRead( ctx, msg );
        }
    }
}
