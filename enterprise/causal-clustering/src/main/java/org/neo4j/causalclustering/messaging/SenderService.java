/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class SenderService extends LifecycleAdapter implements Outbound<AdvertisedSocketAddress,Message>
{
    private ReconnectingChannels channels;

    private final ChannelInitializer channelInitializer;
    private final ReadWriteLock serviceLock = new ReentrantReadWriteLock();
    private final Log log;

    private JobScheduler.JobHandle jobHandle;
    private boolean senderServiceRunning;
    private Bootstrap bootstrap;
    private NioEventLoopGroup eventLoopGroup;

    public SenderService( ChannelInitializer channelInitializer, LogProvider logProvider )
    {
        this.channelInitializer = channelInitializer;
        this.log = logProvider.getLog( getClass() );
        this.channels = new ReconnectingChannels();
    }

    @Override
    public void send( AdvertisedSocketAddress to, Message message, boolean block )
    {
        Future<Void> future;
        serviceLock.readLock().lock();
        try
        {
            if ( !senderServiceRunning )
            {
                return;
            }

            future = channel( to ).writeAndFlush( message );
        }
        finally
        {
            serviceLock.readLock().unlock();
        }

        if ( block )
        {
            try
            {
                future.get();
            }
            catch ( ExecutionException e )
            {
                log.error( "Exception while sending to: " + to, e );
            }
            catch ( InterruptedException e )
            {
                log.info( "Interrupted while sending", e );
            }
        }
    }

    private Channel channel( AdvertisedSocketAddress destination )
    {
        ReconnectingChannel channel = channels.get( destination );

        if ( channel == null )
        {
            channel = new ReconnectingChannel( bootstrap, eventLoopGroup.next(), destination, log );
            channel.start();
            ReconnectingChannel existingNonBlockingChannel = channels.putIfAbsent( destination, channel );

            if ( existingNonBlockingChannel != null )
            {
                channel.dispose();
                channel = existingNonBlockingChannel;
            }
            else
            {
                log.info( "Creating channel to: [%s] ", destination );
            }
        }

        return channel;
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

            Iterator<ReconnectingChannel> itr = channels.values().iterator();
            while ( itr.hasNext() )
            {
                Channel timestampedChannel = itr.next();
                timestampedChannel.dispose();
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

    public Stream<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocols()
    {
        return channels.installedProtocols();
    }
}
