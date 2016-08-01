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
package org.neo4j.coreedge.messaging;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.FutureListener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.coreedge.messaging.monitoring.MessageQueueMonitor;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public class NonBlockingChannel
{
    private static final int CONNECT_BACKOFF_IN_MS = 250;
    /* This pause is a maximum for retrying in case of a park/unpark race as well as for any other abnormal
    situations. */
    private static final int RETRY_DELAY_MS = 100;
    private final Thread messageSendingThread;
    private Channel nettyChannel;
    private Bootstrap bootstrap;
    private InetSocketAddress destination;
    private Queue<Object> messageQueue = new ConcurrentLinkedQueue<>();
    private volatile boolean stillRunning = true;
    private final MessageQueueMonitor monitor;
    private final int maxQueueSize;
    FutureListener<Void> errorListener;

    public NonBlockingChannel( Bootstrap bootstrap, final InetSocketAddress destination,
                               final Log log, MessageQueueMonitor monitor,
                               int maxQueueSize )
    {
        this.bootstrap = bootstrap;
        this.destination = destination;
        this.monitor = monitor;
        this.maxQueueSize = maxQueueSize;

        this.errorListener = future -> {
            if ( !future.isSuccess() )
            {
                log.error( "Failed to send message to " + destination, future.cause() );
            }
        };

        messageSendingThread = new Thread( this::messageSendingThreadWork );
        messageSendingThread.start();
    }

    private void messageSendingThreadWork()
    {
        while ( stillRunning )
        {
            try
            {
                ensureConnected();

                if ( sendMessages() )
                {
                    nettyChannel.flush();
                }
            }
            catch ( IOException e )
            {
                /* IO-exceptions from inside netty are dealt with by closing any existing channel and retrying with a
                 fresh one. */
                if ( nettyChannel != null )
                {
                    nettyChannel.close();
                    nettyChannel = null;
                }
            }

            parkNanos( MILLISECONDS.toNanos( RETRY_DELAY_MS ) );
        }

        if ( nettyChannel != null )
        {
            nettyChannel.close();
            messageQueue.clear();
            monitor.queueSize(destination, messageQueue.size());
        }
    }

    public void dispose()
    {
        stillRunning = false;

        while ( messageSendingThread.isAlive() )
        {
            messageSendingThread.interrupt();

            try
            {
                messageSendingThread.join( 100 );
            }
            catch ( InterruptedException e )
            {
                // Do nothing
            }
        }
    }

    public void send( Object msg )
    {
        if ( !stillRunning )
        {
            throw new IllegalStateException( "sending on disposed channel" );
        }

        if ( messageQueue.size() < maxQueueSize )
        {
            messageQueue.offer( msg );
            LockSupport.unpark( messageSendingThread );
            monitor.queueSize( destination, messageQueue.size());
        }
        else
        {
            monitor.droppedMessage(destination);
        }
    }

    private boolean sendMessages() throws IOException
    {
        if ( nettyChannel == null )
        {
            return false;
        }

        boolean sentSomething = false;
        Object message;
        while ( (message = messageQueue.peek()) != null )
        {
            ChannelFuture write = nettyChannel.write( message );
            write.addListener( errorListener );

            messageQueue.poll();
            monitor.queueSize( destination, messageQueue.size());
            sentSomething = true;
        }

        return sentSomething;
    }

    private void ensureConnected() throws IOException
    {
        if ( nettyChannel != null && !nettyChannel.isOpen() )
        {
            nettyChannel = null;
        }

        while ( nettyChannel == null && stillRunning )
        {
            ChannelFuture channelFuture = bootstrap.connect( destination );

            Channel channel = channelFuture.awaitUninterruptibly().channel();
            if ( channelFuture.isSuccess() )
            {
                channel.flush();
                nettyChannel = channel;
            }
            else
            {
                channel.close();
                parkNanos( MILLISECONDS.toNanos( CONNECT_BACKOFF_IN_MS ) );
            }
        }
    }
}
