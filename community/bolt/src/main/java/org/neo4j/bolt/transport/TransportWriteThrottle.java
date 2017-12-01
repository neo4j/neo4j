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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.util.AttributeKey;

import java.util.function.Supplier;

/**
 * Throttle that blocks write operations to the channel based on channel's isWritable
 * property. Buffer sizes based on which the channel will change its isWritable property
 * and whether to apply this throttle are configurable through GraphDatabaseSettings.
 */
public class TransportWriteThrottle implements TransportThrottle
{
    private static final AttributeKey<ThrottleLock> LOCK_KEY = AttributeKey.valueOf( "BOLT.WRITE_THROTTLE.LOCK" );
    private final int lowWaterMark;
    private final int highWaterMark;
    private final Supplier<ThrottleLock> lockSupplier;
    private final ChannelInboundHandler listener;

    public TransportWriteThrottle( int lowWaterMark, int highWaterMark )
    {
        this( lowWaterMark, highWaterMark, () -> new DefaultThrottleLock() );
    }

    public TransportWriteThrottle( int lowWaterMark, int highWaterMark, Supplier<ThrottleLock> lockSupplier )
    {
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
        this.lockSupplier = lockSupplier;
        this.listener = new ChannelStatusListener();
    }

    @Override
    public void install( Channel channel )
    {
        ThrottleLock lock = lockSupplier.get();

        channel.attr( LOCK_KEY ).set( lock );
        channel.config().setWriteBufferWaterMark( new WriteBufferWaterMark( lowWaterMark, highWaterMark ) );
        channel.pipeline().addLast( listener );
    }

    @Override
    public void acquire( Channel channel )
    {
        ThrottleLock lock = channel.attr( LOCK_KEY ).get();

        while ( channel.isOpen() && !channel.isWritable() )
        {
            try
            {
                lock.lock( channel, 1000 );
            }
            catch ( InterruptedException ex )
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void release( Channel channel )
    {
        if ( channel.isWritable() )
        {
            ThrottleLock lock = channel.attr( LOCK_KEY ).get();

            lock.unlock( channel );
        }
    }

    @Override
    public void uninstall( Channel channel )
    {
        channel.attr( LOCK_KEY ).set( null );
    }

    @ChannelHandler.Sharable
    private class ChannelStatusListener extends ChannelInboundHandlerAdapter
    {

        @Override
        public void channelWritabilityChanged( ChannelHandlerContext ctx ) throws Exception
        {
            release( ctx.channel() );
        }
    }
}
