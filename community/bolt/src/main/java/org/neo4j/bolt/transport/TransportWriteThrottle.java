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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.util.AttributeKey;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * Throttle that blocks write operations to the channel based on channel's isWritable
 * property. Buffer sizes based on which the channel will change its isWritable property
 * and whether to apply this throttle are configurable through GraphDatabaseSettings.
 */
public class TransportWriteThrottle implements TransportThrottle
{
    static final AttributeKey<ThrottleLock> LOCK_KEY = AttributeKey.valueOf( "BOLT.WRITE_THROTTLE.LOCK" );
    static final AttributeKey<Boolean> MAX_DURATION_EXCEEDED_KEY = AttributeKey.valueOf( "BOLT.WRITE_THROTTLE.MAX_DURATION_EXCEEDED" );
    private final int lowWaterMark;
    private final int highWaterMark;
    private final Clock clock;
    private final long maxLockDuration;
    private final Supplier<ThrottleLock> lockSupplier;
    private final ChannelInboundHandler listener;

    public TransportWriteThrottle( int lowWaterMark, int highWaterMark, Clock clock, Duration maxLockDuration )
    {
        this( lowWaterMark, highWaterMark, clock, maxLockDuration, DefaultThrottleLock::new );
    }

    public TransportWriteThrottle( int lowWaterMark, int highWaterMark, Clock clock, Duration maxLockDuration, Supplier<ThrottleLock> lockSupplier )
    {
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
        this.clock = clock;
        this.maxLockDuration = maxLockDuration.toMillis();
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
    public void acquire( Channel channel ) throws TransportThrottleException
    {
        // if this channel's max lock duration is already exceeded, we'll allow the protocol to
        // (at least) try to communicate the error to the client before aborting the connection
        if ( !isDurationAlreadyExceeded( channel ) )
        {
            ThrottleLock lock = channel.attr( LOCK_KEY ).get();

            long startTimeMillis = 0;
            while ( channel.isOpen() && !channel.isWritable() )
            {
                if ( maxLockDuration > 0 )
                {
                    long currentTimeMillis = clock.millis();
                    if ( startTimeMillis == 0 )
                    {
                        startTimeMillis = currentTimeMillis;
                    }
                    else
                    {
                        if ( currentTimeMillis - startTimeMillis > maxLockDuration )
                        {
                            setDurationExceeded( channel );

                            throw new TransportThrottleException( String.format(
                                    "Bolt connection [%s] will be closed because the client did not consume outgoing buffers for %s which is not expected.",
                                    channel.remoteAddress(), DurationFormatUtils.formatDurationHMS( maxLockDuration ) ) );
                        }
                    }
                }

                try
                {
                    lock.lock( channel, 1000 );
                }
                catch ( InterruptedException ex )
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException( ex );
                }
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

    private static boolean isDurationAlreadyExceeded( Channel channel )
    {
        Boolean marker = channel.attr( MAX_DURATION_EXCEEDED_KEY ).get();
        return marker != null && marker;
    }

    private static void setDurationExceeded( Channel channel )
    {
        channel.attr( MAX_DURATION_EXCEEDED_KEY ).set( Boolean.TRUE );
    }

    @ChannelHandler.Sharable
    private class ChannelStatusListener extends ChannelInboundHandlerAdapter
    {

        @Override
        public void channelWritabilityChanged( ChannelHandlerContext ctx )
        {
            release( ctx.channel() );
        }
    }
}
