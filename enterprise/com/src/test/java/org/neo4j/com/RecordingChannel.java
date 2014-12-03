/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.com;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RecordingChannel implements Channel
{
    private Queue<ByteBuf> recievedMessages = new LinkedList<ByteBuf>();

    @Override
    public ChannelFuture write( Object message )
    {
        if(message instanceof ByteBuf )
        {
            ByteBuf buffer = (ByteBuf)message;
            recievedMessages.offer( buffer.duplicate() );
        }
        return immediateFuture;
    }

    @Override
    public ChannelFuture write( Object msg, ChannelPromise promise )
    {
        write( msg );
        return immediateFuture;
    }

    @Override
    public Channel flush()
    {
        return null;
    }

    @Override
    public ChannelFuture writeAndFlush( Object msg, ChannelPromise promise )
    {
        write(msg);
        return immediateFuture;
    }

    @Override
    public ChannelFuture writeAndFlush( Object msg )
    {
        write(msg);
        return immediateFuture;
    }

    @Override
    public EventLoop eventLoop()
    {
        return null;
    }

    @Override
    public Channel parent()
    {
        return null;
    }

    @Override
    public ChannelConfig config()
    {
        return null;
    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public boolean isRegistered()
    {
        return false;
    }

    @Override
    public boolean isActive()
    {
        return true;
    }

    @Override
    public ChannelMetadata metadata()
    {
        return null;
    }

    @Override
    public SocketAddress localAddress()
    {
        return null;
    }

    @Override
    public SocketAddress remoteAddress()
    {
        return null;
    }

    @Override
    public ChannelFuture closeFuture()
    {
        return null;
    }

    @Override
    public ChannelFuture bind( SocketAddress localAddress )
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public ChannelFuture connect( SocketAddress remoteAddress )
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public ChannelFuture connect( SocketAddress remoteAddress, SocketAddress localAddress )
    {
        return null;
    }

    @Override
    public ChannelFuture disconnect()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public ChannelFuture close()
    {
        return null;
    }

    @Override
    public ChannelFuture deregister()
    {
        return null;
    }

    @Override
    public ChannelFuture bind( SocketAddress localAddress, ChannelPromise promise )
    {
        return null;
    }

    @Override
    public ChannelFuture connect( SocketAddress remoteAddress, ChannelPromise promise )
    {
        return null;
    }

    @Override
    public ChannelFuture connect( SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise )
    {
        return null;
    }

    @Override
    public ChannelFuture disconnect( ChannelPromise promise )
    {
        return null;
    }

    @Override
    public ChannelFuture close( ChannelPromise promise )
    {
        return null;
    }

    @Override
    public ChannelFuture deregister( ChannelPromise promise )
    {
        return null;
    }

    @Override
    public Channel read()
    {
        return null;
    }

    @Override
    public boolean isWritable()
    {
        return true;
    }

    @Override
    public Unsafe unsafe()
    {
        return null;
    }

    @Override
    public ChannelPipeline pipeline()
    {
        return null;
    }

    @Override
    public ByteBufAllocator alloc()
    {
        return PooledByteBufAllocator.DEFAULT;
    }

    @Override
    public ChannelPromise newPromise()
    {
        return null;
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise()
    {
        return null;
    }

    @Override
    public ChannelFuture newSucceededFuture()
    {
        return null;
    }

    @Override
    public ChannelFuture newFailedFuture( Throwable cause )
    {
        return null;
    }

    @Override
    public ChannelPromise voidPromise()
    {
        return null;
    }

    @Override
    public int compareTo( Channel o )
    {
        return 0;
    }

    // This is due to a tight coupling of the netty pipeline and message deserialization, we can't deserialize without
    // this pipeline item yet. We should refactor the serialization/deserialzation code appropriately such that it is
    // not tied like this to components it should not be aware of.
    public BlockingReadHandler<ByteBuf> asBlockingReadHandler()
    {
        return new BlockingReadHandler<ByteBuf>()
        {
            @Override
            public ByteBuf read() throws IOException, InterruptedException
            {
                return recievedMessages.poll();
            }

            @Override
            public ByteBuf read( long timeout, TimeUnit unit ) throws IOException, InterruptedException
            {
                return read();
            }
        };
    }

    private ChannelFuture immediateFuture = new ChannelFuture()
    {
        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException
        {
            return null;
        }

        @Override
        public Void get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException
        {
            return null;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isSuccess()
        {
            return true;
        }

        @Override
        public boolean isCancellable()
        {
            return false;
        }

        @Override
        public Throwable cause()
        {
            return null;
        }

        @Override
        public Channel channel()
        {
            return null;
        }

        @Override
        public ChannelFuture addListener( GenericFutureListener<? extends Future<? super Void>> listener )
        {
            try
            {
//                listener.operationComplete( this );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            return this;
        }

        @Override
        public ChannelFuture addListeners( GenericFutureListener<? extends Future<? super Void>>... listeners )
        {
            return null;
        }

        @Override
        public ChannelFuture removeListener( GenericFutureListener<? extends Future<? super Void>> listener )
        {
            return null;
        }

        @Override
        public ChannelFuture removeListeners( GenericFutureListener<? extends Future<? super Void>>... listeners )
        {
            return null;
        }

        @Override
        public ChannelFuture sync() throws InterruptedException
        {
            return null;
        }

        @Override
        public ChannelFuture syncUninterruptibly()
        {
            return null;
        }

        @Override
        public ChannelFuture await() throws InterruptedException
        {
            return null;
        }

        @Override
        public ChannelFuture awaitUninterruptibly()
        {
            return null;
        }

        @Override
        public boolean await( long timeout, TimeUnit unit ) throws InterruptedException
        {
            return false;
        }

        @Override
        public boolean await( long timeoutMillis ) throws InterruptedException
        {
            return false;
        }

        @Override
        public boolean awaitUninterruptibly( long timeout, TimeUnit unit )
        {
            return false;
        }

        @Override
        public boolean awaitUninterruptibly( long timeoutMillis )
        {
            return false;
        }

        @Override
        public Void getNow()
        {
            return null;
        }

        @Override
        public boolean cancel( boolean mayInterruptIfRunning )
        {
            return false;
        }
    };

    @Override
    public <T> Attribute<T> attr( AttributeKey<T> key )
    {
        return null;
    }
}