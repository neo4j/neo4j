/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.handler.queue.BlockingReadHandler;

import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class RecordingChannel implements Channel
{
    private Queue<ChannelBuffer> recievedMessages = new LinkedList<>();

    @Override
    public ChannelFuture write( Object message )
    {
        if ( message instanceof ChannelBuffer )
        {
            ChannelBuffer buffer = (ChannelBuffer) message;
            recievedMessages.offer( buffer.duplicate() );
        }
        return immediateFuture;
    }

    @Override
    public ChannelFuture write( Object message, SocketAddress remoteAddress )
    {
        write(message);
        return immediateFuture;
    }

    @Override
    public Integer getId()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public ChannelFactory getFactory()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public Channel getParent()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public ChannelConfig getConfig()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public ChannelPipeline getPipeline()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public boolean isBound()
    {
        return true;
    }

    @Override
    public boolean isConnected()
    {
        return true;
    }

    @Override
    public SocketAddress getLocalAddress()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public SocketAddress getRemoteAddress()
    {
        throw new UnsupportedOperationException(  );
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
    public ChannelFuture disconnect()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public ChannelFuture unbind()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public ChannelFuture close()
    {
        return null;
    }

    @Override
    public ChannelFuture getCloseFuture()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public int getInterestOps()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public boolean isReadable()
    {
        return false;
    }

    @Override
    public boolean isWritable()
    {
        return true;
    }

    @Override
    public ChannelFuture setInterestOps( int interestOps )
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public ChannelFuture setReadable( boolean readable )
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public Object getAttachment()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public void setAttachment( Object attachment )
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public int compareTo( Channel o )
    {
        return 0;
    }

    // This is due to a tight coupling of the netty pipeline and message deserialization, we can't deserialize without
    // this pipeline item yet. We should refactor the serialization/deserialzation code appropriately such that it is
    // not tied like this to components it should not be aware of.
    public BlockingReadHandler<ChannelBuffer> asBlockingReadHandler()
    {
        return new BlockingReadHandler<ChannelBuffer>()
        {
            @Override
            public ChannelBuffer read()
            {
                return recievedMessages.poll();
            }

            @Override
            public ChannelBuffer read( long timeout, TimeUnit unit )
            {
                return read();
            }
        };
    }

    private ChannelFuture immediateFuture = new ChannelFuture()
    {
        @Override
        public Channel getChannel()
        {
            return RecordingChannel.this;
        }

        @Override
        public boolean isDone()
        {
            return true;
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
        public Throwable getCause()
        {
            return null;
        }

        @Override
        public boolean cancel()
        {
            return false;
        }

        @Override
        public boolean setSuccess()
        {
            return true;
        }

        @Override
        public boolean setFailure( Throwable cause )
        {
            return false;
        }

        @Override
        public boolean setProgress( long amount, long current, long total )
        {
            return false;
        }

        @Override
        public void addListener( ChannelFutureListener listener )
        {
            try
            {
                listener.operationComplete( this );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void removeListener( ChannelFutureListener listener )
        {
        }

        @Override
        public ChannelFuture rethrowIfFailed()
        {
            return null;
        }

        @Override
        public ChannelFuture sync()
        {
            return null;
        }

        @Override
        public ChannelFuture syncUninterruptibly()
        {
            return null;
        }

        @Override
        public ChannelFuture await()
        {
            return null;
        }

        @Override
        public ChannelFuture awaitUninterruptibly()
        {
            return null;
        }

        @Override
        public boolean await( long timeout, TimeUnit unit )
        {
            return false;
        }

        @Override
        public boolean await( long timeoutMillis )
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
    };
}
