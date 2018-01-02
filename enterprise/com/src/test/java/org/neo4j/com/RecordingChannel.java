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
package org.neo4j.com;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.handler.queue.BlockingReadHandler;

public class RecordingChannel implements Channel
{
    private Queue<ChannelBuffer> recievedMessages = new LinkedList<ChannelBuffer>();

    @Override
    public ChannelFuture write( Object message )
    {
        if(message instanceof ChannelBuffer )
        {
            ChannelBuffer buffer = (ChannelBuffer)message;
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
            public ChannelBuffer read() throws IOException, InterruptedException
            {
                return recievedMessages.poll();
            }

            @Override
            public ChannelBuffer read( long timeout, TimeUnit unit ) throws IOException, InterruptedException
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
        public ChannelFuture rethrowIfFailed() throws Exception
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
    };
}
