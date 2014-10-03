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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class BlockingReadHandler<E> extends ChannelInboundHandlerAdapter
{
    private final BlockingQueue<Object> queue;
    private volatile boolean closed;

    /**
     * Creates a new instance with the default unbounded {@link BlockingQueue}
     * implementation.
     */
    public BlockingReadHandler() {
        this(new LinkedTransferQueue<>());
    }

    /**
     * Creates a new instance with the specified {@link BlockingQueue}.
     */
    public BlockingReadHandler(BlockingQueue<Object> queue) {
        if (queue == null) {
            throw new NullPointerException("queue");
        }
        this.queue = queue;
    }

    /**
     * Returns the queue which stores the received messages.  The default
     * implementation returns the queue which was specified in the constructor.
     */
    protected BlockingQueue<Object> getQueue() {
        return queue;
    }

    public boolean isClosed() {
        return closed;
    }

    public E read() throws IOException, InterruptedException {
        Object e = readEvent();
        if (e == null) {
            return null;
        }

        if(e instanceof Throwable)
        {
            throw new IOException( (Throwable)e );
        }
        else
        {
            return (E)e;
        }
    }

    public E read(long timeout, TimeUnit unit) throws IOException, InterruptedException, BlockingReadTimeoutException
    {
        Object e = readEvent(timeout, unit);
        if (e == null) {
            return null;
        }

        if(e instanceof Throwable)
        {
            throw new IOException( (Throwable)e );
        }
        else
        {
            return (E)e;
        }
    }

    private Object readEvent() throws InterruptedException {
        if (isClosed()) {
            if (getQueue().isEmpty()) {
                return null;
            }
        }

        return getQueue().take();
    }

    private Object readEvent(long timeout, TimeUnit unit) throws InterruptedException, BlockingReadTimeoutException {
        if (isClosed()) {
            if (getQueue().isEmpty()) {
                return null;
            }
        }

        Object e = getQueue().poll(timeout, unit);
        if (e == null) {
            throw new BlockingReadTimeoutException();
        } else {
            return e;
        }
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        getQueue().put( msg );
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
    {
        getQueue().put( cause );
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx ) throws Exception
    {
        closed = true;
        getQueue().put( new ComException( "Connection closed." ) );
    }
}