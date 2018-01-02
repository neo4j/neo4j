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
package org.neo4j.backup;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.com.ChunkingChannelBuffer;
import org.neo4j.function.Factory;

/**
 * {@linkplain ChunkingChannelBuffer Chunking buffer} that is able to reuse up to {@link #MAX_WRITE_AHEAD_CHUNKS}
 * netty channel buffers.
 * <p>
 * Buffer is considered to be free when future corresponding to the call {@link Channel#write(Object)} is completed.
 * Argument to {@link Channel#write(Object)} is {@link ChannelBuffer}.
 * Method {@link ChannelFutureListener#operationComplete(ChannelFuture)} is called upon future completion and
 * than {@link ChannelBuffer} is returned to the queue of free buffers.
 * <p>
 * Allocation of buffers is traded for allocation of {@link ChannelFutureListener}s that returned buffers to the
 * queue of free buffers.
 */
class BufferReusingChunkingChannelBuffer extends ChunkingChannelBuffer
{
    private static final Factory<ChannelBuffer> DEFAULT_CHANNEL_BUFFER_FACTORY = new Factory<ChannelBuffer>()
    {
        @Override
        public ChannelBuffer newInstance()
        {
            return ChannelBuffers.dynamicBuffer();
        }
    };

    private final Factory<ChannelBuffer> bufferFactory;
    private final Queue<ChannelBuffer> freeBuffers = new LinkedBlockingQueue<>( MAX_WRITE_AHEAD_CHUNKS );

    BufferReusingChunkingChannelBuffer( ChannelBuffer initialBuffer, Channel channel, int capacity,
            byte internalProtocolVersion, byte applicationProtocolVersion )
    {
        this( initialBuffer, DEFAULT_CHANNEL_BUFFER_FACTORY, channel, capacity, internalProtocolVersion,
                applicationProtocolVersion );
    }

    BufferReusingChunkingChannelBuffer( ChannelBuffer initialBuffer, Factory<ChannelBuffer> bufferFactory,
            Channel channel, int capacity, byte internalProtocolVersion, byte applicationProtocolVersion )
    {
        super( initialBuffer, channel, capacity, internalProtocolVersion, applicationProtocolVersion );
        this.bufferFactory = bufferFactory;
    }

    @Override
    protected ChannelBuffer newChannelBuffer()
    {
        ChannelBuffer buffer = freeBuffers.poll();
        return (buffer == null) ? bufferFactory.newInstance() : buffer;
    }

    @Override
    protected ChannelFutureListener newChannelFutureListener( final ChannelBuffer buffer )
    {
        return new ChannelFutureListener()
        {
            @Override
            public void operationComplete( ChannelFuture future ) throws Exception
            {
                buffer.clear();
                freeBuffers.offer( buffer );
                BufferReusingChunkingChannelBuffer.super.operationComplete( future );
            }
        };
    }
}
