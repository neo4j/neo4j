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
package org.neo4j.backup.impl;

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
    private static final Factory<ChannelBuffer> DEFAULT_CHANNEL_BUFFER_FACTORY = ChannelBuffers::dynamicBuffer;

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
        return future ->
        {
            buffer.clear();
            freeBuffers.offer( buffer );
            BufferReusingChunkingChannelBuffer.super.operationComplete( future );
        };
    }
}
