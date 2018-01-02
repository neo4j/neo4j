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
import org.junit.Test;

import org.neo4j.function.Factory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BufferReusingChunkingChannelBufferTest
{
    @Test
    @SuppressWarnings( "unchecked" )
    public void newBuffersAreCreatedIfNoFreeBuffersAreAvailable()
    {
        CountingChannelBufferFactory bufferFactory = new CountingChannelBufferFactory();
        BufferReusingChunkingChannelBuffer buffer = newBufferReusingChunkingChannelBuffer( 10, bufferFactory );

        buffer.writeLong( 1 );
        buffer.writeLong( 2 );
        buffer.writeLong( 3 );

        assertEquals( 3, bufferFactory.instancesCreated );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void freeBuffersAreReused() throws Exception
    {
        CountingChannelBufferFactory bufferFactory = new CountingChannelBufferFactory();
        BufferReusingChunkingChannelBuffer buffer = newBufferReusingChunkingChannelBuffer( 10, bufferFactory );

        buffer.writeLong( 1 );
        buffer.writeLong( 2 );

        // return 2 buffers to the pool
        ChannelBuffer reusedBuffer1 = triggerOperationCompleteCallback( buffer );
        ChannelBuffer reusedBuffer2 = triggerOperationCompleteCallback( buffer );

        buffer.writeLong( 3 );
        buffer.writeLong( 4 );

        // 2 buffers were created
        assertEquals( 2, bufferFactory.instancesCreated );

        // and 2 buffers were reused
        verify( reusedBuffer1 ).writeLong( 3 );
        verify( reusedBuffer2 ).writeLong( 4 );
    }

    private static BufferReusingChunkingChannelBuffer newBufferReusingChunkingChannelBuffer( int capacity,
            CountingChannelBufferFactory bufferFactory )
    {
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();

        Channel channel = mock( Channel.class );
        ChannelFuture channelFuture = mock( ChannelFuture.class );
        when( channel.isOpen() ).thenReturn( true );
        when( channel.isConnected() ).thenReturn( true );
        when( channel.isBound() ).thenReturn( true );
        when( channel.write( anyObject() ) ).thenReturn( channelFuture );

        return new BufferReusingChunkingChannelBuffer( buffer, bufferFactory, channel, capacity, (byte) 1, (byte) 1 );
    }

    private static ChannelBuffer triggerOperationCompleteCallback( BufferReusingChunkingChannelBuffer buffer )
            throws Exception
    {
        ChannelBuffer reusedBuffer = spy( ChannelBuffers.dynamicBuffer() );

        ChannelFuture channelFuture = mock( ChannelFuture.class );
        when( channelFuture.isDone() ).thenReturn( true );
        when( channelFuture.isSuccess() ).thenReturn( true );

        buffer.newChannelFutureListener( reusedBuffer ).operationComplete( channelFuture );
        return reusedBuffer;
    }

    private static class CountingChannelBufferFactory implements Factory<ChannelBuffer>
    {
        int instancesCreated;

        @Override
        public ChannelBuffer newInstance()
        {
            instancesCreated++;
            return ChannelBuffers.dynamicBuffer();
        }
    }
}
