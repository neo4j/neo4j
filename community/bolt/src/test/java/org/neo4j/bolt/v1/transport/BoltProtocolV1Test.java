/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.SynchronousBoltWorker;
import org.neo4j.kernel.impl.logging.NullLogService;

import java.io.IOException;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class BoltProtocolV1Test
{
    @Test
    public void shouldNotTalkToChannelDirectlyOnFatalError() throws Throwable
    {
        // Given
        Channel outputChannel = mock( Channel.class );
        ByteBufAllocator allocator = mock( ByteBufAllocator.class, RETURNS_MOCKS );
        when( outputChannel.alloc() ).thenReturn( allocator );

        BoltStateMachine machine = mock( BoltStateMachine.class );
        BoltProtocolV1 protocol = new BoltProtocolV1( new SynchronousBoltWorker( machine ),
                outputChannel, NullLogService.getInstance() );
        verify( outputChannel ).alloc();

        // And given inbound data that'll explode when the protocol tries to interpret it
        ByteBuf bomb = mock(ByteBuf.class);
        doThrow( IOException.class ).when( bomb ).readableBytes();

        // When
        protocol.handle( mock(ChannelHandlerContext.class), bomb );

        // Then the protocol should not mess with the channel (because it runs on the IO thread, and only the worker thread should produce writes)
        verifyNoMoreInteractions( outputChannel );

        // But instead make sure the state machine is shut down
        verify( machine ).close();
    }

    @Test
    public void closesInputAndOutput()
    {
        Channel outputChannel = mock( Channel.class );
        ByteBufAllocator allocator = mock( ByteBufAllocator.class );
        ByteBuf buffer = mock( ByteBuf.class );
        when( outputChannel.alloc() ).thenReturn( allocator );
        when( allocator.buffer( anyInt(), anyInt() ) ).thenReturn( buffer );

        BoltStateMachine machine = mock( BoltStateMachine.class );

        BoltProtocolV1 protocol = new BoltProtocolV1( new SynchronousBoltWorker( machine ),
                outputChannel, NullLogService.getInstance() );
        protocol.close();

        verify( machine ).close();
        verify( buffer ).release();
    }
}
