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
package org.neo4j.bolt.v1.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltMessageLogger;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.SynchronousBoltWorker;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class BoltMessagingProtocolV1HandlerTest
{
    @Test
    public void shouldNotTalkToChannelDirectlyOnFatalError() throws Throwable
    {
        // Given
        Channel outputChannel = newChannelMock();
        BoltChannel boltChannel = mock( BoltChannel.class );
        when( boltChannel.rawChannel() ).thenReturn( outputChannel );

        BoltStateMachine machine = mock( BoltStateMachine.class );
        BoltMessagingProtocolV1Handler protocol = new BoltMessagingProtocolV1Handler(
                boltChannel, new SynchronousBoltWorker( machine ), NullLogService.getInstance() );
        verify( outputChannel ).alloc();

        // And given inbound data that'll explode when the protocol tries to interpret it
        ByteBuf bomb = mock(ByteBuf.class);
        doThrow( IOException.class ).when( bomb ).readableBytes();

        // When
        protocol.handle( mock(ChannelHandlerContext.class), bomb );

        // Then the protocol should not mess with the channel (because it runs on the IO thread,
        // and only the worker thread should produce writes)
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

        BoltChannel boltChannel = mock( BoltChannel.class );
        when( boltChannel.rawChannel() ).thenReturn( outputChannel );

        BoltMessagingProtocolV1Handler protocol = new BoltMessagingProtocolV1Handler(
                boltChannel, new SynchronousBoltWorker( machine ), NullLogService.getInstance() );
        protocol.close();

        verify( machine ).close();
        verify( buffer ).release();
    }

    @Test
    public void messageProcessingErrorIsLogged() throws IOException
    {
        RuntimeException error = new RuntimeException( "Unexpected error!" );
        ByteBuf data = newThrowingByteBuf( error );

        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        SimpleLogService logService = new SimpleLogService( NullLogProvider.getInstance(), assertableLogProvider );

        Channel outputChannel = newChannelMock();
        BoltChannel boltChannel = mock( BoltChannel.class );
        when( boltChannel.rawChannel() ).thenReturn( outputChannel );

        BoltMessagingProtocolV1Handler protocol = new BoltMessagingProtocolV1Handler(
                boltChannel, mock( BoltWorker.class ), logService );

        protocol.handle( mock( ChannelHandlerContext.class ), data );

        assertableLogProvider.assertExactly(
                inLog( BoltMessagingProtocolV1Handler.class ).error(
                        equalTo( "Failed to handle incoming Bolt message. Connection will be closed." ),
                        equalTo( error ) ) );
    }

    private static ByteBuf newThrowingByteBuf( RuntimeException exceptionToThrow )
    {
        Objects.requireNonNull( exceptionToThrow );
        ByteBuf buf = mock( ByteBuf.class, (Answer<Void>) invocation ->
        {
            throw exceptionToThrow;
        } );
        doReturn( true ).when( buf ).release();
        return buf;
    }

    private static Channel newChannelMock()
    {
        Channel channel = mock( Channel.class );
        ByteBufAllocator allocator = mock( ByteBufAllocator.class, RETURNS_MOCKS );
        when( channel.alloc() ).thenReturn( allocator );
        return channel;
    }
}
