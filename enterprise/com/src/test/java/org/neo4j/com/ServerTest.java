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

import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.time.Clocks;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.com.Protocol.DEFAULT_FRAME_LENGTH;
import static org.neo4j.com.Protocol.EMPTY_SERIALIZER;
import static org.neo4j.com.Protocol.VOID_DESERIALIZER;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class ServerTest
{
    private final Protocol protocol = new Protocol214( 1024, (byte) 0, Server.INTERNAL_PROTOCOL_VERSION );
    private final TxChecksumVerifier checksumVerifier = mock( TxChecksumVerifier.class );
    private final RequestType reqType = mock( RequestType.class );
    private final RecordingChannel channel = new RecordingChannel();

    @Test
    public void shouldSendExceptionBackToClientOnInvalidChecksum() throws Throwable
    {
        // Given
        Server<Object,Object> server = newServer( checksumVerifier );
        RequestContext ctx = new RequestContext( 0, 1, 0, 2, 12 );

        doThrow( new IllegalStateException( "123" ) ).when( checksumVerifier ).assertMatch( anyLong(), anyLong() );

        // When
        try
        {
            server.messageReceived( channelCtx( channel ), message( reqType, ctx, channel, EMPTY_SERIALIZER ) );
            fail( "Should have failed." );
        }
        catch ( IllegalStateException e )
        {
            // Expected
        }

        // Then
        try
        {
            protocol.deserializeResponse( channel.asBlockingReadHandler(), ByteBuffer.allocate( (int) ByteUnit.kibiBytes( 1 ) ), 1,
                    VOID_DESERIALIZER, mock( ResourceReleaser.class ), new VersionAwareLogEntryReader<>() );
            fail( "Should have failed." );
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(), equalTo( "123" ) );
        }
    }

    @Test
    public void shouldNotSendExceptionBackToClientOnInvalidChecksumIfThereAreNoTransactions() throws Throwable
    {
        // Given
        Server<Object,Object> server = newServer( checksumVerifier );
        RequestContext ctx =
                new RequestContext( 0, 1, 0, TransactionIdStore.BASE_TX_ID, TransactionIdStore.BASE_TX_CHECKSUM );

        // When
        server.messageReceived( channelCtx( channel ), message( reqType, ctx, channel, EMPTY_SERIALIZER ) );

        // Then
        verifyZeroInteractions( checksumVerifier );
    }

    private MessageEvent message( RequestType reqType, RequestContext ctx,
            Channel serverToClientChannel, Serializer payloadSerializer ) throws IOException
    {
        ByteBuffer backingBuffer = ByteBuffer.allocate( 1024 );

        protocol.serializeRequest( new RecordingChannel(), new ByteBufferBackedChannelBuffer( backingBuffer ),
                reqType, ctx,
                payloadSerializer );

        MessageEvent event = mock( MessageEvent.class );
        when( event.getMessage() ).thenReturn( new ByteBufferBackedChannelBuffer( backingBuffer ) );
        when( event.getChannel() ).thenReturn( serverToClientChannel );

        return event;
    }

    private ChannelHandlerContext channelCtx( Channel channel )
    {
        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
        when( ctx.getChannel() ).thenReturn( channel );
        return ctx;
    }

    private Server<Object,Object> newServer( final TxChecksumVerifier checksumVerifier )
    {
        Server.Configuration conf = mock( Server.Configuration.class );
        when( conf.getServerAddress() ).thenReturn( new HostnamePort( "aa", -1667 ) );
        Server<Object,Object> server = new Server<Object,Object>( null, conf,
                getInstance(),
                DEFAULT_FRAME_LENGTH,
                new ProtocolVersion( (byte) 0, ProtocolVersion.INTERNAL_PROTOCOL_VERSION ),
                checksumVerifier, Clocks.systemClock(),
                mock( ByteCounterMonitor.class ), mock( RequestMonitor.class ) )
        {
            @Override
            protected RequestType getRequestContext( byte id )
            {
                return mock( RequestType.class );
            }

            @Override
            protected void stopConversation( RequestContext context )
            {
            }
        };
        server.init();
        return server;
    }
}
