/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.transport.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.transport.BoltProtocolPipelineInstaller;
import org.neo4j.bolt.transport.BoltProtocolPipelineInstallerFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class ProtocolHandshaker extends ChannelInboundHandlerAdapter
{
    public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;
    private static final int HANDSHAKE_BUFFER_SIZE = 5 * Integer.BYTES;

    private final BoltChannel boltChannel;
    private final BoltProtocolPipelineInstallerFactory handlerFactory;
    private final Log log;
    private final boolean encryptionRequired;
    private final boolean encrypted;

    private ByteBuf handshakeBuffer;
    private BoltProtocolPipelineInstaller protocol;

    public ProtocolHandshaker( BoltProtocolPipelineInstallerFactory handlerFactory, BoltChannel boltChannel, LogProvider logging, boolean encryptionRequired,
            boolean encrypted )
    {
        this.handlerFactory = handlerFactory;
        this.boltChannel = boltChannel;
        this.log = logging.getLog( getClass() );
        this.encryptionRequired = encryptionRequired;
        this.encrypted = encrypted;
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx )
    {
        handshakeBuffer = ctx.alloc().buffer( HANDSHAKE_BUFFER_SIZE, HANDSHAKE_BUFFER_SIZE );
    }

    @Override
    public void handlerRemoved( ChannelHandlerContext ctx )
    {
        handshakeBuffer.release();
        handshakeBuffer = null;
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg )
    {
        if ( !(msg instanceof ByteBuf) )
        {
            // we know it is HTTP as we only have HTTP (for Websocket) and TCP handlers installed.
            log.warn( "Unsupported connection type: 'HTTP'. Bolt protocol only operates over a TCP connection or WebSocket." );
            ctx.close();
            return;
        }
        ByteBuf buf = (ByteBuf) msg;

        try
        {
            assertEncryptedIfRequired();

            // try to fill out handshake buffer
            handshakeBuffer.writeBytes( buf, Math.min( buf.readableBytes(), handshakeBuffer.writableBytes() ) );

            // we filled up the handshake buffer
            if ( handshakeBuffer.writableBytes() == 0 )
            {
                if ( verifyBoltPreamble() )
                {
                    // let's handshake
                    if ( performHandshake() )
                    {
                        // announce selected protocol to the client
                        ctx.writeAndFlush( ctx.alloc().buffer( 4 ).writeInt( (int)protocol.version() ) );

                        // install related protocol handlers into the pipeline
                        protocol.install();
                        ctx.pipeline().remove( this );

                        // if we somehow end up with more data in the incoming buffers, let's send them
                        // down to the pipeline for the chosen protocol handlers to handle whatever they
                        // are.
                        if ( buf.readableBytes() > 0 )
                        {
                            ctx.fireChannelRead( buf.readRetainedSlice( buf.readableBytes() ) );
                        }
                    }
                    else
                    {
                        ctx.writeAndFlush( ctx.alloc().buffer().writeBytes( new byte[]{0, 0, 0, 0} ) ).addListener( ChannelFutureListener.CLOSE );
                    }
                }
                else
                {
                    ctx.close();
                }
            }
        }
        finally
        {
            buf.release();
        }
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
    {
        // log insecure handshake to the bolt message log
        if ( cause instanceof SecurityException )
        {
            boltChannel.log().serverError( "HANDSHAKE", "Insecure handshake" );
        }

        log.error( "Fatal error occurred during protocol handshaking: " + ctx.channel(), cause );
        ctx.close();
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx )
    {
        ctx.close();
    }

    private void assertEncryptedIfRequired()
    {
        if ( encryptionRequired && !encrypted )
        {
            throw new SecurityException( "An unencrypted connection attempt was made where encryption is required." );
        }
    }

    private boolean verifyBoltPreamble()
    {
        if ( handshakeBuffer.getInt( 0 ) != BOLT_MAGIC_PREAMBLE )
        {
            boltChannel.log().clientError( "HANDSHAKE", "Invalid Bolt signature", () -> format( "0x%08X", handshakeBuffer.getInt( 0 ) ) );

            return false;
        }

        return true;
    }

    private boolean performHandshake()
    {
        boltChannel.log().clientEvent( "HANDSHAKE", () -> format( "0x%08X", BOLT_MAGIC_PREAMBLE ) );

        for ( int i = 0; i < 4; i++ )
        {
            final long suggestion = handshakeBuffer.getInt( (i + 1) * Integer.BYTES ) & 0xFFFFFFFFL;

            protocol = handlerFactory.create( suggestion, boltChannel );
            if ( protocol != null )
            {
                boltChannel.log().serverEvent( "HANDSHAKE", () -> format( "0x%02X", suggestion ) );

                break;
            }
        }

        if ( protocol == null )
        {
            boltChannel.log().serverError( "HANDSHAKE", "No applicable protocol" );
        }

        return protocol != null;
    }
}
