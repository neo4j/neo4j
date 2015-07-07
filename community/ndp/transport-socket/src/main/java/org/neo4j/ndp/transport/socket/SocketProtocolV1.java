/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.transport.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.ndp.messaging.v1.MessageFormat;
import org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1;
import org.neo4j.ndp.messaging.v1.msgprocess.TransportBridge;
import org.neo4j.ndp.runtime.Session;
import org.neo4j.ndp.runtime.internal.ErrorTranslator;
import org.neo4j.packstream.PackStream;

/**
 * Implements version one of the Neo4j protocol when transported over a socket. This means this class will handle a
 * simple message framing protocol and forward messages to the messaging protocol implementation, version 1.
 * <p/>
 * Versions of the framing protocol are lock-step with the messaging protocol versioning.
 */
public class SocketProtocolV1 implements SocketProtocol
{
    public static final int VERSION = 1;
    public static final int DEFAULT_BUFFER_SIZE = 8192;

    private final ChunkedInput input;
    private final ChunkedOutput output;

    private final MessageFormat.Reader unpacker;
    private final MessageFormat.Writer packer;

    private final TransportBridge bridge;
    private final Session session;

    private final Log log;
    private final AtomicInteger inFlight = new AtomicInteger( 0 );
    private final ErrorTranslator errorTranslator;

    public enum State
    {
        AWAITING_CHUNK,
        IN_CHUNK,
        IN_HEADER,
        CLOSED
    }

    private State state = State.AWAITING_CHUNK;
    private int chunkSize = 0;

    public SocketProtocolV1( final LogService logging, Session session, Channel channel )
    {
        this.log = logging.getInternalLog( getClass() );
        this.session = session;
        this.errorTranslator = new ErrorTranslator( logging );
        this.output = new ChunkedOutput( channel, DEFAULT_BUFFER_SIZE );
        this.input = new ChunkedInput();
        this.packer = new PackStreamMessageFormatV1.Writer( new PackStream.Packer( output ), output.messageBoundaryHook() );
        this.unpacker = new PackStreamMessageFormatV1.Reader( new PackStream.Unpacker( input ) );
        this.bridge = new TransportBridge( log ).reset( session, packer, new Runnable()
        {
            @Override
            public void run()
            {
                onMessageDone();
            }
        } );
    }

    /**
     * Handle an incoming network packet. We currently deal with the chunked input by building up full messages in
     * RAM before we deserialize them. This is fine with most messages, but will become a problem with very large
     * parameters and so on. The next step will be to write a new protocol V1 deserializer that can do incremental
     * deserialization, see the Netty HTTP parser for an example.
     */
    @Override
    public void handle( ChannelHandlerContext channelContext, ByteBuf data )
    {
        onBatchOfMessagesStarted();
        try
        {
            while ( data.readableBytes() > 0 )
            {
                switch ( state )
                {
                case AWAITING_CHUNK:
                {
                    if ( data.readableBytes() >= 2 )
                    {
                        // Whole header available, read that
                        chunkSize = data.readUnsignedShort();
                        handleHeader( channelContext );
                    }
                    else
                    {
                        // Only one byte available, read that and wait for the second byte
                        chunkSize = data.readByte() << 8;
                        state = State.IN_HEADER;
                    }
                    break;
                }
                case IN_HEADER:
                {
                    // First header byte read, now we read the next one
                    chunkSize = (chunkSize | data.readByte()) & 0xFFFF;
                    handleHeader( channelContext );
                    break;
                }
                case IN_CHUNK:
                {
                    if ( chunkSize < data.readableBytes() )
                    {
                        // Current packet is larger than current chunk, slice of the chunk
                        input.append( data.readSlice( chunkSize ) );
                        state = State.AWAITING_CHUNK;
                    }
                    else if ( chunkSize == data.readableBytes() )
                    {
                        // Current packet perfectly maps to current chunk
                        input.append( data );
                        state = State.AWAITING_CHUNK;
                        return;
                    }
                    else
                    {
                        // Current packet is smaller than the chunk we're reading, split the current chunk itself up
                        chunkSize -= data.readableBytes();
                        input.append( data );
                        return;
                    }
                    break;
                }
                case CLOSED:
                {
                    // No-op
                    return;
                }
                }
            }
        }
        finally
        {
            data.release();
            onBatchOfMessagesDone();
        }
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    @Override
    public void close()
    {
        state = State.CLOSED;
        input.close();
        session.close();
        output.close();
    }

    public State state()
    {
        return state;
    }

    private void handleHeader( ChannelHandlerContext channelContext )
    {
        if(chunkSize == 0)
        {
            // Message boundary
            processCollectedMessage( channelContext );
            state = State.AWAITING_CHUNK;
        }
        else
        {
            state = State.IN_CHUNK;
        }
    }

    private void processCollectedMessage( final ChannelHandlerContext channelContext )
    {
        try
        {
            onMessageStarted();
            unpacker.read( bridge );
            // onMessageDone() called via request completion callback in TransportBridge
        }
        catch ( Throwable e )
        {
            handleUnexpectedError( channelContext, e );
        }
        finally
        {
            input.clear();
        }
    }

    private void handleUnexpectedError( ChannelHandlerContext channelContext, Throwable e )
    {
        try
        {
            try
            {
                packer.handleFailureMessage( errorTranslator.translate( e ) );
                packer.flush();
            }
            catch ( Throwable e1 )
            {
                log.error( String.format( "Session %s: Secondary error while notifying client of problem: %s",
                        session.key(), e.getMessage() ), e );
            }
            finally
            {
                channelContext.close();
            }
        }
        finally
        {
            close();
        }
    }

    /*
     * The methods below are used to determine the when to flush our output buffers explicitly. The
     * buffers are implicitly flushed when they fill up, so what the code below deals with is detecting when
     * messaging has "stalled", meaning we're not processing anything and there's no new messages waiting from the
     * client. Since the session backend is allowed to be async, this is a bit tricky.
     *
     * We use a single atomic integer to track both the number of in-flight requests,
     * as well as the "batch of messages" that the user sent, tracked as if it
     * were a single pending request. Doing it this way makes the concurrency edge cases much simpler,
     * we just increment the counter when we receive a network packet from the user, and for each message we
     * start processing. We decrement it for each message we're done with and when we're done with a network
     * packet.
     *
     * Whenever the counter hits 0, we know no processing is taking place, and thus that we should flush.
     */

    public void onBatchOfMessagesStarted()
    {
        onMessageStarted();
    }

    public void onBatchOfMessagesDone()
    {
        onMessageDone();
    }

    private void onMessageStarted()
    {
        inFlight.incrementAndGet();
    }

    private void onMessageDone()
    {
        if ( inFlight.decrementAndGet() == 0 )
        {
            try
            {
                packer.flush();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
