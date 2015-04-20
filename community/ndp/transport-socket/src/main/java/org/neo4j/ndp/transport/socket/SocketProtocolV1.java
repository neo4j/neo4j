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
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.ndp.messaging.v1.MessageFormat;
import org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1;
import org.neo4j.ndp.messaging.v1.msgprocess.TransportBridge;
import org.neo4j.ndp.runtime.Session;
import org.neo4j.ndp.runtime.internal.Neo4jError;

/**
 * Implements version one of the Neo4j protocol when transported over a socket. This means this class will handle a
 * simple message framing protocol and forward messages to the messaging protocol implementation, version 1.
 * <p/>
 * Versions of the framing protocol are lock-step with the messaging protocol versioning.
 */
public class SocketProtocolV1 implements SocketProtocol
{
    public static final int VERSION = 1;

    private final ChunkedInput input;
    private final ChunkedOutput output;

    private final MessageFormat.Reader unpacker;
    private final MessageFormat.Writer packer;

    private final TransportBridge bridge;
    private final Session session;
    private final Log log;
    private final AtomicInteger inFlight = new AtomicInteger( 0 );

    public SocketProtocolV1( final Log log, Session session )
    {
        this.log = log;
        this.session = session;
        this.output = new ChunkedOutput();
        this.input = new ChunkedInput();
        this.packer = new PackStreamMessageFormatV1.Writer( output, output.messageBoundaryHook() );
        this.unpacker = new PackStreamMessageFormatV1.Reader( input );
        this.bridge = new TransportBridge( log ).reset( session, packer, new Runnable()
        {
            @Override
            public void run()
            {
                onMessagDone();
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
        while ( data.readableBytes() > 0 )
        {
            int chunkSize = data.readUnsignedShort();
            if ( chunkSize > 0 )
            {
                if ( chunkSize <= data.readableBytes() )
                {
                    // Incoming buffer contains the whole chunk, forward it to our chunked input handling
                    input.addChunk( data.readSlice( chunkSize ) );
                }
                else
                {
                    throw new UnsupportedOperationException( "Chunks split across packets not yet supported" ); // TODO
                }
            }
            else
            {
                processChunkedMessage( channelContext );
            }
        }
        onBatchOfMessagesDone();
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    public void processChunkedMessage( final ChannelHandlerContext channelContext )
    {
        output.setTargetChannel( channelContext );
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
        log.error( String.format( "Session %s: Unexpected error while processing message. Session will be " +
                                  "terminated: %s", session.key(), e.getMessage() ), e );
        try
        {
            try
            {
                packer.handleFailureMessage( new Neo4jError( // TODO Map kernelException causes
                        Status.General.UnknownFailure,
                        e.getMessage() ) );
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
            session.close();
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
        onMessagDone();
    }

    private void onMessageStarted()
    {
        inFlight.incrementAndGet();
    }

    private void onMessagDone()
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
