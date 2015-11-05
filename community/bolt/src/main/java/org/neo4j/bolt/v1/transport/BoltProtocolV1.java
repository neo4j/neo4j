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
package org.neo4j.bolt.v1.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.bolt.transport.BoltProtocol;
import org.neo4j.bolt.v1.messaging.MessageFormat;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.PackStreamMessageFormatV1;
import org.neo4j.bolt.v1.messaging.msgprocess.TransportBridge;
import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.internal.Neo4jError;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import static org.neo4j.bolt.v1.messaging.msgprocess.MessageProcessingCallback.publishError;

/**
 * Implements version one of the Neo4j protocol when transported over a socket. This means this class will handle a
 * simple message framing protocol and forward messages to the messaging protocol implementation, version 1.
 * <p/>
 * Versions of the framing protocol are lock-step with the messaging protocol versioning.
 */
public class BoltProtocolV1 implements BoltProtocol
{
    public static final int VERSION = 1;
    public static final int DEFAULT_BUFFER_SIZE = 8192;

    private final ChunkedOutput output;
    private final MessageFormat.Writer packer;
    private final BoltV1Dechunker dechunker;

    private final Session session;

    private final Log log;
    private final AtomicInteger inFlight = new AtomicInteger( 0 );

    // Remembering that the actual work processing is done in threads separate from the IO threads, this callback gets invoked for each
    // completed request on the worker thread. We use it to ensure that we flush outbound buffers when all in-flight messages for a session have been handled.
    private final Runnable onEachCompletedMessage = new Runnable()
    {
        @Override
        public void run()
        {
            onMessageDone();
        }
    };

    private final Runnable onEachStartedMessage = new Runnable()
    {
        @Override
        public void run()
        {
            onMessageStarted();
        }
    };

    public BoltProtocolV1( final LogService logging, Session session, Channel channel )
    {
        this.log = logging.getInternalLog( getClass() );
        this.session = session;
        this.output = new ChunkedOutput( channel, DEFAULT_BUFFER_SIZE );
        this.packer = new PackStreamMessageFormatV1.Writer( new Neo4jPack.Packer( output ), output );

        this.dechunker = new BoltV1Dechunker( new TransportBridge( log ).reset( session, packer, onEachCompletedMessage ), onEachStartedMessage );
    }

    /**
     * Handle an incoming network packet. We currently deal with the chunked input by building up full messages in
     * RAM before we deserialize them. This is fine with most messages, but will become a problem with very large
     * parameters and so on. The next step will be to write a new protocol V1 deserializer that can do incremental
     * deserialization, see the Netty HTTP parser for an example.
     */
    @Override
    public void handle( ChannelHandlerContext channelContext, ByteBuf data ) throws IOException
    {
        try
        {
            dechunker.handle( data );
        }
        catch ( Throwable e )
        {
            handleUnexpectedError( channelContext, e );
        }
        finally
        {
            data.release();
        }
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    @Override
    public synchronized void close()
    {
        dechunker.close();
        session.close();
        output.close();
    }

    private void handleUnexpectedError( ChannelHandlerContext channelContext, Throwable e )
    {
        try
        {
            try
            {
                // TODO: This is dangerousish, since the worker thread may be writing to the packer at the same time. Better have an approach where we can
                // signal to the worker that we are shutting it down because of this error, and it can signal to the client.
                publishError( packer, Neo4jError.from( e ) );
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
     * Ths methods below are used to track in-flight messages (messages the client has sent us that are waiting to be processed). We use this information
     * to determine when to explicitly flush our output buffers - if there are no more pending messages when a message is done processing, we should flush
     * the buffers for the session.
     */
    private void onMessageStarted()
    {
        inFlight.incrementAndGet();
    }

    private void onMessageDone()
    {
        // If this is the last in-flight message, and we're not in the middle of reading another message over the wire
        // If we are in the middle of a message, we assume there's no need for us to flush partial outbound buffers, we simply
        // wait for more stuff to do to fill the buffers up in order to use network buffers maximally.
        if ( inFlight.decrementAndGet() == 0 && !dechunker.isInMiddleOfAMessage() )
        {
            try
            {
                // Then flush outbound buffers
                packer.flush();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
