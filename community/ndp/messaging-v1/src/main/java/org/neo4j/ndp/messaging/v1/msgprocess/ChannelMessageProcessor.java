/**
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
package org.neo4j.ndp.messaging.v1.msgprocess;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.ndp.messaging.v1.MessageFormat;
import org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1;
import org.neo4j.ndp.runtime.Session;
import org.neo4j.ndp.runtime.internal.Neo4jError;
import org.neo4j.packstream.PackStream;

/**
 * This processes incoming messages from a pair of raw I/O {@link java.nio.channels.Channel channels},
 * forwarding requests into a provided {@link org.neo4j.ndp.runtime.Session} and back.
 * <p/>
 * This class creates, implicitly, large input and output buffers (in {@link #reader} and {@link #writer}), meaning
 * you will ideally want to use one of these per session or so, rather than create new instances when new messages
 * arrive
 * from whatever source you are using.
 */
public class ChannelMessageProcessor
{
    private final MessageFormat.Reader reader;
    private final MessageFormat.Writer writer;

    private final AtomicInteger inFlight = new AtomicInteger();
    private final TransportBridge transportBridge;

    private final Log log;
    private final Consumer<Session> onFatalSessionError;

    /**
     * Notify that one (out of potentially many) request has been completed. If this is the last request,
     * this will flush output buffers and call {@link #onAllMessagesCompleted}
     */
    private final Runnable onEachCompletedRequest = new Runnable()
    {
        @Override
        public void run()
        {
            if ( inFlight.decrementAndGet() == 0 )
            {
                try
                {
                    // All requests completed (if this was not 0, there are requests running the
                    // the background, and they will do this once they have completed).
                    writer.flush();
                    onAllMessagesCompleted.run();
                }
                catch ( IOException e )
                {
                    log.error( "Network failure while writing to client", e );
                }
            }
        }
    };

    private Session session;
    private Runnable onAllMessagesCompleted;

    public ChannelMessageProcessor( Log log, Consumer<Session> onFatalSessionError )
    {
        this.log = log;
        this.onFatalSessionError = onFatalSessionError;
        this.transportBridge = new TransportBridge( log );
        reader = new PackStreamMessageFormatV1.Reader();
        writer = new PackStreamMessageFormatV1.Writer();
    }

    public ChannelMessageProcessor reset( ReadableByteChannel in, WritableByteChannel out, Session session )
    {
        this.session = session;
        this.writer.reset( out );
        this.reader.reset( in );
        this.transportBridge.reset( session, writer, onEachCompletedRequest );
        return this;
    }

    public void process( Runnable onAllMessagesCompleted )
    {
        boolean sessionsMustDie = false;
        inFlight.set( 1 );
        try // for IOExceptions
        {
            try // for all other exceptions, the handling of which can only throw IOException
            {
                this.onAllMessagesCompleted = onAllMessagesCompleted;

                while ( reader.hasNext() )
                {
                    inFlight.incrementAndGet();
                    reader.read( transportBridge );
                }
            }
            catch ( PackStream.PackstreamException e )
            {
                sessionsMustDie = true;
                handleRequestFormatError();
            }
            catch ( Throwable e )
            {
                sessionsMustDie = true;
                handleUnexpectedError( session, e );
            }
        }
        catch ( IOException e )
        {
            sessionsMustDie = true;
            log.error( "Network failure while processing messages for '" + session + "'.", e );
        }
        finally
        {
            if ( sessionsMustDie )
            {
                onFatalSessionError.accept( session );
            }
            else
            {
                onEachCompletedRequest.run();
            }
        }
    }

    private void handleRequestFormatError() throws IOException
    {
        // Since the message itself is broken, we do not have a correlation ID
        // to pass back. Therefore, we'll use zero here instead.
        writer.handleFailureMessage( new Neo4jError(
                Status.Request.Invalid,
                "One or more malformed messages were received, please verify that your driver is of " +
                "the latest applicable version and if not, please file a bug report. The session will be " +
                "terminated." ) );
        writer.flush();
    }

    private void handleUnexpectedError( Session session, Throwable e ) throws IOException
    {
        // Since the message itself is broken, we do not have a correlation ID
        // to pass back. Therefore, we'll use zero here instead.
        log.error( "Unexpected error while processing messages for '" + session + "', " +
                   "session will be terminated: " + e.getMessage() + ".", e );
        writer.handleFailureMessage( new Neo4jError(
                Status.General.UnknownFailure,
                e.getMessage() ) );
        writer.flush();
    }

}
