/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal.connector.http;

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.message.BasicRequestLine;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.internal.messaging.AckFailureMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.internal.spi.StreamCollector;

import static org.neo4j.driver.internal.messaging.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.PullAllMessage.PULL_ALL;

public class HttpConnection implements Connection
{
    private final LinkedList<Message> pendingMessages = new LinkedList<>();
    private final MessageFormat.Reader reader;

    private final HttpClient http;
    private final Neo4jMessagesEntity requestEntity;
    private final HttpResponseHandler responseHandler = new HttpResponseHandler();

    private BasicRequestLine sessionResource;
    private int requestCounter = 0;

    public HttpConnection( URI sessionURL, Logger logger, int defaultPort )
    {
        this( sessionURL, logger, defaultPort, new DefaultBHttpClientConnection( 8 * 1024 ) );
    }

    public HttpConnection( URI sessionURL, Logger logger, int defaultPort, DefaultBHttpClientConnection conn )
    {
        this.http = new HttpClient( sessionURL, logger, conn, defaultPort );

        MessageFormat format = new PackStreamMessageFormatV1();
        this.reader = format.newReader();
        this.requestEntity = new Neo4jMessagesEntity( format );

        createSession();
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, StreamCollector collector )
    {
        int messageId = queueMessage( new RunMessage( statement, parameters ) );
        if ( collector != null )
        {
            responseHandler.registerResultCollector( messageId, collector );
        }
    }

    @Override
    public void discardAll()
    {
        queueMessage( DISCARD_ALL );
    }

    @Override
    public void pullAll( StreamCollector collector )
    {
        int messageId = queueMessage( PULL_ALL );
        responseHandler.registerResultCollector( messageId, collector );
    }

    @Override
    public void sync()
    {
        if ( pendingMessages.size() == 0 )
        {
            return;
        }

        HttpResponse res = http.send( sessionResource, requestEntity.reset( pendingMessages ) );
        requestCounter = 0; // Reset once we've sent all pending request to avoid wrap-around handling

        ensureValidStatusCode( res );

        try
        {
            reader.reset( res.getEntity().getContent() );

            while ( reader.hasNext() )
            {
                reader.read( responseHandler );
            }

            // Handle failures that the database reported back to us
            if ( responseHandler.serverFailureOccurred() )
            {
                // Its enough to simply add the ack message to the outbound queue, it'll get sent
                // off as the first message the next time we need to sync with the database.
                queueMessage( new AckFailureMessage() );

                throw responseHandler.serverFailure();
            }
        }
        catch ( IOException e )
        {
            pendingMessages.clear();
            String message = e.getMessage();
            if ( message == null )
            {
                throw new ClientException( "Unable to read response from network: " +
                                           e.getClass().getSimpleName(), e );
            }
            else
            {
                throw new ClientException( "Unable to read response from network: " + message, e );
            }
        }
        finally
        {
            responseHandler.clear();
        }
    }

    private int queueMessage( Message msg )
    {
        int messageId = nextRequestId();
        pendingMessages.add( msg );
        return messageId;
    }

    private void ensureValidStatusCode( HttpResponse res )
    {
        StatusLine status = res.getStatusLine();
        int statusCode = status.getStatusCode();
        switch ( statusCode / 100 )
        {
        case 1:
        case 3:
            throw new ClientException( "Server requested an unexpected action, " +
                                       "don't know how to handle it: '" + status.getReasonPhrase() + "'." );
        case 4:
            if ( statusCode == 404 )
            {
                throw new ClientException(
                        "Session is no longer available. This is most often caused by the session timing " +
                        "out due to idleness, but can also be caused by a previous fatal error." );
            }
            else
            {
                throw new ClientException( "A fatal transport error occurred: '" + status.getReasonPhrase() + "', " +
                                           "please refer to the database logs for details." );
            }
        case 5:
            throw new DatabaseException( "A fatal transport error occurred: '" + status.getReasonPhrase() + "', " +
                                         "please refer to the database logs for details." );
        }
    }

    @Override
    public void close()
    {
        http.send( new BasicRequestLine( "DELETE", sessionResource.getUri(), HttpVersion.HTTP_1_1 ) );
    }

    private void createSession()
    {
        HttpResponse req = http.send( new BasicRequestLine( "POST", "/session/", HttpVersion.HTTP_1_1 ) );
        String sessionPath = req.getFirstHeader( "Location" ).getValue();
        sessionResource = new BasicRequestLine( "POST", sessionPath, HttpVersion.HTTP_1_1 );
    }

    private int nextRequestId()
    {
        return (requestCounter++);
    }
}
