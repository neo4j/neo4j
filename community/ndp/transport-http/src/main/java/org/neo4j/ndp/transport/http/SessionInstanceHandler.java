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
package org.neo4j.ndp.transport.http;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import io.undertow.util.StatusCodes;

import org.neo4j.function.Consumer;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.ndp.messaging.v1.msgprocess.ChannelMessageProcessor;
import org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1;
import org.neo4j.ndp.runtime.Session;

/**
 * This is the "primary" http request handler for session management, it handles incoming messages and forwards them
 * to the appropriate session, as well as termination of running sessions.
 */
public class SessionInstanceHandler implements HttpHandler
{
    private final SessionRegistry sessionRegistry;
    private final StringLogger log;

    public SessionInstanceHandler( SessionRegistry sessionRegistry, StringLogger log )
    {
        this.sessionRegistry = sessionRegistry;
        this.log = log;
    }

    @Override
    public void handleRequest( final HttpServerExchange exchange ) throws Exception
    {
        String sessionKey = exchange.getRequestPath().substring( "/session/".length() );
        HttpString method = exchange.getRequestMethod();
        if( method.equals( Methods.POST ) )
        {
            post( exchange, sessionKey );
        }
        else if ( method.equals( Methods.DELETE ) )
        {
            delete( exchange, sessionKey );
        }
        else
        {
            exchange.setResponseCode( StatusCodes.METHOD_NOT_ALLOWED );
        }
    }

    private void post( final HttpServerExchange exchange, String sessionKey )
    {
        SessionAcquisition acquisition = sessionRegistry.acquire( sessionKey );
        if ( acquisition.success() )
        {
            exchange.setResponseCode( StatusCodes.OK );
            exchange.getResponseHeaders().add( Headers.CONTENT_TYPE, PackStreamMessageFormatV1.CONTENT_TYPE );

            processorForSession( exchange, acquisition.session() )
                    .process( onAllMessagesProcessed( exchange, acquisition.session() ) );
        }
        else if ( acquisition.sessionExists() )
        {
            exchange.setResponseCode( StatusCodes.CONFLICT );
            exchange.endExchange();
        }
        else
        {
            exchange.setResponseCode( StatusCodes.NOT_FOUND );
            exchange.endExchange();
        }
    }

    private void delete( HttpServerExchange exchange, String sessionKey )
    {
        SessionAcquisition acquisition = sessionRegistry.acquire( sessionKey );
        try
        {
            if ( acquisition.success() )
            {
                exchange.setResponseCode( StatusCodes.OK );
                sessionRegistry.destroy( sessionKey );
            }
            else if ( acquisition.sessionExists() )
            {
                exchange.setResponseCode( StatusCodes.CONFLICT );
            }
            else
            {
                exchange.setResponseCode( StatusCodes.NOT_FOUND );
            }
        }
        catch(Throwable e)
        {
            e.printStackTrace(); // TODO
        }
        finally
        {
            exchange.endExchange();
        }
    }

    private ChannelMessageProcessor processorForSession( HttpServerExchange exchange, Session session )
    {
        return new ChannelMessageProcessor(log, new Consumer<Session>()
        {
            @Override
            public void accept( Session session )
            {
                sessionRegistry.destroy( session.key() );
            }
        }).reset( exchange.getRequestChannel(), exchange.getResponseChannel(), session );
    }

    private Runnable onAllMessagesProcessed( final HttpServerExchange exchange, final Session session )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                sessionRegistry.release( session.key() );
                exchange.endExchange();
            }
        };
    }

}
