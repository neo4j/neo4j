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

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.URI;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.spi.Logger;

/** Thin layer on top of the apache http client. Single threaded, single socket. */
public class HttpClient
{
    private final Logger logger;

    private final HttpRequestExecutor httpexecutor = new HttpRequestExecutor();
    private final HttpProcessor httpproc = HttpProcessorBuilder.create()
            .add( new RequestContent() )
            .add( new RequestTargetHost() )
            .add( new RequestUserAgent( HttpConnector.USER_AGENT ) ).build();
    private final HttpCoreContext coreContext = HttpCoreContext.create();
    private final DefaultBHttpClientConnection conn;
    private final ConnectionReuseStrategy connStrategy = DefaultConnectionReuseStrategy.INSTANCE;
    private final HttpHost host;
    private final int port;

    public HttpClient( URI sessionURL, Logger logger, DefaultBHttpClientConnection conn, int defaultPort )
    {
        this.logger = logger;
        this.conn = conn;

        this.port = sessionURL.getPort() == -1 ? defaultPort : sessionURL.getPort();
        this.host = new HttpHost( sessionURL.getHost(), port );
        this.coreContext.setTargetHost( host );
    }

    public HttpResponse send( RequestLine to )
    {
        return send( to, null );
    }

    public HttpResponse send( RequestLine to, HttpEntity entity )
    {
        try
        {
            if ( !conn.isOpen() )
            {
                conn.bind( new Socket( host.getHostName(), host.getPort() ) );
            }

            HttpEntityEnclosingRequest req = new BasicHttpEntityEnclosingRequest( to );
            if ( entity != null )
            {
                req.setEntity( entity );
            }

            httpexecutor.preProcess( req, httpproc, coreContext );
            HttpResponse response = httpexecutor.execute( req, conn, coreContext );
            httpexecutor.postProcess( response, httpproc, coreContext );

            if ( !connStrategy.keepAlive( response, coreContext ) )
            {
                conn.close();
            }

            return response;
        }
        catch ( ConnectException e )
        {
            throw new ClientException( String.format( "Unable to connect to '%s' on port %s, " +
                                                      "ensure the database is running and that there is a working " +
                                                      "network " +
                                                      "connection to it.", host.getHostName(), port ) );
        }
        catch ( HttpException | IOException e )
        {
            throw new ClientException( "Unable to process request: " + e.getMessage(), e );
        }
    }
}
