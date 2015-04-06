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

import io.undertow.Undertow;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static io.undertow.Handlers.path;

public class HttpTransport extends LifecycleAdapter
{

    private final String host;
    private final int port;
    private final SessionRegistry sessionRegistry;
    private final StringLogger log;
    private Undertow server;

    public HttpTransport( String host, int port, SessionRegistry sessionRegistry, StringLogger log )
    {
        this.host = host;
        this.port = port;
        this.sessionRegistry = sessionRegistry;
        this.log = log;
    }

    @Override
    public void init() throws Throwable
    {
        server = Undertow.builder()
                .addHttpListener( port, host )
                .setHandler( path()
                    .addExactPath( "/session/",  new SessionCollectionHandler( sessionRegistry ) )
                    .addPrefixPath( "/session/", new SessionInstanceHandler( sessionRegistry, log ) )
                )
                .build();
    }

    @Override
    public void start() throws Throwable
    {
        server.start();
    }

    @Override
    public void stop() throws Throwable
    {
        server.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        server = null;
    }

    public String baseURL()
    {
        return "http://" + host + ":" + port;
    }

}
