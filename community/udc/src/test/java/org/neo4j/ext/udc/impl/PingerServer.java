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
package org.neo4j.ext.udc.impl;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import java.util.Map;

public class PingerServer implements AutoCloseable
{
    private final Server server;
    private final PingerHandler handler;
    private final ServerConnector connector;

    public PingerServer() throws Exception
    {
        server = new Server();
        handler = new PingerHandler();
        connector = new ServerConnector( server );
        connector.setHost( "127.0.0.1" );
        connector.setPort( 0 );
        server.addConnector( connector );
        server.setHandler( handler );
        server.start();
    }

    public Map<String,String> getQueryMap()
    {
        return handler.getQueryMap();
    }

    public String getHost()
    {
        return connector.getHost();
    }

    public int getPort()
    {
        return connector.getLocalPort();
    }

    @Override
    public void close() throws Exception
    {
        server.stop();
    }
}
