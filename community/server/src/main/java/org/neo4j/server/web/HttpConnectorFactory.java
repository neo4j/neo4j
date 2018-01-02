/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.web;

import java.util.Arrays;

import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;

public class HttpConnectorFactory
{
    private Config configuration;

    public HttpConnectorFactory( Config config )
    {

        this.configuration = config;
    }

    public ConnectionFactory createHttpConnectionFactory()
    {
        return new HttpConnectionFactory( createHttpConfig() );
    }

    protected HttpConfiguration createHttpConfig()
    {
        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setRequestHeaderSize( configuration.get( ServerSettings.maximum_request_header_size) );
        httpConfig.setResponseHeaderSize( configuration.get( ServerSettings.maximum_response_header_size) );
        return httpConfig;
    }

    public ServerConnector createConnector( Server server, String host, int port, JettyThreadCalculator jettyThreadCalculator )
    {
        ConnectionFactory httpFactory = createHttpConnectionFactory();
        return createConnector(server, host, port, jettyThreadCalculator, httpFactory );
    }

    public ServerConnector createConnector( Server server, String host, int port, JettyThreadCalculator jettyThreadCalculator, ConnectionFactory... httpFactories )
    {
        int acceptors = jettyThreadCalculator.getAcceptors();
        int selectors = jettyThreadCalculator.getSelectors();

        ServerConnector connector = new ServerConnector( server , null, null, null, acceptors, selectors, httpFactories );

        connector.setConnectionFactories( Arrays.asList( httpFactories ) );

        // TCP backlog, per socket, 50 is the default, consider adapting if needed
        connector.setAcceptQueueSize( 50 );

        connector.setHost( host );
        connector.setPort( port );

        return connector;
    }
}
