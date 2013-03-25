/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.security;

import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;


public class SslSocketConnectorFactory {

    public ServerConnector createConnector(Server server, KeyStoreInformation config, String host, int port) {

        SslContextFactory sslContextFactory = new SslContextFactory();

        sslContextFactory.setKeyStorePath( config.getKeyStorePath() );
        sslContextFactory.setKeyStorePassword( String.valueOf( config.getKeyStorePassword() ) );
        sslContextFactory.setKeyManagerPassword( String.valueOf( config.getKeyPassword() ) );

        ServerConnector connector = new ServerConnector( server, new SslConnectionFactory( sslContextFactory, HttpVersion.HTTP_1_1.asString() ), new HttpConnectionFactory() );

        connector.setPort( port );
        connector.setHost( host );

        return connector;

    }

}
