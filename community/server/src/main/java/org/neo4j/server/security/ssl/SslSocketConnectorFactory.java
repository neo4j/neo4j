/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.security.ssl;

import org.eclipse.jetty.http.HttpScheme;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import org.neo4j.bolt.security.ssl.KeyStoreInformation;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.web.HttpConnectorFactory;
import org.neo4j.server.web.JettyThreadCalculator;


public class SslSocketConnectorFactory extends HttpConnectorFactory
{
    public SslSocketConnectorFactory( Config configuration )
    {
        super(configuration);
    }

    @Override
    protected HttpConfiguration createHttpConfig()
    {
        HttpConfiguration httpConfig = super.createHttpConfig();
        httpConfig.addCustomizer(
                ( connector, channelConfig, request ) -> request.setScheme( HttpScheme.HTTPS.asString() ) );
        return httpConfig;
    }

    public ServerConnector createConnector( Server server, KeyStoreInformation config, ListenSocketAddress address, JettyThreadCalculator jettyThreadCalculator )
    {
        SslConnectionFactory sslConnectionFactory = createSslConnectionFactory( config );
        return super.createConnector( server, address, jettyThreadCalculator, sslConnectionFactory, createHttpConnectionFactory() );
    }

    private SslConnectionFactory createSslConnectionFactory( KeyStoreInformation ksInfo )
    {
        SslContextFactory sslContextFactory = new SslContextFactory();

        sslContextFactory.setKeyStore( ksInfo.getKeyStore() );
        sslContextFactory.setKeyStorePassword( String.valueOf( ksInfo.getKeyStorePassword() ) );
        sslContextFactory.setKeyManagerPassword( String.valueOf( ksInfo.getKeyPassword() ) );

        return new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString());
    }

}
