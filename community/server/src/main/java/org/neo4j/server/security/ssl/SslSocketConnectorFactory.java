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
package org.neo4j.server.security.ssl;

import org.eclipse.jetty.http.HttpScheme;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.neo4j.server.web.HttpConnectorFactory;
import org.neo4j.server.web.JettyThreadCalculator;


public class SslSocketConnectorFactory extends HttpConnectorFactory
{
    @Override
    protected HttpConfiguration createHttpConfig()
    {
        HttpConfiguration httpConfig = super.createHttpConfig();
        httpConfig.addCustomizer( new HttpConfiguration.Customizer()
        {
            @Override
            public void customize( Connector connector, HttpConfiguration channelConfig, Request request )
            {
                request.setScheme( HttpScheme.HTTPS.asString() );
            }
        } );
        return httpConfig;
    }

    public ServerConnector createConnector( Server server, KeyStoreInformation config, String host, int port, JettyThreadCalculator jettyThreadCalculator )
    {

        SslConnectionFactory sslConnectionFactory = createSslConnectionFactory( config );

        return super.createConnector( server, host, port, jettyThreadCalculator, sslConnectionFactory, createHttpConnectionFactory() );
    }

    private SslConnectionFactory createSslConnectionFactory( KeyStoreInformation config )
    {
        SslContextFactory sslContextFactory = new SslContextFactory();

        sslContextFactory.setKeyStorePath( config.getKeyStorePath() );
        sslContextFactory.setKeyStorePassword( String.valueOf( config.getKeyStorePassword() ) );
        sslContextFactory.setKeyManagerPassword( String.valueOf( config.getKeyPassword() ) );

        return new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString());
    }

}
