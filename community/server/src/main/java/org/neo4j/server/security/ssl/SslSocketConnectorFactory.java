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
package org.neo4j.server.security.ssl;

import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConfiguration.Customizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.List;
import java.util.UUID;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.web.HttpConnectorFactory;
import org.neo4j.server.web.JettyThreadCalculator;
import org.neo4j.ssl.SslPolicy;


public class SslSocketConnectorFactory extends HttpConnectorFactory
{
    private final Customizer requestCustomizer;

    public SslSocketConnectorFactory( Config config )
    {
        super( config );
        requestCustomizer = new HttpsRequestCustomizer( config );
    }

    @Override
    protected HttpConfiguration createHttpConfig()
    {
        HttpConfiguration httpConfig = super.createHttpConfig();
        httpConfig.addCustomizer( requestCustomizer );
        return httpConfig;
    }

    public ServerConnector createConnector( Server server, SslPolicy sslPolicy, ListenSocketAddress address,
            JettyThreadCalculator jettyThreadCalculator )
    {
        SslConnectionFactory sslConnectionFactory = createSslConnectionFactory( sslPolicy );
        return super.createConnector( server, address, jettyThreadCalculator, sslConnectionFactory, createHttpConnectionFactory() );
    }

    private SslConnectionFactory createSslConnectionFactory( SslPolicy sslPolicy )
    {
        SslContextFactory sslContextFactory = new SslContextFactory();

        String password = UUID.randomUUID().toString();
        sslContextFactory.setKeyStore( sslPolicy.getKeyStore( password.toCharArray(), password.toCharArray() ) );
        sslContextFactory.setKeyStorePassword( password );
        sslContextFactory.setKeyManagerPassword( password );

        List<String> ciphers = sslPolicy.getCipherSuites();
        if ( ciphers != null )
        {
            sslContextFactory.setIncludeCipherSuites( ciphers.toArray( new String[ciphers.size()] ) );
            sslContextFactory.setExcludeCipherSuites();
        }

        String[] protocols = sslPolicy.getTlsVersions();
        if ( protocols != null )
        {
            sslContextFactory.setIncludeProtocols( protocols );
            sslContextFactory.setExcludeProtocols();
        }

        switch ( sslPolicy.getClientAuth() )
        {
        case REQUIRE:
            sslContextFactory.setNeedClientAuth( true );
            break;
        case OPTIONAL:
            sslContextFactory.setWantClientAuth( true );
            break;
        case NONE:
            sslContextFactory.setWantClientAuth( false );
            sslContextFactory.setNeedClientAuth( false );
            break;
        default:
            throw new IllegalArgumentException( "Not supported: " + sslPolicy.getClientAuth() );
        }

        return new SslConnectionFactory( sslContextFactory, HttpVersion.HTTP_1_1.asString() );
    }
}
