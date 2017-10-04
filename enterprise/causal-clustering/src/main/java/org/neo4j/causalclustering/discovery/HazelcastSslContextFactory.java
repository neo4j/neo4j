/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import com.hazelcast.nio.ssl.SSLContextFactory;

import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.Properties;
import java.util.UUID;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

import static java.lang.String.format;

class HazelcastSslContextFactory implements SSLContextFactory
{
    private static final String PROTOCOL = "TLS";

    private final SslPolicy sslPolicy;
    private final Log log;

    HazelcastSslContextFactory( SslPolicy sslPolicy, LogProvider logProvider )
    {
        this.sslPolicy = sslPolicy;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void init( Properties properties ) throws Exception
    {
    }

    @Override
    public SSLContext getSSLContext()
    {
        SSLContext sslCtx;

        try
        {
            sslCtx = SSLContext.getInstance( PROTOCOL );
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new RuntimeException( e );
        }

        KeyManagerFactory keyManagerFactory;
        try
        {
            keyManagerFactory = KeyManagerFactory.getInstance( KeyManagerFactory.getDefaultAlgorithm() );
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new RuntimeException( e );
        }

        String password = UUID.randomUUID().toString();
        KeyStore keyStore = sslPolicy.getKeyStore( password.toCharArray(), password.toCharArray() );

        try
        {
            keyManagerFactory.init( keyStore, password.toCharArray() );
        }
        catch ( KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e )
        {
            throw new RuntimeException( e );
        }

        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
        TrustManager[] trustManagers = sslPolicy.getTrustManagerFactory().getTrustManagers();

        try
        {
            sslCtx.init( keyManagers, trustManagers, null );
        }
        catch ( KeyManagementException e )
        {
            throw new RuntimeException( e );
        }

        if ( sslPolicy.getTlsVersions() != null )
        {
            log.warn( format( "Restricting TLS versions through policy not supported." +
                              " System defaults for %s family will be used.", PROTOCOL ) );
        }

        if ( sslPolicy.getCipherSuites() != null )
        {
            log.warn( "Restricting ciphers through policy not supported." +
                      " System defaults will be used." );
        }

        return sslCtx;
    }
}
