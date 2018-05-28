/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.discovery;

import com.hazelcast.nio.ssl.SSLContextFactory;

import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
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

        SecureRandom rand = new SecureRandom();
        char[] password = new char[32];
        for ( int i = 0; i < password.length; i++ )
        {
            password[i] = (char) rand.nextInt( Character.MAX_VALUE + 1 );
        }

        try
        {
            KeyStore keyStore = sslPolicy.getKeyStore( password, password );
            keyManagerFactory.init( keyStore, password );
        }
        catch ( KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            for ( int i = 0; i < password.length; i++ )
            {
                password[i] = 0;
            }
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
