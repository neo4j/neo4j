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
package org.neo4j.ssl;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;

import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

public class SslPolicy
{
    /* cryptographic objects */
    private final PrivateKey privateKey;
    private final X509Certificate[] keyCertChain;

    /* cryptographic parameters */
    private final List<String> ciphers;
    private final String[] tlsVersions;
    private final ClientAuth clientAuth;

    private final TrustManagerFactory trustManagerFactory;
    private final SslProvider sslProvider;

    public SslPolicy( PrivateKey privateKey, X509Certificate[] keyCertChain,
            List<String> tlsVersions, List<String> ciphers, ClientAuth clientAuth,
            TrustManagerFactory trustManagerFactory, SslProvider sslProvider )
    {
        this.privateKey = privateKey;
        this.keyCertChain = keyCertChain;
        this.tlsVersions = tlsVersions == null ? null : tlsVersions.toArray( new String[tlsVersions.size()] );
        this.ciphers = ciphers;
        this.clientAuth = clientAuth;
        this.trustManagerFactory = trustManagerFactory;
        this.sslProvider = sslProvider;
    }

    public SslContext nettyServerContext() throws SSLException
    {
        return SslContextBuilder.forServer( privateKey, keyCertChain )
                .sslProvider( sslProvider )
                .clientAuth( forNetty( clientAuth ) )
                .protocols( tlsVersions )
                .ciphers( ciphers )
                .trustManager( trustManagerFactory )
                .build();
    }

    public SslContext nettyClientContext() throws SSLException
    {
        return SslContextBuilder.forClient()
                .sslProvider( sslProvider )
                .keyManager( privateKey, keyCertChain )
                .protocols( tlsVersions )
                .ciphers( ciphers )
                .trustManager( trustManagerFactory )
                .build();
    }

    private io.netty.handler.ssl.ClientAuth forNetty( ClientAuth clientAuth )
    {
        switch ( clientAuth )
        {
        case NONE:
            return io.netty.handler.ssl.ClientAuth.NONE;
        case OPTIONAL:
            return io.netty.handler.ssl.ClientAuth.OPTIONAL;
        case REQUIRE:
            return io.netty.handler.ssl.ClientAuth.REQUIRE;
        default:
            throw new IllegalArgumentException( "Cannot translate to netty equivalent: " + clientAuth );
        }
    }

    @SuppressWarnings( "unused" )
    public SslHandler nettyServerHandler( Channel channel ) throws SSLException
    {
        return makeNettyHandler( channel, nettyServerContext() );
    }

    @SuppressWarnings( "unused" )
    public SslHandler nettyClientHandler( Channel channel ) throws SSLException
    {
        return makeNettyHandler( channel, nettyClientContext() );
    }

    private SslHandler makeNettyHandler( Channel channel, SslContext sslContext )
    {
        SSLEngine sslEngine = sslContext.newEngine( channel.alloc() );
        if ( tlsVersions != null )
        {
            sslEngine.setEnabledProtocols( tlsVersions );
        }
        return new SslHandler( sslEngine );
    }

    public PrivateKey privateKey()
    {
        return privateKey;
    }

    public X509Certificate[] certificateChain()
    {
        return keyCertChain;
    }

    public KeyStore getKeyStore( char[] keyStorePass, char[] privateKeyPass )
    {
        KeyStore keyStore;
        try
        {
            keyStore = KeyStore.getInstance( KeyStore.getDefaultType() );
            keyStore.load( null, keyStorePass );
            keyStore.setKeyEntry( "key", privateKey, privateKeyPass, keyCertChain );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }

        return keyStore;
    }

    public TrustManagerFactory getTrustManagerFactory()
    {
        return trustManagerFactory;
    }

    public List<String> getCipherSuites()
    {
        return ciphers;
    }

    public String[] getTlsVersions()
    {
        return tlsVersions;
    }

    public ClientAuth getClientAuth()
    {
        return clientAuth;
    }

    @Override
    public String toString()
    {
        return "SslPolicy{" +
               "keyCertChain=" + describeCertChain() +
               ", ciphers=" + ciphers +
               ", tlsVersions=" + Arrays.toString( tlsVersions ) +
               ", clientAuth=" + clientAuth +
               '}';
    }

    private String describeCertificate( X509Certificate certificate )
    {
        return "Subject: " + certificate.getSubjectDN() +
               ", Issuer: " + certificate.getIssuerDN();
    }

    private String describeCertChain()
    {
        List<String> certificates = Arrays.stream( keyCertChain ).map( this::describeCertificate ).collect( Collectors.toList() );
        return String.join( ", ", certificates );
    }
}
