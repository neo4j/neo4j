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
package org.neo4j.ssl;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;


import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

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
                .clientAuth( forNetty( clientAuth ) )
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

    public SslHandler nettyServerHandler( Channel channel ) throws SSLException
    {
        return makeNettyHandler( channel, nettyServerContext(), SslHandler::new );
    }

    public SslHandler nettyClientHandler( Channel channel ) throws SSLException
    {
        return makeNettyHandler( channel, nettyClientContext(), sslEngine -> new SslHandler( sslEngine )
        {
            private void bootstrapSNI( final InetSocketAddress remoteAddress )
            {
                SSLParameters params = this.engine().getSSLParameters();
                SNIHostName remoteHost = new SNIHostName( remoteAddress.getHostString() );

                List<SNIServerName> serverNames = params.getServerNames();
                if ( serverNames.stream().anyMatch( name -> name.equals( remoteHost ) ) )
                {
                    // No need to do anything the ssl params server names are already set correctly
                    return;
                }

                Stream<SNIServerName> newServerNames = Stream.of( remoteHost );
                List<SNIServerName> replacementServerNames = Stream
                        .concat( serverNames.stream(), newServerNames )
                        .distinct()
                        .collect( collectingAndThen( toList(), Collections::unmodifiableList ) );

                params.setServerNames( replacementServerNames );
                this.engine().setSSLParameters( params );
            }

            private void checkRemoteAddressAndBootstrapSNI( SocketAddress remoteAddress )
            {
                //TODO: warn if not using InetSocket Address?
                if ( remoteAddress != null && remoteAddress instanceof InetSocketAddress )
                {
                    bootstrapSNI( (InetSocketAddress) remoteAddress );
                }
            }

            @Override
            public void connect( ChannelHandlerContext ctx, SocketAddress remoteAddress,
                    SocketAddress localAddress, ChannelPromise promise ) throws Exception
            {
                checkRemoteAddressAndBootstrapSNI( remoteAddress );
                super.connect( ctx, remoteAddress, localAddress, promise );
            }
        } );
    }

    private SslHandler makeNettyHandler( Channel channel, SslContext sslContext,
            Function<SSLEngine,SslHandler> sslHandlerFactory )
    {
        SSLEngine sslEngine = sslContext.newEngine( channel.alloc() );
        if ( tlsVersions != null )
        {
            sslEngine.setEnabledProtocols( tlsVersions );
        }
        return sslHandlerFactory.apply(sslEngine);
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
        List<String> certificates = Arrays.stream( keyCertChain ).map( this::describeCertificate ).collect( toList() );
        return String.join( ", ", certificates );
    }
}
