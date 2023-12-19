/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.ssl;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;

import java.io.File;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyConfig;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.configuration.ssl.SslSystemSettings;
import org.neo4j.logging.NullLogProvider;

public class SslContextFactory
{
    public interface Ciphers
    {
        SslParameters ciphers( String... ciphers );
    }

    public static class SslParameters implements Ciphers
    {
        private String protocols;
        private String ciphers;

        private SslParameters( String protocols, String ciphers )
        {
            this.protocols = protocols;
            this.ciphers = ciphers;
        }

        public static Ciphers protocols( String... protocols )
        {
            return new SslParameters( joinOrNull( protocols ), null );
        }

        @Override
        public SslParameters ciphers( String... ciphers )
        {
            this.ciphers = joinOrNull( ciphers );
            return this;
        }

        /**
         * The low-level frameworks use null to signify that defaults shall be used, and so does our SSL framework.
         */
        private static String joinOrNull( String[] parts )
        {
            return parts.length > 0 ? String.join( ",", parts ) : null;
        }

        @Override
        public String toString()
        {
            return "SslParameters{" + "protocols='" + protocols + '\'' + ", ciphers='" + ciphers + '\'' + '}';
        }
    }

    public static SslContext makeSslContext( SslResource sslResource, boolean forServer, SslParameters params ) throws CertificateException, IOException
    {
        return makeSslContext( sslResource, forServer, SslProvider.JDK.name(), params.protocols, params.ciphers );
    }

    public static SslContext makeSslContext( SslResource sslResource, boolean forServer, String sslProvider ) throws CertificateException, IOException
    {
        return makeSslContext( sslResource, forServer, sslProvider, null, null );
    }

    public static SslContext makeSslContext( SslResource sslResource, boolean forServer ) throws CertificateException, IOException
    {
        return makeSslContext( sslResource, forServer, SslProvider.JDK.name(), null, null );
    }

    public static SslContext makeSslContext( SslResource sslResource, boolean forServer, String sslProvider, String protocols, String ciphers )
            throws CertificateException, IOException
    {
        Map<String,String> config = new HashMap<>();
        config.put( SslSystemSettings.netty_ssl_provider.name(), sslProvider );

        SslPolicyConfig policyConfig = new SslPolicyConfig( "default" );
        File baseDirectory = sslResource.privateKey().getParentFile();
        new File( baseDirectory, "trusted" ).mkdirs();
        new File( baseDirectory, "revoked" ).mkdirs();

        config.put( policyConfig.base_directory.name(), baseDirectory.getPath() );
        config.put( policyConfig.private_key.name(), sslResource.privateKey().getPath() );
        config.put( policyConfig.public_certificate.name(), sslResource.publicCertificate().getPath() );
        config.put( policyConfig.trusted_dir.name(), sslResource.trustedDirectory().getPath() );
        config.put( policyConfig.revoked_dir.name(), sslResource.revokedDirectory().getPath() );

        if ( protocols != null )
        {
            config.put( policyConfig.tls_versions.name(), protocols );
        }

        if ( ciphers != null )
        {
            config.put( policyConfig.ciphers.name(), ciphers );
        }

        SslPolicyLoader sslPolicyFactory = SslPolicyLoader.create( Config.fromSettings( config ).build(), NullLogProvider.getInstance() );

        SslPolicy sslPolicy = sslPolicyFactory.getPolicy( "default" );
        return forServer ? sslPolicy.nettyServerContext() : sslPolicy.nettyClientContext();
    }
}
