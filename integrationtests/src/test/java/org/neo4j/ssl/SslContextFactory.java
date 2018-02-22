/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

class SslContextFactory
{
    interface Ciphers
    {
        SslParameters ciphers( String... ciphers );
    }

    static class SslParameters implements Ciphers
    {
        private String protocols;
        private String ciphers;

        private SslParameters( String protocols, String ciphers )
        {
            this.protocols = protocols;
            this.ciphers = ciphers;
        }

        static Ciphers protocols( String... protocols )
        {
            return new SslParameters( String.join( ",", protocols ), null );
        }

        @Override
        public SslParameters ciphers( String... ciphers )
        {
            this.ciphers = String.join( ",", ciphers );
            return this;
        }

        @Override
        public String toString()
        {
            return "SslParameters{" + "protocols='" + protocols + '\'' + ", ciphers='" + ciphers + '\'' + '}';
        }
    }

    static SslContext makeSslContext( SslResource sslResource, boolean forServer, SslParameters params ) throws CertificateException, IOException
    {
        return makeSslContext( sslResource, forServer, SslProvider.JDK.name(), params.protocols, params.ciphers );
    }

    static SslContext makeSslContext( SslResource sslResource, boolean forServer, String sslProvider ) throws CertificateException, IOException
    {
        return makeSslContext( sslResource, forServer, sslProvider, null, null );
    }

    static SslContext makeSslContext( SslResource sslResource, boolean forServer ) throws CertificateException, IOException
    {
        return makeSslContext( sslResource, forServer, SslProvider.JDK.name(), null, null );
    }

    static SslContext makeSslContext( SslResource sslResource, boolean forServer, String sslProvider, String protocols, String ciphers ) throws CertificateException, IOException
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

        SslPolicyLoader sslPolicyFactory = SslPolicyLoader.create( Config.serverDefaults( config ), NullLogProvider.getInstance() );

        SslPolicy sslPolicy = sslPolicyFactory.getPolicy( "default" );
        return forServer ? sslPolicy.nettyServerContext() : sslPolicy.nettyClientContext();
    }
}
