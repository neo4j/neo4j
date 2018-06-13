/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.configuration.ssl;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.cert.CertStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CRL;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.Collection;
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.TrustManagerFactory;

import org.neo4j.ssl.ClientAuth;

import static java.lang.String.format;

public class TrustManagerFactoryProvider
{
    public TrustManagerFactory get( boolean trustAll, File trustedCertificatesDir,
            Collection<X509CRL> crls, ClientAuth clientAuth )
    {
        try
        {
            if ( trustAll )
            {
                return InsecureTrustManagerFactory.INSTANCE;
            }

            KeyStore trustStore = KeyStore.getInstance( KeyStore.getDefaultType() );
            trustStore.load( null, null );

            File[] trustedCertFiles = trustedCertificatesDir.listFiles();

            if ( trustedCertFiles == null )
            {
                throw new RuntimeException( format( "Could not find or list files in trusted directory: %s", trustedCertificatesDir ) );
            }
            else if ( clientAuth == ClientAuth.REQUIRE && trustedCertFiles.length == 0 )
            {
                throw new RuntimeException( format( "Client auth is required but no trust anchors found in: %s", trustedCertificatesDir ) );
            }

            int i = 0;
            for ( File trustedCertFile : trustedCertFiles )
            {
                CertificateFactory certificateFactory = CertificateFactory.getInstance( "X.509" );
                try ( InputStream input = Files.newInputStream( trustedCertFile.toPath() ) )
                {
                    while ( input.available() > 0 )
                    {
                        try
                        {
                            X509Certificate cert = (X509Certificate) certificateFactory.generateCertificate( input );
                            trustStore.setCertificateEntry( Integer.toString( i++ ), cert );
                        }
                        catch ( Exception e )
                        {
                            throw new CertificateException( "Error loading certificate file: " + trustedCertFile, e );
                        }
                    }
                }
            }

            String algorithm = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance( algorithm );

            if ( !crls.isEmpty() )
            {
                PKIXBuilderParameters pkixParamsBuilder = new PKIXBuilderParameters( trustStore, new X509CertSelector() );
                pkixParamsBuilder.setRevocationEnabled( true );

                pkixParamsBuilder.addCertStore( CertStore.getInstance( "Collection", new CollectionCertStoreParameters( crls ) ) );

                trustManagerFactory.init( new CertPathTrustManagerParameters( pkixParamsBuilder ) );
            }
            else
            {
                trustManagerFactory.init( trustStore );
            }

            return trustManagerFactory;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to create trust manager based on: " + trustedCertificatesDir, e );
        }
    }
}
