/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.configuration.ssl;

import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CRLException;
import java.security.cert.CertStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CRL;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.TrustManagerFactory;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.ClientAuth;
import org.neo4j.ssl.PkiUtils;
import org.neo4j.ssl.SslPolicy;

import static java.lang.String.format;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig.LEGACY_POLICY_NAME;

/**
 * Each component which utilises SSL policies is recommended to provide a component
 * specific override setting for the name of the SSL policy to use. It is also recommended
 * that this setting allows null to be specified with the meaning that no SSL security shall
 * be put into place. What this means practically is up to the component, but it could for
 * example mean that the traffic will be plaintext over TCP in such case.
 *
 * @see SslPolicyConfig
 */
public class SslPolicyLoader
{
    private final Map<String,SslPolicy> policies = new ConcurrentHashMap<>();
    private final PkiUtils pkiUtils = new PkiUtils();
    private final Config config;
    private final SslProvider sslProvider;

    private SslPolicy legacyPolicy;

    private SslPolicyLoader( Config config )
    {
        this.config = config;
        this.sslProvider = config.get( SslSystemSettings.netty_ssl_provider );
    }

    /**
     * Loads all the SSL policies as defined by the config.
     *
     * @param config The configuration for the SSL policies.
     * @return A factory populated with SSL policies.
     */
    public static SslPolicyLoader create( Config config, LogProvider logProvider )
    {
        SslPolicyLoader policyFactory = new SslPolicyLoader( config );
        policyFactory.load( config, logProvider.getLog( SslPolicyLoader.class ) );
        return policyFactory;
    }

    /**
     * Use this for retrieving the SSL policy configured under the specified name.
     *
     * @param policyName The name of the SSL policy to be returned.
     * @return Returns the policy defined under the requested name. If the policy name is null
     * then the null policy will be returned which means that SSL will not be used. It is up
     * to each respective SSL policy using component to decide exactly what that means.
     * @throws IllegalArgumentException If a policy with the supplied name does not exist.
     */
    public SslPolicy getPolicy( String policyName )
    {
        if ( policyName == null )
        {
            return null;
        }
        else if ( policyName.equals( LEGACY_POLICY_NAME ) )
        {
            return getOrCreateLegacyPolicy();
        }

        SslPolicy sslPolicy = policies.get( policyName );

        if ( sslPolicy == null )
        {
            throw new IllegalArgumentException(
                    format( "Cannot find enabled SSL policy with name '%s' in the configuration", policyName ) );
        }
        return sslPolicy;
    }

    private SslPolicy getOrCreateLegacyPolicy()
    {
        if ( legacyPolicy != null )
        {
            return legacyPolicy;
        }
        legacyPolicy = loadOrCreateLegacyPolicy();
        return legacyPolicy;
    }

    private SslPolicy loadOrCreateLegacyPolicy()
    {
        File privateKeyFile = config.get( LegacySslPolicyConfig.tls_key_file ).getAbsoluteFile();
        File certficateFile = config.get( LegacySslPolicyConfig.tls_certificate_file ).getAbsoluteFile();

        if ( !privateKeyFile.exists() && !certficateFile.exists() )
        {
            String hostname = config.get( default_advertised_address );

            try
            {
                pkiUtils.createSelfSignedCertificate( certficateFile, privateKeyFile, hostname );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Failed to generate private key and certificate", e );
            }
        }

        PrivateKey privateKey = loadPrivateKey( privateKeyFile, null );
        X509Certificate[] keyCertChain = loadCertificateChain( certficateFile );

        return new SslPolicy( privateKey, keyCertChain, null, null,
                ClientAuth.NONE, InsecureTrustManagerFactory.INSTANCE, sslProvider );
    }

    private void load( Config config, Log log )
    {
        Set<String> policyNames = config.identifiersFromGroup( SslPolicyConfig.class );

        for ( String policyName : policyNames )
        {
            if ( policyName.equals( LEGACY_POLICY_NAME ) )
            {
                // the legacy policy name is reserved for the legacy policy which derives its configuration from legacy settings
                throw new IllegalArgumentException( "Legacy policy cannot be configured. Please migrate to new SSL policy system." );
            }

            SslPolicyConfig policyConfig = new SslPolicyConfig( policyName );
            File baseDirectory = config.get( policyConfig.base_directory );
            File trustedCertificatesDir = config.get( policyConfig.trusted_dir );
            File revokedCertificatesDir = config.get( policyConfig.revoked_dir );

            if ( !baseDirectory.exists() )
            {
                throw new IllegalArgumentException(
                        format( "Base directory '%s' for SSL policy with name '%s' does not exist.", baseDirectory, policyName ) );
            }

            boolean allowKeyGeneration = config.get( policyConfig.allow_key_generation );

            File privateKeyFile = config.get( policyConfig.private_key );
            String privateKeyPassword = config.get( policyConfig.private_key_password );
            PrivateKey privateKey;

            X509Certificate[] keyCertChain;
            File keyCertChainFile = config.get( policyConfig.public_certificate );

            if ( !privateKeyFile.exists() && !keyCertChainFile.exists() && allowKeyGeneration )
            {
                log.info( format( "Generating key and self-signed certificate for SSL policy '%s'", policyName ) );
                String hostname = config.get( default_advertised_address );

                try
                {
                    pkiUtils.createSelfSignedCertificate( keyCertChainFile, privateKeyFile, hostname );

                    trustedCertificatesDir.mkdir();
                    revokedCertificatesDir.mkdir();
                }
                catch ( GeneralSecurityException | IOException | OperatorCreationException e )
                {
                    throw new RuntimeException( "Failed to generate private key and certificate", e );
                }
            }

            privateKey = loadPrivateKey( privateKeyFile, privateKeyPassword );
            keyCertChain = loadCertificateChain( keyCertChainFile );

            ClientAuth clientAuth = config.get( policyConfig.client_auth );
            boolean trustAll = config.get( policyConfig.trust_all );
            TrustManagerFactory trustManagerFactory;

            Collection<X509CRL> crls = getCRLs( revokedCertificatesDir );

            try
            {
                trustManagerFactory = createTrustManagerFactory( trustAll, trustedCertificatesDir, crls, clientAuth );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Failed to create trust manager based on: " + trustedCertificatesDir, e );
            }

            List<String> tlsVersions = config.get( policyConfig.tls_versions );
            List<String> ciphers = config.get( policyConfig.ciphers );

            SslPolicy sslPolicy = new SslPolicy( privateKey, keyCertChain, tlsVersions, ciphers, clientAuth, trustManagerFactory, sslProvider );
            log.info( format( "Loaded SSL policy '%s' = %s", policyName, sslPolicy ) );
            policies.put( policyName, sslPolicy );
        }
    }

    private Collection<X509CRL> getCRLs( File revokedCertificatesDir )
    {
        Collection<X509CRL> crls = new ArrayList<>();
        File[] revocationFiles = revokedCertificatesDir.listFiles();

        if ( revocationFiles == null )
        {
            throw new RuntimeException( format( "Could not find or list files in revoked directory: %s", revokedCertificatesDir ) );
        }

        if ( revocationFiles.length == 0 )
        {
            return crls;
        }

        CertificateFactory certificateFactory;

        try
        {
            certificateFactory = CertificateFactory.getInstance( "X.509" );
        }
        catch ( CertificateException e )
        {
            throw new RuntimeException( "Could not generated certificate factory", e );
        }

        for ( File crl : revocationFiles )
        {
            try ( FileInputStream input = new FileInputStream( crl ) )
            {
                crls.addAll( (Collection<X509CRL>) certificateFactory.generateCRLs( input ) );
            }
            catch ( IOException | CRLException e )
            {
                throw new RuntimeException( format( "Could not load CRL: %s", crl ), e );
            }
        }

        return crls;
    }

    private X509Certificate[] loadCertificateChain( File keyCertChainFile )
    {
        try
        {
            return pkiUtils.loadCertificates( keyCertChainFile );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to load public certificate chain: " + keyCertChainFile, e );
        }
    }

    private PrivateKey loadPrivateKey( File privateKeyFile, String privateKeyPassword )
    {
        if ( privateKeyPassword != null )
        {
            // TODO: Support loading of private keys with passwords.
            throw new UnsupportedOperationException( "Loading private keys with passwords is not yet supported" );
        }

        try
        {
            return pkiUtils.loadPrivateKey( privateKeyFile );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to load private key: " + privateKeyFile +
                                        (privateKeyPassword == null ? "" : " (using configured password)"), e );
        }
    }

    private TrustManagerFactory createTrustManagerFactory( boolean trustAll, File trustedCertificatesDir,
            Collection<X509CRL> crls, ClientAuth clientAuth ) throws Exception
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
            try ( FileInputStream input = new FileInputStream( trustedCertFile ) )
            {
                while ( input.available() > 0 )
                {
                    X509Certificate cert = (X509Certificate) certificateFactory.generateCertificate( input );
                    trustStore.setCertificateEntry( Integer.toString( i++ ), cert );
                }
            }
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance( TrustManagerFactory.getDefaultAlgorithm() );

        if ( !crls.isEmpty() )
        {
            PKIXBuilderParameters pkixParamsBuilder = new PKIXBuilderParameters( trustStore, new X509CertSelector() );
            pkixParamsBuilder.setRevocationEnabled( true );

            pkixParamsBuilder.addCertStore( CertStore.getInstance( "Collection",
                    new CollectionCertStoreParameters( crls ) ) );

            trustManagerFactory.init( new CertPathTrustManagerParameters( pkixParamsBuilder ) );
        }
        else
        {
            trustManagerFactory.init( trustStore );
        }

        return trustManagerFactory;
    }
}
