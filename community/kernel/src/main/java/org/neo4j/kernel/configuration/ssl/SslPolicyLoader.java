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

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.net.ssl.TrustManagerFactory;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigValues;
import org.neo4j.kernel.configuration.GroupSettingSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.ClientAuth;
import org.neo4j.ssl.PkiUtils;
import org.neo4j.ssl.SslPolicy;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
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
// TODO: Use FileSystemAbstraction
// TODO: Create SslPolicies class in SSL module (distinct from loader)
public class SslPolicyLoader
{
    private final Map<String,SslPolicy> policies = new ConcurrentHashMap<>();
    private final PkiUtils pkiUtils = new PkiUtils();
    private final Config config;

    private SslPolicy legacyPolicy;

    private SslPolicyLoader( Config config )
    {
        this.config = config;
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

        return new SslPolicy( privateKey, keyCertChain, null, null, ClientAuth.OPTIONAL, trustAllFactory() );
    }

    private TrustManagerFactory trustAllFactory()
    {
        return InsecureTrustManagerFactory.INSTANCE;
    }

    private void load( Config config, Log log )
    {
        Function<ConfigValues,Stream<String>> enumeration = GroupSettingSupport.enumerate( SslPolicyConfig.class );
        Set<String> policyNames = config.view( enumeration ).collect( toSet() );

        for ( String policyName : policyNames )
        {
            if ( policyName.equals( LEGACY_POLICY_NAME ) )
            {
                // the legacy policy name is reserved for the legacy policy which derives its configuration from legacy settings
                throw new IllegalArgumentException( "Legacy policy cannot be configured. Please migrate to new SSL policy system." );
            }

            SslPolicyConfig policyConfig = new SslPolicyConfig( policyName );
            File baseDirectory = policyConfig.base_directory.from( config );
            File trustedCertificatesDir = policyConfig.trusted_dir.from( config );
            File revokedCertificatesDir = policyConfig.revoked_dir.from( config );

            if ( !baseDirectory.exists() )
            {
                throw new IllegalArgumentException(
                        format( "Base directory '%s' for SSL policy with name '%s' does not exist.", baseDirectory, policyName ) );
            }

            boolean allowKeyGeneration = policyConfig.allow_key_generation.from( config );

            File privateKeyFile = policyConfig.private_key.from( config );
            String privateKeyPassword = policyConfig.private_key_password.from( config );
            PrivateKey privateKey;

            X509Certificate[] keyCertChain;
            File keyCertChainFile = policyConfig.public_certificate.from( config );

            if ( !privateKeyFile.exists() && !keyCertChainFile.exists() && allowKeyGeneration )
            {
                log.info( format( "Generating key and self-signed certificate for SSL policy '%s'", policyName ) );
                String hostname = config.get( default_advertised_address );

                try
                {
                    pkiUtils.createSelfSignedCertificate( keyCertChainFile, privateKeyFile, hostname );

                    trustedCertificatesDir.mkdir();
                    // TODO: Add back when supporting loading of certificate revocation lists (CRL).
                    //revokedCertificatesDir.mkdir();
                }
                catch ( GeneralSecurityException | IOException | OperatorCreationException e )
                {
                    throw new RuntimeException( "Failed to generate private key and certificate", e );
                }
            }

            privateKey = loadPrivateKey( privateKeyFile, privateKeyPassword );
            keyCertChain = loadCertificateChain( keyCertChainFile );

            ClientAuth clientAuth = policyConfig.client_auth.from( config );
            boolean trustAll = policyConfig.trust_all.from( config );
            TrustManagerFactory trustManagerFactory;

            if ( trustAll )
            {
                trustManagerFactory = trustAllFactory();
            }
            else
            {
                try
                {
                    trustManagerFactory = createTrustManagerFactory( trustedCertificatesDir, clientAuth );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( "Failed to create trust manager based on: " + trustedCertificatesDir, e );
                }
            }

            // TODO: Add back when supporting loading of certificate revocation lists (CRL).
//            File[] revokedCertificateFiles = revokedCertificatesDir.listFiles();
//            if ( revokedCertificateFiles == null )
//            {
//                throw new RuntimeException( format( "Could not find or list files in revoked directory: %s", revokedCertificatesDir ) );
//            }
//
//            if ( revokedCertificateFiles.length != 0 )
//            {
//                throw new UnsupportedOperationException( "Loading of certificate revocation lists is not yet supported" );
//            }

            List<String> tlsVersions = policyConfig.tls_versions.from( config );
            List<String> ciphers = policyConfig.ciphers.from( config );

            SslPolicy sslPolicy = new SslPolicy( privateKey, keyCertChain, tlsVersions, ciphers, clientAuth, trustManagerFactory );
            log.info( format( "Loaded SSL policy '%s' = %s", policyName, sslPolicy ) );
            policies.put( policyName, sslPolicy );
        }
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
            throw new RuntimeException( "Failed to load private key: " + privateKeyFile
                                        + (privateKeyPassword == null ? "" : " (using configured password)"), e );
        }
    }

    private TrustManagerFactory createTrustManagerFactory( File trustedCertificatesDir, ClientAuth clientAuth ) throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance( KeyStore.getDefaultType() );
        keyStore.load( null, null );

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
                    keyStore.setCertificateEntry( Integer.toString( i++ ), cert );
                }
            }
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance( TrustManagerFactory.getDefaultAlgorithm() );
        trustManagerFactory.init( keyStore );

        return trustManagerFactory;
    }
}
