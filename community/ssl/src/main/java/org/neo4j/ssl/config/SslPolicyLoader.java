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
package org.neo4j.ssl.config;

import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CRLException;
import java.security.cert.CertStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CRL;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.TrustManagerFactory;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.ssl.BaseSslPolicyConfig;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.KeyStoreSslPolicyConfig;
import org.neo4j.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.configuration.ssl.PemSslPolicyConfig;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.PkiUtils;
import org.neo4j.ssl.SslPolicy;

import static java.lang.String.format;
import static org.neo4j.configuration.ssl.BaseSslPolicyConfig.CIPHER_SUITES_DEFAULTS;
import static org.neo4j.configuration.ssl.BaseSslPolicyConfig.TLS_VERSION_DEFAULTS;
import static org.neo4j.configuration.ssl.LegacySslPolicyConfig.LEGACY_POLICY_NAME;

/**
 * Each component which utilises SSL policies is recommended to provide a component
 * specific override setting for the name of the SSL policy to use. It is also recommended
 * that this setting allows null to be specified with the meaning that no SSL security shall
 * be put into place. What this means practically is up to the component, but it could for
 * example mean that the traffic will be plaintext over TCP in such case.
 *
 * @see BaseSslPolicyConfig
 */
public class SslPolicyLoader
{
    private final Map<String,SslPolicy> policies = new ConcurrentHashMap<>();
    private final PkiUtils pkiUtils = new PkiUtils();
    private final Config config;
    private final SslProvider sslProvider;
    private final LogProvider logProvider;

    private SslPolicy legacyPolicy;

    private SslPolicyLoader( Config config, LogProvider logProvider )
    {
        this.config = config;
        this.sslProvider = config.get( SslSystemSettings.netty_ssl_provider );
        this.logProvider = logProvider;
    }

    /**
     * Loads all the SSL policies as defined by the config.
     *
     * @param config The configuration for the SSL policies.
     * @return A factory populated with SSL policies.
     */
    public static SslPolicyLoader create( Config config, LogProvider logProvider )
    {
        SslPolicyLoader policyFactory = new SslPolicyLoader( config, logProvider );
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

    private synchronized SslPolicy getOrCreateLegacyPolicy()
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
        File certificateFile = config.get( LegacySslPolicyConfig.tls_certificate_file ).getAbsoluteFile();

        if ( !privateKeyFile.exists() && !certificateFile.exists() )
        {
            String hostname = config.get( GraphDatabaseSettings.default_advertised_address );

            try
            {
                pkiUtils.createSelfSignedCertificate( certificateFile, privateKeyFile, hostname );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Failed to generate private key and certificate", e );
            }
        }

        PrivateKey privateKey = loadPrivateKey( privateKeyFile, null );
        X509Certificate[] keyCertChain = loadCertificateChain( certificateFile );

        return new SslPolicy( privateKey, keyCertChain, TLS_VERSION_DEFAULTS, CIPHER_SUITES_DEFAULTS,
                ClientAuth.NONE, InsecureTrustManagerFactory.INSTANCE, sslProvider, false, logProvider );
    }

    private void load( Config config, Log log )
    {
        Set<String> policyNames = config.identifiersFromGroup( PemSslPolicyConfig.class );

        for ( String policyName : policyNames )
        {
            if ( policyName.equals( LEGACY_POLICY_NAME ) )
            {
                // the legacy policy name is reserved for the legacy policy which derives its configuration from legacy settings
                throw new IllegalArgumentException( "Legacy policy cannot be configured. Please migrate to new SSL policy system." );
            }

            SslPolicy sslPolicy;
            if ( BaseSslPolicyConfig.Format.PEM.equals( config.get( new BaseSslPolicyConfig.StubSslPolicyConfig( policyName ).format ) ) )
            {
                sslPolicy = pemSslPolicy( config, log, policyName );
            }
            else
            {
                sslPolicy = keyStoreSslPolicy( config, log, policyName );
            }

            log.info( format( "Loaded SSL policy '%s' = %s", policyName, sslPolicy ) );
            policies.put( policyName, sslPolicy );
        }
    }

    private SslPolicy pemSslPolicy( Config config, Log log, String policyName )
    {
        PemSslPolicyConfig policyConfig = new PemSslPolicyConfig( policyName );
        File baseDirectory = config.get( policyConfig.base_directory );
        File revokedCertificatesDir = config.get( policyConfig.revoked_dir );

        if ( !baseDirectory.exists() )
        {
            throw new IllegalArgumentException(
                    format( "Base directory '%s' for SSL policy with name '%s' does not exist.", baseDirectory, policyName ) );
        }

        KeyAndChain keyAndChain = pemKeyAndChain( config, log, policyName, policyConfig, revokedCertificatesDir );

        return sslPolicy( config, policyConfig, revokedCertificatesDir, keyAndChain );
    }

    private KeyAndChain pemKeyAndChain( Config config, Log log, String policyName, PemSslPolicyConfig policyConfig, File revokedCertificatesDir )
    {
        boolean allowKeyGeneration = config.get( policyConfig.allow_key_generation );

        File privateKeyFile = config.get( policyConfig.private_key );
        String privateKeyPassword = config.get( policyConfig.private_key_password );
        File trustedCertificatesDir = config.get( policyConfig.trusted_dir );

        PrivateKey privateKey;

        X509Certificate[] keyCertChain;
        File keyCertChainFile = config.get( policyConfig.public_certificate );

        if ( allowKeyGeneration && !privateKeyFile.exists() && !keyCertChainFile.exists() )
        {
            generatePrivateKeyAndCertificate( log, policyName, keyCertChainFile, privateKeyFile, trustedCertificatesDir, revokedCertificatesDir );
        }

        privateKey = loadPrivateKey( privateKeyFile, privateKeyPassword );
        keyCertChain = loadCertificateChain( keyCertChainFile );

        ClientAuth clientAuth = config.get( policyConfig.client_auth );
        KeyStore trustStore;
        try
        {
            trustStore = createTrustStoreFromPem( trustedCertificatesDir, clientAuth );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to create trust manager based on: " + trustedCertificatesDir, e );
        }

        return new KeyAndChain( privateKey, keyCertChain, trustStore );
    }

    private SslPolicy keyStoreSslPolicy( Config config, Log log, String policyName )
    {
        KeyStoreSslPolicyConfig policyConfig = new KeyStoreSslPolicyConfig( policyName );
        File baseDirectory = config.get( policyConfig.base_directory );
        File revokedCertificatesDir = config.get( policyConfig.revoked_dir );

        if ( !baseDirectory.exists() )
        {
            throw new IllegalArgumentException(
                    format( "Base directory '%s' for SSL policy with name '%s' does not exist.", baseDirectory, policyName ) );
        }

        KeyAndChain keyAndChain = keyStoreKeyAndChain( config, policyConfig );

        return sslPolicy( config, policyConfig, revokedCertificatesDir, keyAndChain );
    }

    private KeyAndChain keyStoreKeyAndChain( Config config, KeyStoreSslPolicyConfig policyConfig )
    {
        File keyStoreFile = config.get( policyConfig.keystore );

        String storePass = config.get( policyConfig.keystore_pass );

        String keyPass = config.get( policyConfig.entry_pass );
        String keyAlias = config.get( policyConfig.entry_alias );

        BaseSslPolicyConfig.Format type = config.get( policyConfig.format );
        KeyStore keyStore = loadKeyStore( keyStoreFile, storePass, type );

        KeyStore trustStore;
        File trustStoreFile = config.get( policyConfig.truststore );
        if ( trustStoreFile != null && !trustStoreFile.equals( keyStoreFile ) )
        {
            String trustStorePass = config.get( policyConfig.truststore_pass );
            trustStore = loadKeyStore( trustStoreFile, trustStorePass, type );
        }
        else
        {
            trustStore = keyStore;
        }

        X509Certificate[] certificateChain;
        PrivateKey key;
        try
        {
            Certificate[] chain = keyStore.getCertificateChain( keyAlias );
            certificateChain = Arrays.copyOf( chain, chain.length, X509Certificate[].class );
        }
        catch ( KeyStoreException e )
        {
            throw new RuntimeException( String.format( "Unable to load certificate chain from: %s alias %s ", keyStoreFile, keyAlias ), e );
        }
        try
        {
            key = (PrivateKey)keyStore.getKey( keyAlias, keyPass == null ? null : keyPass.toCharArray() );
        }
        catch ( KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException | ClassCastException e )
        {
            throw new RuntimeException( String.format( "Unable to load private key from: %s alias %s ", keyStoreFile, keyAlias ), e );
        }

        return new KeyAndChain( key, certificateChain, trustStore );
    }

    private KeyStore loadKeyStore( File keyStoreFile, String storePass, BaseSslPolicyConfig.Format type )
    {
        KeyStore keyStore;
        try
        {
            keyStore = KeyStore.getInstance( type.name() );
        }
        catch ( KeyStoreException e )
        {
            throw new RuntimeException( "Unable to create keystore with type: " + type, e );
        }

        try ( FileInputStream fis = new FileInputStream( keyStoreFile ) )
        {
            keyStore.load( fis, storePass == null ? null : storePass.toCharArray() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to open file: " + keyStoreFile, e );
        }
        catch ( CertificateException | NoSuchAlgorithmException e )
        {
            throw new RuntimeException( "Cryptographic error creating keystore from file: " + keyStoreFile, e );
        }
        return keyStore;
    }

    private SslPolicy sslPolicy( Config config, BaseSslPolicyConfig policyConfig, File revokedCertificatesDir, KeyAndChain keyAndChain )
    {
        Collection<X509CRL> crls = getCRLs( revokedCertificatesDir );
        TrustManagerFactory trustManagerFactory;
        try
        {
            trustManagerFactory = createTrustManagerFactory( config.get( policyConfig.trust_all ), crls, keyAndChain.trustStore );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to create trust manager", e );
        }

        boolean verifyHostname = config.get( policyConfig.verify_hostname );
        ClientAuth clientAuth = config.get( policyConfig.client_auth );
        List<String> tlsVersions = config.get( policyConfig.tls_versions );
        List<String> ciphers = config.get( policyConfig.ciphers );

        return new SslPolicy(
                keyAndChain.privateKey,
                keyAndChain.keyCertChain,
                tlsVersions,
                ciphers,
                clientAuth,
                trustManagerFactory,
                sslProvider,
                verifyHostname,
                logProvider );
    }

    private void generatePrivateKeyAndCertificate( Log log, String policyName, File keyCertChainFile, File privateKeyFile, File trustedCertificatesDir,
            File revokedCertificatesDir )
    {
        log.info( format( "Generating key and self-signed certificate for SSL policy '%s'", policyName ) );
        String hostname = config.get( GraphDatabaseSettings.default_advertised_address );

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
            certificateFactory = CertificateFactory.getInstance( PkiUtils.CERTIFICATE_TYPE );
        }
        catch ( CertificateException e )
        {
            throw new RuntimeException( "Could not generated certificate factory", e );
        }

        for ( File crl : revocationFiles )
        {
            try ( InputStream input = Files.newInputStream( crl.toPath() ) )
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

    private TrustManagerFactory createTrustManagerFactory( boolean trustAll, Collection<X509CRL> crls, KeyStore trustStore ) throws Exception
    {
        if ( trustAll )
        {
            return InsecureTrustManagerFactory.INSTANCE;
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

    private KeyStore createTrustStoreFromPem( File trustedCertificatesDir, ClientAuth clientAuth )
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException
    {
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
            CertificateFactory certificateFactory = CertificateFactory.getInstance( PkiUtils.CERTIFICATE_TYPE );
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
        return trustStore;
    }

    private static class KeyAndChain
    {
        final PrivateKey privateKey;
        final X509Certificate[] keyCertChain;
        final KeyStore trustStore;

        private KeyAndChain( PrivateKey privateKey, X509Certificate[] keyCertChain, KeyStore trustStore )
        {
            this.privateKey = privateKey;
            this.keyCertChain = keyCertChain;
            this.trustStore = trustStore;
        }
    }
}
