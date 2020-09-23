/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.TrustManagerFactory;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.SslSystemInternalSettings;
import org.neo4j.configuration.SslSystemSettings;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.PkiUtils;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.string.SecureString;

import static java.lang.String.format;
import static java.nio.file.Files.exists;

/**
 * Each component which utilises SSL policies is recommended to provide a component
 * specific override setting for the name of the SSL policy to use. It is also recommended
 * that this setting allows null to be specified with the meaning that no SSL security shall
 * be put into place. What this means practically is up to the component, but it could for
 * example mean that the traffic will be plaintext over TCP in such case.
 *
 * @see SslPolicy
 */
public class SslPolicyLoader
{
    private static final DirectoryStream.Filter<Path> ALL_FILES = path -> true;
    private static final DirectoryStream.Filter<Path> SKIP_DOT_FILES = path -> !path.getFileName().toString().startsWith( "." );

    private final Map<SslPolicyScope,SslPolicy> policies = new ConcurrentHashMap<>();
    private final Config config;
    private final SslProvider sslProvider;
    private final LogProvider logProvider;
    private final Log log;
    private final boolean skipDotFiles;

    private SslPolicyLoader( Config config, LogProvider logProvider )
    {
        this.config = config;
        this.skipDotFiles = config.get( SslSystemInternalSettings.ignore_dotfiles );
        this.sslProvider = config.get( SslSystemSettings.netty_ssl_provider );
        this.logProvider = logProvider;
        this.log = logProvider.getLog( SslPolicyLoader.class );
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
        policyFactory.load();
        return policyFactory;
    }

    /**
     * Use this for retrieving the SSL policy configured under the specified name.
     *
     * @param scope The scope of the SSL policy to be returned.
     * @return Returns the policy defined under the requested name. If the policy name is null
     * then the null policy will be returned which means that SSL will not be used. It is up
     * to each respective SSL policy using component to decide exactly what that means.
     * @throws IllegalArgumentException If a policy with the supplied name does not exist.
     */
    public SslPolicy getPolicy( SslPolicyScope scope )
    {
        if ( scope == null )
        {
            return null;
        }

        SslPolicy sslPolicy = policies.get( scope );

        if ( sslPolicy == null )
        {
            throw new IllegalArgumentException(
                    format( "Cannot find enabled SSL policy with name '%s' in the configuration", scope ) );
        }
        return sslPolicy;
    }

    public boolean hasPolicyForSource( SslPolicyScope scope )
    {
        return policies.containsKey( scope );
    }

    private void load()
    {
        config.getGroups( SslPolicyConfig.class ).values().forEach( this::addPolicy );

        policies.forEach( ( scope, sslPolicy ) -> log.info( format( "Loaded SSL policy '%s' = %s", scope.name(), sslPolicy ) ) );
    }

    private void addPolicy( SslPolicyConfig policyConfig )
    {
        if ( config.get( policyConfig.enabled ) )
        {
            SslPolicyScope scope = policyConfig.getScope();
            SslPolicy policy = createSslPolicy( policyConfig );
            if ( policies.put( scope, policy ) != null )
            {
                throw new IllegalStateException( "Can not have multiple SslPolicies configured for " + scope.name() );
            }
        }
    }

    private SslPolicy createSslPolicy( SslPolicyConfig policyConfig )
    {
        Path baseDirectory = config.get( policyConfig.base_directory );
        Path revokedCertificatesDir = config.get( policyConfig.revoked_dir );

        if ( Files.notExists( baseDirectory ) )
        {
            throw new IllegalArgumentException(
                    format( "Base directory '%s' for SSL policy with name '%s' does not exist.", baseDirectory, policyConfig.name() ) );
        }

        KeyAndChain keyAndChain = pemKeyAndChain( policyConfig );

        Collection<X509CRL> crls = getCRLs( revokedCertificatesDir, certificateFilenameFilter() );
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

    private KeyAndChain pemKeyAndChain( SslPolicyConfig policyConfig )
    {
        Path privateKeyFile = config.get( policyConfig.private_key );
        SecureString privateKeyPassword = config.get( policyConfig.private_key_password );
        Path trustedCertificatesDir = config.get( policyConfig.trusted_dir );

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

        if ( policyConfig.getScope().isClientOnly() && !exists( privateKeyFile ) )
        {
            return new KeyAndChain( null, new X509Certificate[0], trustStore );
        }

        PrivateKey privateKey;

        X509Certificate[] keyCertChain;
        Path keyCertChainFile = config.get( policyConfig.public_certificate );

        privateKey = loadPrivateKey( privateKeyFile, privateKeyPassword );
        keyCertChain = loadCertificateChain( keyCertChainFile );

        return new KeyAndChain( privateKey, keyCertChain, trustStore );
    }

    private static Collection<X509CRL> getCRLs( Path revokedCertificatesDir, DirectoryStream.Filter<Path> filter )
    {
        if ( Files.notExists( revokedCertificatesDir ) )
        {
            return new ArrayList<>();
        }

        Path[] revocationFiles;
        try ( DirectoryStream<Path> paths = Files.newDirectoryStream( revokedCertificatesDir, filter ) )
        {
            revocationFiles = StreamSupport.stream( paths.spliterator(), false ).toArray( Path[]::new );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( format( "Could not find or list files in revoked directory: %s", revokedCertificatesDir ), e );
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

        Collection<X509CRL> crls = new ArrayList<>();
        for ( Path crl : revocationFiles )
        {
            try ( InputStream input = Files.newInputStream( crl ) )
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

    private static X509Certificate[] loadCertificateChain( Path keyCertChainFile )
    {
        try
        {
            return PkiUtils.loadCertificates( keyCertChainFile );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to load public certificate chain: " + keyCertChainFile, e );
        }
    }

    private PrivateKey loadPrivateKey( Path privateKeyFile, SecureString privateKeyPassword )
    {
        String password = privateKeyPassword != null ? privateKeyPassword.getString() : null;

        try
        {
            return PkiUtils.loadPrivateKey( privateKeyFile, password );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to load private key: " + privateKeyFile +
                                        (privateKeyPassword == null ? "" : " (using configured password)"), e );
        }
    }

    private static TrustManagerFactory createTrustManagerFactory( boolean trustAll, Collection<X509CRL> crls, KeyStore trustStore ) throws Exception
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

    private KeyStore createTrustStoreFromPem( Path trustedCertificatesDir, ClientAuth clientAuth )
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException
    {
        KeyStore trustStore = KeyStore.getInstance( KeyStore.getDefaultType() );
        trustStore.load( null, null );

        if ( Files.notExists( trustedCertificatesDir ) )
        {
            return trustStore;
        }

        Path[] trustedCertFiles;
        try ( DirectoryStream<Path> paths = Files.newDirectoryStream( trustedCertificatesDir, certificateFilenameFilter() ) )
        {
            trustedCertFiles = StreamSupport.stream( paths.spliterator(), false ).toArray( Path[]::new );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( format( "Could not find or list files in trusted directory: %s", trustedCertificatesDir ), e );
        }

        if ( clientAuth == ClientAuth.REQUIRE && trustedCertFiles.length == 0 )
        {
            throw new RuntimeException( format( "Client auth is required but no trust anchors found in: %s", trustedCertificatesDir ) );
        }

        int i = 0;
        for ( Path trustedCertFile : trustedCertFiles )
        {
            CertificateFactory certificateFactory = CertificateFactory.getInstance( PkiUtils.CERTIFICATE_TYPE );
            try ( InputStream input = Files.newInputStream( trustedCertFile ) )
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

    private static final class KeyAndChain
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

    private DirectoryStream.Filter<Path> certificateFilenameFilter()
    {
        return skipDotFiles ? SKIP_DOT_FILES : ALL_FILES;
    }
}
