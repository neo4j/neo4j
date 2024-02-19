/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.ssl.config;

import static java.lang.String.format;
import static org.neo4j.pki.PkiUtils.CERTIFICATE_FACTORY;

import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CRLException;
import java.security.cert.CertStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
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
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.TrustManagerFactory;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SslSystemInternalSettings;
import org.neo4j.configuration.SslSystemSettings;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.pki.PkiUtils;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.string.SecureString;

/**
 * Each component which utilises SSL policies is recommended to provide a component
 * specific override setting for the name of the SSL policy to use. It is also recommended
 * that this setting allows null to be specified with the meaning that no SSL security shall
 * be put into place. What this means practically is up to the component, but it could for
 * example mean that the traffic will be plaintext over TCP in such case.
 *
 * @see SslPolicy
 */
public class SslPolicyLoader {
    private static final DirectoryStream.Filter<Path> ALL_FILES = path -> true;
    private static final DirectoryStream.Filter<Path> SKIP_DOT_FILES =
            path -> !path.getFileName().toString().startsWith(".");

    private final Map<SslPolicyScope, SslPolicy> policies = new ConcurrentHashMap<>();
    private final Config config;
    private final SslProvider sslProvider;
    private final InternalLogProvider logProvider;
    private final InternalLog log;
    private final boolean skipDotFiles;
    private final FileSystemAbstraction fileSystem;

    private SslPolicyLoader(FileSystemAbstraction fileSystem, Config config, InternalLogProvider logProvider) {
        this.fileSystem = fileSystem;
        this.config = config;
        this.skipDotFiles = config.get(SslSystemInternalSettings.ignore_dotfiles);
        this.sslProvider = config.get(SslSystemSettings.netty_ssl_provider);
        this.logProvider = logProvider;
        this.log = logProvider.getLog(SslPolicyLoader.class);
    }

    /**
     * Loads all the SSL policies as defined by the config.
     *
     * @param config The configuration for the SSL policies.
     * @return A factory populated with SSL policies.
     */
    public static SslPolicyLoader create(
            FileSystemAbstraction fileSystem, Config config, InternalLogProvider logProvider) {
        SslPolicyLoader policyFactory = new SslPolicyLoader(fileSystem, config, logProvider);
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
    public SslPolicy getPolicy(SslPolicyScope scope) {
        if (scope == null) {
            return null;
        }

        SslPolicy sslPolicy = policies.get(scope);

        if (sslPolicy == null) {
            throw new IllegalArgumentException(
                    format("Cannot find enabled SSL policy with name '%s' in the configuration", scope));
        }
        return sslPolicy;
    }

    public boolean hasPolicyForSource(SslPolicyScope scope) {
        return policies.containsKey(scope);
    }

    private void load() {
        config.getGroups(SslPolicyConfig.class).values().forEach(this::addPolicy);

        policies.forEach(
                (scope, sslPolicy) -> log.info(format("Loaded SSL policy '%s' = %s", scope.name(), sslPolicy)));
    }

    private void addPolicy(SslPolicyConfig policyConfig) {
        if (config.get(policyConfig.enabled)) {
            SslPolicyScope scope = policyConfig.getScope();
            SslPolicy policy = createSslPolicy(policyConfig);
            if (policies.put(scope, policy) != null) {
                throw new IllegalStateException("Can not have multiple SslPolicies configured for " + scope.name());
            }
        }
    }

    private SslPolicy createSslPolicy(SslPolicyConfig policyConfig) {
        Path baseDirectory = config.get(policyConfig.base_directory);
        Path revokedCertificatesDir = config.get(policyConfig.revoked_dir);

        if (!fileSystem.fileExists(baseDirectory)) {
            throw new IllegalArgumentException(format(
                    "Base directory '%s' for SSL policy with name '%s' does not exist.",
                    baseDirectory, policyConfig.name()));
        }

        KeyAndChain keyAndChain = pemKeyAndChain(policyConfig);

        Collection<X509CRL> crls = getCRLs(fileSystem, revokedCertificatesDir, certificateFilenameFilter());
        TrustManagerFactory trustManagerFactory;
        try {
            trustManagerFactory =
                    createTrustManagerFactory(config.get(policyConfig.trust_all), crls, keyAndChain.trustStore);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create trust manager", e);
        }

        boolean verifyHostname = config.get(policyConfig.verify_hostname);
        boolean verifyExpiration = !config.get(policyConfig.trust_expired);
        ClientAuth clientAuth = config.get(policyConfig.client_auth);
        List<String> tlsVersions = config.get(policyConfig.tls_versions);
        List<String> ciphers = config.get(policyConfig.ciphers);

        if (!verifyHostname && !config.isExplicitlySet(policyConfig.verify_hostname)) {
            log.warn(
                    "SSL Hostname verification is disabled by default. Consider explicitly setting %s",
                    policyConfig.verify_hostname.name());
        }

        return new SslPolicy(
                keyAndChain.privateKey,
                keyAndChain.keyCertChain,
                tlsVersions,
                ciphers,
                clientAuth,
                trustManagerFactory,
                sslProvider,
                verifyHostname,
                verifyExpiration,
                logProvider);
    }

    private KeyAndChain pemKeyAndChain(SslPolicyConfig policyConfig) {
        Path privateKeyFile = config.get(policyConfig.private_key);
        SecureString privateKeyPassword = config.get(policyConfig.private_key_password);
        Path trustedCertificatesDir = config.get(policyConfig.trusted_dir);

        ClientAuth clientAuth = config.get(policyConfig.client_auth);
        KeyStore trustStore;
        try {
            trustStore = createTrustStoreFromPem(trustedCertificatesDir, clientAuth);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create trust manager based on: " + trustedCertificatesDir, e);
        }

        if (policyConfig.getScope().isClientOnly() && !fileSystem.fileExists(privateKeyFile)) {
            return new KeyAndChain(null, new X509Certificate[0], trustStore);
        }

        PrivateKey privateKey;

        X509Certificate[] keyCertChain;
        Path keyCertChainFile = config.get(policyConfig.public_certificate);

        privateKey = loadPrivateKey(fileSystem, privateKeyFile, privateKeyPassword);
        keyCertChain = loadCertificateChain(fileSystem, keyCertChainFile);
        if (!config.get(policyConfig.trust_expired)) {
            for (var cert : keyCertChain) {
                if (cert != null) {
                    try {
                        cert.checkValidity();
                    } catch (CertificateExpiredException | CertificateNotYetValidException e) {
                        throw new RuntimeException("Expired certificate", e);
                    }
                }
            }
        }

        return new KeyAndChain(privateKey, keyCertChain, trustStore);
    }

    private static Collection<X509CRL> getCRLs(
            FileSystemAbstraction fileSystem, Path revokedCertificatesDir, DirectoryStream.Filter<Path> filter) {
        if (!fileSystem.fileExists(revokedCertificatesDir)) {
            return new ArrayList<>();
        }

        Path[] revocationFiles;
        try {
            revocationFiles = fileSystem.listFiles(revokedCertificatesDir, filter);
        } catch (IOException e) {
            throw new RuntimeException(
                    format("Could not find or list files in revoked directory: %s", revokedCertificatesDir), e);
        }

        Collection<X509CRL> crls = new ArrayList<>();
        for (Path crl : revocationFiles) {
            try (InputStream input = fileSystem.openAsInputStream(crl)) {
                crls.addAll((Collection<X509CRL>) PkiUtils.CERTIFICATE_FACTORY.generateCRLs(input));
            } catch (IOException | CRLException e) {
                throw new RuntimeException(format("Could not load CRL: %s", crl), e);
            }
        }

        return crls;
    }

    private static X509Certificate[] loadCertificateChain(FileSystemAbstraction fs, Path keyCertChainFile) {
        try {
            return PkiUtils.loadCertificates(fs, keyCertChainFile);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load public certificate chain: " + keyCertChainFile, e);
        }
    }

    private static PrivateKey loadPrivateKey(
            FileSystemAbstraction fs, Path privateKeyFile, SecureString privateKeyPassword) {
        String password = privateKeyPassword != null ? privateKeyPassword.getString() : null;

        try {
            return PkiUtils.loadPrivateKey(fs, privateKeyFile, password);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load private key: " + privateKeyFile
                            + (privateKeyPassword == null ? "" : " (using configured password)"),
                    e);
        }
    }

    private static TrustManagerFactory createTrustManagerFactory(
            boolean trustAll, Collection<X509CRL> crls, KeyStore trustStore) throws Exception {
        if (trustAll) {
            return InsecureTrustManagerFactory.INSTANCE;
        }

        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        if (!crls.isEmpty()) {
            PKIXBuilderParameters pkixParamsBuilder = new PKIXBuilderParameters(trustStore, new X509CertSelector());
            pkixParamsBuilder.setRevocationEnabled(true);

            pkixParamsBuilder.addCertStore(
                    CertStore.getInstance("Collection", new CollectionCertStoreParameters(crls)));

            trustManagerFactory.init(new CertPathTrustManagerParameters(pkixParamsBuilder));
        } else {
            trustManagerFactory.init(trustStore);
        }
        return trustManagerFactory;
    }

    private KeyStore createTrustStoreFromPem(Path trustedCertificatesDir, ClientAuth clientAuth)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        if (!fileSystem.fileExists(trustedCertificatesDir)) {
            return trustStore;
        }

        Path[] trustedCertFiles = fileSystem.listFiles(trustedCertificatesDir, certificateFilenameFilter());
        if (clientAuth == ClientAuth.REQUIRE && trustedCertFiles.length == 0) {
            throw new RuntimeException(
                    format("Client auth is required but no trust anchors found in: %s", trustedCertificatesDir));
        }

        int i = 0;
        for (Path trustedCertFile : trustedCertFiles) {
            try (InputStream input = fileSystem.openAsInputStream(trustedCertFile)) {
                try {
                    for (Certificate certificate : CERTIFICATE_FACTORY.generateCertificates(input)) {
                        X509Certificate cert = (X509Certificate) certificate;
                        trustStore.setCertificateEntry(Integer.toString(i++), cert);
                    }
                } catch (Exception e) {
                    throw new CertificateException("Error loading certificate file: " + trustedCertFile, e);
                }
            }
        }
        return trustStore;
    }

    private record KeyAndChain(PrivateKey privateKey, X509Certificate[] keyCertChain, KeyStore trustStore) {}

    private DirectoryStream.Filter<Path> certificateFilenameFilter() {
        return skipDotFiles ? SKIP_DOT_FILES : ALL_FILES;
    }
}
