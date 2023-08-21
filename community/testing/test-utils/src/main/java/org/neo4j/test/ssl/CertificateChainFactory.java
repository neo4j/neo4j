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
package org.neo4j.test.ssl;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Random;
import java.util.Set;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AccessDescription;
import org.bouncycastle.asn1.x509.AuthorityInformationAccess;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;

/**
 * Utility for generating a 3 certificate chain with embedded ocsp revocation checking URIs
 */
public final class CertificateChainFactory {
    /**
     * Current time minus 1 year, just in case software clock goes back due to time synchronization
     */
    private static final Date NOT_BEFORE = new Date(System.currentTimeMillis() - 86_400_000L * 365);
    /**
     * The maximum possible value in X.509 specification: 9999-12-31 23:59:59
     */
    private static final Date NOT_AFTER = new Date(253_402_300_799_000L);

    private static volatile boolean cleanupRequired = true;

    private CertificateChainFactory() {}

    public static void createCertificateChain(
            Path endUserCertPath,
            Path endUserPrivateKeyPath,
            Path intCertPath,
            Path intPrivateKeyPath,
            Path rootCertPath,
            Path rootPrivateKeyPath,
            int ocspServerPortNo,
            BouncyCastleProvider bouncyCastleProvider)
            throws Exception {
        Security.addProvider(bouncyCastleProvider);
        installCleanupHook(bouncyCastleProvider);

        String ocspBaseURL = "http://localhost:" + ocspServerPortNo;

        KeyPair rootCertKeyPair = generateKeyPair();
        KeyPair intCertKeyPair = generateKeyPair();
        KeyPair endUserCertKeyPair = generateKeyPair();

        X509Certificate rootCert = generateCertificate(
                null,
                null,
                rootCertKeyPair,
                "rootCA",
                ocspBaseURL,
                rootCertPath,
                rootPrivateKeyPath,
                bouncyCastleProvider);
        X509Certificate intCert = generateCertificate(
                rootCert,
                rootCertKeyPair.getPrivate(),
                intCertKeyPair,
                "intCA",
                ocspBaseURL,
                intCertPath,
                intPrivateKeyPath,
                bouncyCastleProvider);
        X509Certificate endUserCert = generateCertificate(
                intCert,
                intCertKeyPair.getPrivate(),
                endUserCertKeyPair,
                "endUserCA",
                ocspBaseURL,
                endUserCertPath,
                endUserPrivateKeyPath,
                bouncyCastleProvider);

        // for the end user certificate we overwrite the single cert for entire chain
        writePem("CERTIFICATE", endUserCert.getEncoded(), intCert.getEncoded(), rootCert.getEncoded(), endUserCertPath);
        writePem("PRIVATE KEY", endUserCertKeyPair.getPrivate().getEncoded(), endUserPrivateKeyPath);

        // Mark as done so we don't clean up certificates
        cleanupRequired = false;
    }

    private static KeyPair generateKeyPair() throws NoSuchAlgorithmException, NoSuchProviderException {
        KeyPairGenerator kpGen = KeyPairGenerator.getInstance("RSA", "BC");
        kpGen.initialize(2048, new SecureRandom());
        return kpGen.generateKeyPair();
    }

    private static X509Certificate generateCertificate(
            X509Certificate issuingCert,
            PrivateKey issuingPrivateKey,
            KeyPair certKeyPair,
            String certName,
            String ocspURL,
            Path certificatePath,
            Path keyPath,
            BouncyCastleProvider bouncyCastleProvider)
            throws Exception {
        X509v3CertificateBuilder builder;

        if (issuingCert == null) {
            builder = new JcaX509v3CertificateBuilder(
                    new X500Name("CN=" + certName), // issuer authority
                    BigInteger.valueOf(new Random().nextInt()), // serial number of certificate
                    NOT_BEFORE, // start of validity
                    NOT_AFTER, // end of certificate validity
                    new X500Name("CN=" + certName), // subject name of certificate
                    certKeyPair.getPublic()); // public key of certificate
        } else {
            builder = new JcaX509v3CertificateBuilder(
                    issuingCert, // issuer authority
                    BigInteger.valueOf(new Random().nextInt()), // serial number of certificate
                    NOT_BEFORE, // start of validity
                    NOT_AFTER, // end of certificate validity
                    new X500Name("CN=" + certName), // subject name of certificate
                    certKeyPair.getPublic()); // public key of certificate
        }

        // key usage restrictions
        builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.digitalSignature));
        builder.addExtension(Extension.extendedKeyUsage, true, new ExtendedKeyUsage(KeyPurposeId.anyExtendedKeyUsage));
        builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(true));

        // embed ocsp URI
        builder.addExtension(
                Extension.authorityInfoAccess,
                false,
                new AuthorityInformationAccess(new AccessDescription(
                        AccessDescription.id_ad_ocsp,
                        new GeneralName(GeneralName.uniformResourceIdentifier, ocspURL + "/" + certName))));
        X509Certificate certificate = new JcaX509CertificateConverter()
                .getCertificate(builder.build(new JcaContentSignerBuilder("SHA256withRSA")
                        .setProvider(bouncyCastleProvider)
                        .build(
                                issuingPrivateKey == null
                                        ? certKeyPair.getPrivate()
                                        : issuingPrivateKey))); // self sign if root cert

        writePem("CERTIFICATE", certificate.getEncoded(), certificatePath);
        writePem("PRIVATE KEY", certKeyPair.getPrivate().getEncoded(), keyPath);

        return certificate;
    }

    /**
     * Makes sure to delete partially generated certificates and reset the security context.
     * Does nothing if both certificate and private key have been generated successfully.
     * <p>
     * The hook should only be installed prior to generation of the certificate chain, and not if certificates already exist.
     */
    private static void installCleanupHook(BouncyCastleProvider bouncyCastleProvider) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (cleanupRequired) {
                System.err.println("Cleaning up partially generated self-signed certificate...");
                Security.removeProvider(bouncyCastleProvider.getName());
            }
        }));
    }

    private static void writePem(String type, byte[] encodedContent, Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try (PemWriter writer = new PemWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8))) {
            writer.writeObject(new PemObject(type, encodedContent));
            writer.flush();
        }
        try {
            Files.setPosixFilePermissions(
                    path, Set.of(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ));
        } catch (UnsupportedOperationException ignore) {
            // Fallback for windows
            File file = path.toFile();
            file.setReadable(false, false);
            file.setWritable(false, false);
            file.setReadable(true);
            file.setWritable(true);
        }
    }

    private static void writePem(String type, byte[] certA, byte[] certB, byte[] certC, Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try (PemWriter writer = new PemWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8))) {
            writer.writeObject(new PemObject(type, certA));
            writer.writeObject(new PemObject(type, certB));
            writer.writeObject(new PemObject(type, certC));
            writer.flush();
        }
        try {
            Files.setPosixFilePermissions(
                    path, Set.of(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ));
        } catch (UnsupportedOperationException ignore) {
            // Fallback for windows
            File file = path.toFile();
            file.setReadable(false, false);
            file.setWritable(false, false);
            file.setReadable(true);
            file.setWritable(true);
        }
    }
}
