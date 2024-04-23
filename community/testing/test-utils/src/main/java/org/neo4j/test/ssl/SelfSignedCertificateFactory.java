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

import static java.nio.file.Files.delete;
import static java.nio.file.Files.exists;
import static org.neo4j.io.fs.FileSystemUtils.writeAllBytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Set;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.memory.EmptyMemoryTracker;

public class SelfSignedCertificateFactory {
    /* Generating SSL certificates takes a long time.
     * This non-official setting allows us to use a fast source of randomness when running tests */
    private static final boolean useInsecureCertificateGeneration =
            Boolean.getBoolean("org.neo4j.useInsecureCertificateGeneration");
    private static final String DEFAULT_ENCRYPTION = "RSA";
    private final SecureRandom random;
    /** Current time minus 1 year, just in case software clock goes back due to time synchronization */
    private static final Date NOT_BEFORE = new Date(System.currentTimeMillis() - 86_400_000L * 365);
    /** The maximum possible value in X.509 specification: 9999-12-31 23:59:59 */
    private static final Date NOT_AFTER = new Date(253_402_300_799_000L);

    public static final String DEFAULT_KEY_FILE_NAME = "private.key";
    public static final String DEFAULT_CERT_FILE_NAME = "public.crt";
    private static final String DEFAULT_HOST_NAME = "localhost";

    private static volatile boolean cleanupRequired = true;

    public static void create(FileSystemAbstraction fs, Path certDir) {
        create(fs, certDir, DEFAULT_KEY_FILE_NAME, DEFAULT_CERT_FILE_NAME);
    }

    public static void create(FileSystemAbstraction fs, Path certDir, String hostname) {
        create(fs, certDir, DEFAULT_KEY_FILE_NAME, DEFAULT_CERT_FILE_NAME, hostname);
    }

    public static void create(
            FileSystemAbstraction fs, Path certDir, String keyFileName, String certFileName, String hostname) {
        var certificateFactory = new SelfSignedCertificateFactory();
        var privateKeyFile = certDir.resolve(keyFileName);
        var certificateFile = certDir.resolve(certFileName);
        if (!exists(privateKeyFile) && !exists(certificateFile)) {
            try {
                certificateFactory.createSelfSignedCertificate(fs, certificateFile, privateKeyFile, hostname);
            } catch (Exception e) {
                throw new RuntimeException("Failed to generate private key and certificate", e);
            }
        }
    }

    public static void create(FileSystemAbstraction fs, Path certDir, String keyFileName, String certFileName) {
        create(fs, certDir, keyFileName, certFileName, DEFAULT_HOST_NAME);
    }

    public SelfSignedCertificateFactory() {
        random = useInsecureCertificateGeneration ? new InsecureRandom() : new SecureRandom();
    }

    public void createSelfSignedCertificate(
            FileSystemAbstraction fs, Path certificatePath, Path privateKeyPath, String hostName)
            throws GeneralSecurityException, IOException, OperatorCreationException {
        installCleanupHook(certificatePath, privateKeyPath);
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance(DEFAULT_ENCRYPTION);
        keyGen.initialize(2048, random);
        KeyPair keypair = keyGen.generateKeyPair();

        // Prepare the information required for generating an X.509 certificate.
        X500Name owner = new X500Name("CN=" + hostName);
        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                owner, new BigInteger(64, random), NOT_BEFORE, NOT_AFTER, owner, keypair.getPublic());

        // Subject alternative name (part of SNI extension, used for hostname verification)
        GeneralNames subjectAlternativeName = new GeneralNames(new GeneralName(GeneralName.dNSName, hostName));
        builder.addExtension(Extension.subjectAlternativeName, false, subjectAlternativeName);

        PrivateKey privateKey = keypair.getPrivate();
        ContentSigner signer = new JcaContentSignerBuilder("SHA512WithRSAEncryption").build(privateKey);
        X509CertificateHolder certHolder = builder.build(signer);
        X509Certificate cert = new JcaX509CertificateConverter()
                .setProvider(new BouncyCastleProvider())
                .getCertificate(certHolder);

        // check so that cert is valid
        cert.verify(keypair.getPublic());

        // write to disk
        writePem(fs, "CERTIFICATE", cert.getEncoded(), certificatePath);
        writePem(fs, "PRIVATE KEY", privateKey.getEncoded(), privateKeyPath);
        // Mark as done so we don't clean up certificates
        cleanupRequired = false;
    }

    /**
     * Makes sure to delete partially generated certificates. Does nothing if both certificate and private key have
     * been generated successfully.
     *
     * The hook should only be installed prior to generation of self-signed certificate, and not if certificates
     * already exist.
     */
    private static void installCleanupHook(final Path certificatePath, final Path privateKeyPath) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (cleanupRequired) {
                System.err.println("Cleaning up partially generated self-signed certificate...");
                try {
                    if (exists(certificatePath)) {
                        delete(certificatePath);
                    }

                    if (exists(privateKeyPath)) {
                        delete(privateKeyPath);
                    }
                } catch (IOException e) {
                    System.err.println("Error cleaning up");
                    e.printStackTrace(System.err);
                }
            }
        }));
    }

    private static void writePem(FileSystemAbstraction fs, String type, byte[] encodedContent, Path path)
            throws IOException {
        fs.mkdirs(path.getParent());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (PemWriter writer = new PemWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            writer.writeObject(new PemObject(type, encodedContent));
        }
        writeAllBytes(fs, path, out.toByteArray(), EmptyMemoryTracker.INSTANCE);

        if (fs instanceof DefaultFileSystemAbstraction) {
            try {
                Files.setPosixFilePermissions(
                        path, Set.of(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ));
            } catch (UnsupportedOperationException ignore) {
                // Fallback for windows
                path.toFile().setReadable(false, false);
                path.toFile().setWritable(false, false);
                path.toFile().setReadable(true);
                path.toFile().setWritable(true);
            }
        }
    }
}
