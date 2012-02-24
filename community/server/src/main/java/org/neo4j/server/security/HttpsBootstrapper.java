/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.security;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Date;

import javax.crypto.NoSuchPaddingException;

import org.bouncycastle.jce.X509Principal;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.x509.X509V3CertificateGenerator;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.logging.Logger;

public class HttpsBootstrapper {

    private static final String CERTIFICATE_TYPE = "X.509";
    private static final String KEY_ENCRYPTION = "RSA";
    
    private Configurator configurator;
    private String hostName;
    private Logger log = Logger.getLogger(getClass());

    public HttpsBootstrapper(Configurator configurator, String hostName) {
        this.configurator = configurator;
        this.hostName = hostName;
    }

    public HttpsConfiguration bootstrap() {

        Security.addProvider(new BouncyCastleProvider());

        File keyStoreFile = new File(getKeyStorePath());
        File certFile = new File(getCertPath());
        File privateKeyFile = new File(getPrivateKeyPath());

        // The key store is recreated each time 
        // the server is restarted. Use new random
        // passwords for it each time.
        char[] keyStorePassword = getRandomChars(50);
        char[] keyPassword = getRandomChars(50);

        if (!certFile.exists()) {
            log.info("No SSL certificate found, generating a self-signed certificate..");
            try {
                createSelfSignedCertificate(certFile, privateKeyFile, keyPassword);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Unable to create self signed certificate, see nested exception.",
                        e);
            }
            log.info("Certificate generation done, saved certificate in [%s] and private key in [%s].", certFile.getAbsolutePath(), privateKeyFile.getAbsolutePath());
        }

        try {
            setUpKeyStore(keyStoreFile, keyStorePassword, keyPassword,
                    certFile, privateKeyFile);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to setup keystore, see nested exception.", e);
        }

        return new HttpsConfiguration(keyStoreFile.getAbsolutePath(),
                keyStorePassword, keyPassword);
    }

    private void createSelfSignedCertificate(File certPath, File privateKeyPath, char[] keyPassword)
            throws NoSuchAlgorithmException, CertificateEncodingException,
            InvalidKeyException, IllegalStateException,
            NoSuchProviderException, SignatureException, IOException {
        FileOutputStream fos = null;
        try {

            KeyPairGenerator keyPairGenerator = KeyPairGenerator
                    .getInstance(KEY_ENCRYPTION);
            keyPairGenerator.initialize(1024);
            KeyPair keyPair = keyPairGenerator.generateKeyPair();

            X509V3CertificateGenerator certGenertor = new X509V3CertificateGenerator();

            certGenertor.setSerialNumber(BigInteger.valueOf(
                    new SecureRandom().nextInt()).abs());
            certGenertor.setIssuerDN(new X509Principal("CN=" + hostName
                    + ", OU=None, O=None L=None, C=None"));
            certGenertor.setNotBefore(new Date(System.currentTimeMillis()
                    - 1000L * 60 * 60 * 24 * 30));
            certGenertor.setNotAfter(new Date(System.currentTimeMillis()
                    + (1000L * 60 * 60 * 24 * 365 * 10)));
            certGenertor.setSubjectDN(new X509Principal("CN=" + hostName
                    + ", OU=None, O=None L=None, C=None"));

            certGenertor.setPublicKey(keyPair.getPublic());
            certGenertor.setSignatureAlgorithm("MD5WithRSAEncryption");

            Certificate certificate = certGenertor.generate(
                    keyPair.getPrivate(), "BC");            

            ensureFolderExists(certPath.getParentFile());
            ensureFolderExists(privateKeyPath.getParentFile());
            
            fos = new FileOutputStream(certPath);
            fos.write(certificate.getEncoded());
            fos.close();

            fos = new FileOutputStream(privateKeyPath);
            fos.write(keyPair.getPrivate().getEncoded());
            fos.close();

        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    // We recreate the keystore on each boot. Jetty wants a keystore,
    // but we want our users to be able to just store their keys 
    // and their certificates directly in the file system, like
    // the Apache web server does.
    private void setUpKeyStore(File keyStorePath, char[] keyStorePassword,
            char[] keyPassword, File certFile, File privateKeyFile)
            throws KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException, InvalidKeyException,
            InvalidKeySpecException, NoSuchPaddingException,
            InvalidAlgorithmParameterException {
        FileOutputStream fis = null;
        try {
            
            if(keyStorePath.exists()) {
                keyStorePath.delete();
            }
            
            ensureFolderExists(keyStorePath.getParentFile());

            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null, keyStorePassword);

            keyStore.setKeyEntry(
                    "key",
                    loadPrivateKey(privateKeyFile),
                    keyPassword,
                    new java.security.cert.Certificate[] { loadCertificate(certFile) });

            fis = new FileOutputStream(keyStorePath.getAbsolutePath());
            keyStore.store(fis, keyStorePassword);

        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private Certificate loadCertificate(File certFile)
            throws CertificateException, FileNotFoundException {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(certFile);
            return CertificateFactory.getInstance(CERTIFICATE_TYPE).generateCertificate(
                    fis);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private PrivateKey loadPrivateKey(File privateKeyFile)
            throws IOException, NoSuchAlgorithmException,
            InvalidKeySpecException, NoSuchPaddingException,
            InvalidKeyException, InvalidAlgorithmParameterException {
        DataInputStream dis = null;
        try {
            FileInputStream fis = new FileInputStream(privateKeyFile);
            dis = new DataInputStream(fis);
            byte[] keyBytes = new byte[(int) privateKeyFile.length()];
            dis.readFully(keyBytes);

            KeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);

            return KeyFactory.getInstance(KEY_ENCRYPTION).generatePrivate(keySpec);
        } catch (FileNotFoundException e ) {
            throw new IOException("Could not find private key file to use for SSL support, see nested exception.", e);
        } finally {
            if (dis != null) {
                try {
                    dis.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private String getKeyStorePath() {
        return configurator.configuration().getString(
                Configurator.WEBSERVER_KEYSTORE_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_WEBSERVER_KEYSTORE_PATH);
    }

    private String getPrivateKeyPath() {
        return configurator.configuration().getString(
                Configurator.WEBSERVER_HTTPS_KEY_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_WEBSERVER_HTTPS_KEY_PATH);
    }

    private String getCertPath() {
        return configurator.configuration().getString(
                Configurator.WEBSERVER_HTTPS_CERT_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_WEBSERVER_HTTPS_CERT_PATH);
    }

    private char[] getRandomChars(int length) {
        SecureRandom rand = new SecureRandom();
        char [] chars = new char[length];
        for(int i=0;i<length;i++) {
            chars[i] = (char)rand.nextInt();
        }
        return chars;
    }

    private void ensureFolderExists(File path) {
        if(!path.exists()) {
            path.mkdirs();
        }
    }
}
