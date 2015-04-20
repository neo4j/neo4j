/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.security.ssl;

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
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Collection;
import java.util.Date;

import javax.crypto.NoSuchPaddingException;

import org.bouncycastle.jce.X509Principal;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.x509.X509V3CertificateGenerator;

public class SslCertificateFactory {

    private static final String CERTIFICATE_TYPE = "X.509";
    private static final String KEY_ENCRYPTION = "RSA";
    
    {
        Security.addProvider(new BouncyCastleProvider());
    }
    
    public void createSelfSignedCertificate(File certificatePath,
            File privateKeyPath, String hostName)
    {
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

            ensureFolderExists(certificatePath.getParentFile());
            ensureFolderExists(privateKeyPath.getParentFile());
            
            fos = new FileOutputStream(certificatePath);
            fos.write(certificate.getEncoded());
            fos.close();

            fos = new FileOutputStream(privateKeyPath);
            fos.write(keyPair.getPrivate().getEncoded());
            fos.close();

        } catch (Exception e)
        {
            throw new RuntimeException("Unable to create self signed SSL certificate, please see nested exception.", e);
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

    public Certificate[] loadCertificates(File certFile)
            throws CertificateException, FileNotFoundException {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(certFile);
            Collection<? extends Certificate> certificates = CertificateFactory.getInstance(CERTIFICATE_TYPE).generateCertificates(
                    fis);
            return certificates.toArray(new Certificate[]{});
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

    public PrivateKey loadPrivateKey(File privateKeyFile)
            throws IOException, NoSuchAlgorithmException,
            InvalidKeySpecException, NoSuchPaddingException,
            InvalidKeyException, InvalidAlgorithmParameterException 
            {
        DataInputStream dis = null;
        try 
        {
            FileInputStream fis = new FileInputStream(privateKeyFile);
            dis = new DataInputStream(fis);
            byte[] keyBytes = new byte[(int) privateKeyFile.length()];
            dis.readFully(keyBytes);

            KeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);

            return KeyFactory.getInstance(KEY_ENCRYPTION).generatePrivate(keySpec);
        } catch (FileNotFoundException e ) 
        {
            throw new IOException("Could not find private key file to use for SSL support, see nested exception.", e);
        } finally 
        {
            if (dis != null) {
                try {
                    dis.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void ensureFolderExists(File path) {
        if(!path.exists()) {
            path.mkdirs();
        }
    }

}
