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
package org.neo4j.ssl;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Public Key Infrastructure utilities, e.g. generating/loading keys and certificates.
 */
public final class PkiUtils
{
    public static final String CERTIFICATE_TYPE = "X.509";

    private static final Provider PROVIDER = new BouncyCastleProvider();
    static
    {
        Security.addProvider( PROVIDER );
    }

    private PkiUtils()
    {
        // Disallow any instance creation. Only static methods are available.
    }

    public static X509Certificate[] loadCertificates( Path certFile ) throws CertificateException, IOException
    {
        CertificateFactory certFactory = CertificateFactory.getInstance( CERTIFICATE_TYPE );
        Collection<X509Certificate> certificates = new LinkedList<>();

        try ( PemReader r = new PemReader( Files.newBufferedReader( certFile ) ) )
        {
            for ( PemObject pemObject = r.readPemObject(); pemObject != null; pemObject = r.readPemObject() )
            {
                byte[] encodedCert = pemObject.getContent();
                Collection<X509Certificate> loadedCertificates =
                        (Collection<X509Certificate>) certFactory.generateCertificates( new ByteArrayInputStream( encodedCert ) );
                certificates.addAll( loadedCertificates );
            }
            return certificates.toArray( new X509Certificate[0] );
        }
    }

    public static PrivateKey loadPrivateKey( Path privateKeyFile, String passPhrase ) throws IOException
    {
        if ( passPhrase == null )
        {
            passPhrase = "";
        }
        try ( PEMParser r = new PEMParser( Files.newBufferedReader( privateKeyFile ) ) )
        {
            Object pemObject = r.readObject();
            final JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider( PROVIDER );

            if ( pemObject instanceof PEMEncryptedKeyPair ) // -----BEGIN RSA/DSA/EC PRIVATE KEY----- Proc-Type: 4,ENCRYPTED
            {
                final PEMEncryptedKeyPair ckp = (PEMEncryptedKeyPair) pemObject;
                final PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder().build( passPhrase.toCharArray() );
                return converter.getKeyPair( ckp.decryptKeyPair( decProv ) ).getPrivate();
            }
            else if ( pemObject instanceof PKCS8EncryptedPrivateKeyInfo ) // -----BEGIN ENCRYPTED PRIVATE KEY-----
            {
                try
                {
                    final PKCS8EncryptedPrivateKeyInfo encryptedInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
                    final InputDecryptorProvider provider = new JceOpenSSLPKCS8DecryptorProviderBuilder().build( passPhrase.toCharArray() );
                    final PrivateKeyInfo privateKeyInfo = encryptedInfo.decryptPrivateKeyInfo( provider );
                    return converter.getPrivateKey( privateKeyInfo );
                }
                catch ( PKCSException | OperatorCreationException e )
                {
                    throw new IOException( "Unable to decrypt private key.", e );
                }
            }
            else if ( pemObject instanceof PrivateKeyInfo ) // -----BEGIN PRIVATE KEY-----
            {
                return converter.getPrivateKey( (PrivateKeyInfo) pemObject );
            }
            else if ( pemObject instanceof PEMKeyPair ) // -----BEGIN RSA/DSA/EC PRIVATE KEY-----
            {
                return converter.getKeyPair( (PEMKeyPair) pemObject ).getPrivate();
            }
            else
            {
                throw new IOException( "Unrecognized private key format." );
            }
        }
    }
}
