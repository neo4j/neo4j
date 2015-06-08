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

import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Collection;
import java.util.LinkedList;
import javax.crypto.NoSuchPaddingException;

public class Certificates
{
    /** Generating SSL certificates takes a long time. This non-official setting allows us to use a fast source of randomness when running tests */
    private static final boolean useInsecureCertificateGeneration = Boolean.getBoolean( "org.neo4j.useInsecureCertificateGeneration" );
    private static final String CERTIFICATE_TYPE = "X.509";
    private static final String DEFAULT_ENCRYPTION = "RSA";
    private final SecureRandom random;

    {
        Security.addProvider(new BouncyCastleProvider());
    }

    public Certificates()
    {
        random = useInsecureCertificateGeneration ? new InsecureRandom() : new SecureRandom();
    }

    public void createSelfSignedCertificate(File certificatePath, File privateKeyPath, String hostName)
            throws NoSuchAlgorithmException, CertificateException, NoSuchProviderException, InvalidKeyException,
            SignatureException, IOException
    {
        SelfSignedCertificate cert = new SelfSignedCertificate(hostName, random, 1024);
        certificatePath.getParentFile().mkdirs();
        privateKeyPath.getParentFile().mkdirs();
        cert.certificate().renameTo( certificatePath );
        cert.privateKey().renameTo( privateKeyPath );
    }

    public Certificate[] loadCertificates(File certFile) throws CertificateException, IOException
    {
        CertificateFactory certFactory = CertificateFactory.getInstance( CERTIFICATE_TYPE );
        Collection<Certificate> certificates = new LinkedList<>();

        try(PemReader r = new PemReader( new FileReader( certFile ) ))
        {
            for( PemObject pemObject = r.readPemObject(); pemObject != null; pemObject = r.readPemObject() )
            {
                byte[] encodedCert = pemObject.getContent();
                certificates.addAll( certFactory.generateCertificates( new ByteArrayInputStream( encodedCert ) ) );
            }
        }

        if(certificates.size() == 0)
        {
            // Ok, failed to read as PEM file, try and read it as raw binary certificate
            try ( FileInputStream in = new FileInputStream( certFile ) )
            {
                certificates = (Collection<Certificate>)certFactory.generateCertificates( in );
            }
        }

        return certificates.toArray( new Certificate[certificates.size()] );
    }

    public PrivateKey loadPrivateKey(File privateKeyFile)
            throws IOException, NoSuchAlgorithmException,
            InvalidKeySpecException, NoSuchPaddingException,
            InvalidKeyException, InvalidAlgorithmParameterException 
    {
        try(PemReader r = new PemReader( new FileReader( privateKeyFile ) ))
        {
            PemObject pemObject = r.readPemObject();
            if( pemObject != null )
            {
                byte[] encodedKey = pemObject.getContent();
                KeySpec keySpec = new PKCS8EncodedKeySpec( encodedKey );
                try
                {
                    return KeyFactory.getInstance( "RSA" ).generatePrivate( keySpec );
                }
                catch ( InvalidKeySpecException ignore )
                {
                    try
                    {
                        return KeyFactory.getInstance( "DSA" ).generatePrivate( keySpec );
                    }
                    catch ( InvalidKeySpecException ignore2 )
                    {
                        try
                        {
                            return KeyFactory.getInstance( "EC" ).generatePrivate( keySpec );
                        }
                        catch ( InvalidKeySpecException e )
                        {
                            throw new InvalidKeySpecException( "Neither RSA, DSA nor EC worked", e );
                        }
                    }
                }
            }
        }

        // Ok, failed to read as PEM file, try and read it as a raw binary private key
        try(DataInputStream in = new DataInputStream(new FileInputStream(privateKeyFile)))
        {
            byte[] keyBytes = new byte[(int) privateKeyFile.length()];
            in.readFully( keyBytes );

            KeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);

            return KeyFactory.getInstance( DEFAULT_ENCRYPTION ).generatePrivate(keySpec);
        }
    }
}
