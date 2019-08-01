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
package org.neo4j.ssl;

import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Public Key Infrastructure utilities, e.g. generating/loading keys and certificates.
 */
public final class PkiUtils
{
    public static final String CERTIFICATE_TYPE = "X.509";
    static final String DEFAULT_ENCRYPTION = "RSA";

    private PkiUtils()
    {
        // Disallow any instance creation. Only static methods are available.
    }

    public static X509Certificate[] loadCertificates( File certFile ) throws CertificateException, IOException
    {
        CertificateFactory certFactory = CertificateFactory.getInstance( CERTIFICATE_TYPE );
        Collection<X509Certificate> certificates = new LinkedList<>();

        try ( PemReader r = new PemReader( new FileReader( certFile ) ) )
        {
            for ( PemObject pemObject = r.readPemObject(); pemObject != null; pemObject = r.readPemObject() )
            {
                byte[] encodedCert = pemObject.getContent();
                Collection<? extends X509Certificate> loadedCertificates = (Collection<X509Certificate>)
                        certFactory.generateCertificates( new ByteArrayInputStream( encodedCert ) );
                certificates.addAll( loadedCertificates );
            }
        }

        if ( certificates.size() == 0 )
        {
            // Ok, failed to read as PEM file, try and read it as raw binary certificate
            try ( FileInputStream in = new FileInputStream( certFile ) )
            {
                certificates = (Collection<X509Certificate>) certFactory.generateCertificates( in );
            }
        }

        return certificates.toArray( new X509Certificate[certificates.size()] );
    }

    public static PrivateKey loadPrivateKey( File privateKeyFile )
            throws IOException, NoSuchAlgorithmException,
            InvalidKeySpecException
    {
        try ( PemReader r = new PemReader( new FileReader( privateKeyFile ) ) )
        {
            PemObject pemObject = r.readPemObject();
            if ( pemObject != null )
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
        try ( DataInputStream in = new DataInputStream( new FileInputStream( privateKeyFile ) ) )
        {
            byte[] keyBytes = new byte[(int) privateKeyFile.length()];
            in.readFully( keyBytes );

            KeySpec keySpec = new PKCS8EncodedKeySpec( keyBytes );

            return KeyFactory.getInstance( DEFAULT_ENCRYPTION ).generatePrivate( keySpec );
        }
    }
}
