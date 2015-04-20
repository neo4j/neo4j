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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import javax.crypto.NoSuchPaddingException;

public class KeyStoreFactory
{
    private SslCertificateFactory sslCertificateFactory;

    public KeyStoreFactory()
    {
        this.sslCertificateFactory = new SslCertificateFactory();
    }

    public KeyStoreInformation createKeyStore( File keyStorePath, File privateKeyPath, File certificatePath )
    {
        try
        {

            char[] keyStorePassword = getRandomChars( 50 );
            char[] keyPassword = getRandomChars( 50 );

            createKeyStore( keyStorePath, keyStorePassword, keyPassword,
                    privateKeyPath, certificatePath );

            return new KeyStoreInformation( keyStorePath.getAbsolutePath(),
                    keyStorePassword, keyPassword );

        }
        catch ( Exception e )
        {
            throw new RuntimeException(
                    "Unable to setup keystore for SSL certificate, see nested exception.", e );
        }
    }

    private void createKeyStore( File keyStorePath, char[] keyStorePassword,
                                 char[] keyPassword, File privateKeyFile, File certFile )
            throws KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException, InvalidKeyException,
            InvalidKeySpecException, NoSuchPaddingException,
            InvalidAlgorithmParameterException
    {
        FileOutputStream fis = null;
        try
        {

            if ( keyStorePath.exists() )
            {
                keyStorePath.delete();
            }

            ensureFolderExists( keyStorePath.getParentFile() );

            KeyStore keyStore = KeyStore.getInstance( "JKS" );
            keyStore.load( null, keyStorePassword );

            keyStore.setKeyEntry(
                    "key",
                    sslCertificateFactory.loadPrivateKey( privateKeyFile ),
                    keyPassword,
                    sslCertificateFactory.loadCertificates( certFile ) );

            fis = new FileOutputStream( keyStorePath.getAbsolutePath() );
            keyStore.store( fis, keyStorePassword );

        }
        finally
        {
            if ( fis != null )
            {
                try
                {
                    fis.close();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }
    }

    private char[] getRandomChars( int length )
    {
        SecureRandom rand = new SecureRandom();
        char[] chars = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            chars[i] = (char) rand.nextInt();
        }
        return chars;
    }

    private void ensureFolderExists( File path )
    {
        if ( !path.exists() )
        {
            path.mkdirs();
        }
    }
}
