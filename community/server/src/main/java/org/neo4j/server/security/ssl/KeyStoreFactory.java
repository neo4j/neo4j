/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;

public class KeyStoreFactory
{
    private Certificates certFactory;

    public KeyStoreFactory()
    {
        this.certFactory = new Certificates();
    }

    public KeyStoreInformation createKeyStore( File privateKeyPath, File certificatePath ) throws IOException, GeneralSecurityException
    {
        char[] keyStorePassword = getRandomChars( 50 );
        char[] keyPassword = getRandomChars( 50 );

        KeyStore ks = createKeyStore( keyStorePassword, keyPassword,
                privateKeyPath, certificatePath );

        return new KeyStoreInformation( ks, keyStorePassword, keyPassword );
    }

    private KeyStore createKeyStore( char[] keyStorePassword, char[] keyPassword, File privateKeyFile, File certFile )
            throws IOException, GeneralSecurityException
    {
        KeyStore keyStore = KeyStore.getInstance( "JKS" );

        // Initialize the keystore
        keyStore.load( null, keyStorePassword );

        // Stuff our key into it
        keyStore.setKeyEntry( "key", certFactory.loadPrivateKey( privateKeyFile ), keyPassword, certFactory.loadCertificates( certFile ) );

        return keyStore;
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
}
